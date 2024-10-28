/**
 * Copyright 2024 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.memq.commons.storage.s3express;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import io.netty.channel.ChannelOption;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsS3V4Signer;
import software.amazon.awssdk.auth.signer.params.AwsS3V4SignerParams;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;

/**
 * Manages S3 Express session credentials
 */
public class SessionTokenManager {

  private static final SessionTokenManager mgr = new SessionTokenManager();
  private static final Logger logger = Logger.getLogger(SessionTokenManager.class.getName());
  private static final String S3_EXPRESS = "s3express";
  private static final String CREDENTIAL_PROVIDER_THREAD_NAME = "IamCredentialUpdater";
  private static final int DEFAULT_MAX_CONNECTIONS_SECOND = 10;
  private static final int DEFAULT_MAX_IDLE_TIME_SECOND = 20;
  private static final int DEFAULT_MAX_LIFE_TIME_SECOND = 60;
  private static final int DEFAULT_PENDING_ACQUIRE_TIMEOUT_SECOND = 60;
  private static final int DEFAULT_EVICT_IN_BACKGROUND_SECOND = 120;
  private static final int FETCH_CREDENTIALS_INTERVAL_MS = 100;
  private static final boolean USE_DEFAULT_CREDENTIAL_PROVIDER = false;
  private Map<String, ConcurrentLinkedDeque<SessionCreds>> bucketCredentialMap = new ConcurrentHashMap<>();
  private ScheduledExecutorService es = Executors.newScheduledThreadPool(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread th = new Thread(r);
      th.setDaemon(true);
      return th;
    }
  });
  private HttpClient secureClient;

  public SessionTokenManager() {
    this(new Properties());
  }

  public SessionTokenManager(Properties props) {
    ConnectionProvider connectionProvider = getConnectionProvider(props);
    secureClient = HttpClient.create(connectionProvider).option(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
            .option(ChannelOption.SO_LINGER, 0).secure();
  }

  public static SessionTokenManager getInstance() {
    return mgr;
  }

  private static ConnectionProvider getConnectionProvider(Properties props) {
    int maxConnections = Integer.parseInt(props.getProperty("maxConnections",
            String.valueOf(DEFAULT_MAX_CONNECTIONS_SECOND)));
    int maxIdleTime = Integer.parseInt(props.getProperty("maxIdleTime",
            String.valueOf(DEFAULT_MAX_IDLE_TIME_SECOND)));
    int maxLifeTime = Integer.parseInt(props.getProperty("maxLifeTime",
            String.valueOf(DEFAULT_MAX_LIFE_TIME_SECOND)));
    int pendingAcquireTimeout = Integer.parseInt(props.getProperty("pendingAcquireTimeout",
            String.valueOf(DEFAULT_PENDING_ACQUIRE_TIMEOUT_SECOND)));
    int evictInBackground = Integer.parseInt(props.getProperty("evictInBackground",
            String.valueOf(DEFAULT_EVICT_IN_BACKGROUND_SECOND)));
    return ConnectionProvider.builder(S3_EXPRESS)
            .maxConnections(maxConnections)
            .maxIdleTime(Duration.ofSeconds(maxIdleTime))
            .maxLifeTime(Duration.ofSeconds(maxLifeTime))
            .pendingAcquireTimeout(Duration.ofSeconds(pendingAcquireTimeout))
            .evictInBackground(Duration.ofSeconds(evictInBackground))
            .build();
  }

  /**
   * Running credentials fetcher for the given bucket.
   * @param bucketName
   * @return SessionCreds
   * @throws InterruptedException
   */
  public SessionCreds getCredentials(final String bucketName) throws InterruptedException {
    ConcurrentLinkedDeque<SessionCreds> concurrentLinkedDeque = bucketCredentialMap.get(bucketName);
    if (concurrentLinkedDeque == null) {
      synchronized (bucketCredentialMap) {
        concurrentLinkedDeque = bucketCredentialMap.get(bucketName);
        if (concurrentLinkedDeque == null) {
          concurrentLinkedDeque = new ConcurrentLinkedDeque<>();
          // start the scheduled task for credential refresh
          final ConcurrentLinkedDeque<SessionCreds> concurrentLinkedDequeRef = concurrentLinkedDeque;
          es.scheduleAtFixedRate(() -> {
            try {
              SessionCreds fetchCredentials = fetchCredentials(bucketName);
              concurrentLinkedDequeRef.add(fetchCredentials);
              if (concurrentLinkedDequeRef.size() == 2) {
                // purge existing credentials
                concurrentLinkedDequeRef.poll();
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }, 0, 4, TimeUnit.MINUTES);
          bucketCredentialMap.put(bucketName, concurrentLinkedDeque);
        }
      }
    }
    while (concurrentLinkedDeque.isEmpty()) {
      Thread.sleep(FETCH_CREDENTIALS_INTERVAL_MS);
    }
    return concurrentLinkedDeque.peek();
  }

  /**
   * Fetch the session credentials for the given bucket
   * @param bucketName
   * @return SessionCreds
   * @throws Exception
   */
  protected SessionCreds fetchCredentials(String bucketName) throws Exception {
    SdkHttpFullRequest createSessionRequest = generateCreateSessionRequest(bucketName);
    Region region = Region.of(S3ExpressHelper.getRegionFromBucket(bucketName));
    SdkHttpFullRequest signedCreateSessionRequest = signRequest(createSessionRequest, region);
    Map<String, List<String>> signedCreateSessionRequestHeaders = signedCreateSessionRequest.headers();
    String awsResponse = secureClient.headers(headers -> {
      for (Entry<String, List<String>> entry : signedCreateSessionRequestHeaders.entrySet()) {
        headers.set(entry.getKey(), entry.getValue().get(0));
      }
    }).get().uri(createSessionRequest.getUri()).responseSingle((response, bytes) -> bytes.asString()).block();
    logger.fine("AWS Credential Response: " + awsResponse);
    return generateSessionCreds(awsResponse);
  }

  /**
   * Sign the request with instance credentials.
   * Override this method to use different credential provider.
   * @param req
   * @param region
   * @return SdkHttpFullRequest signed request
   */
  public static SdkHttpFullRequest signRequest(SdkHttpFullRequest req, Region region) {
    AwsS3V4Signer signer = AwsS3V4Signer.create();
    if (USE_DEFAULT_CREDENTIAL_PROVIDER) {
      DefaultCredentialsProvider credentialProvider = DefaultCredentialsProvider
              .builder().asyncCredentialUpdateEnabled(true).build();
      return signer.sign(req,
              AwsS3V4SignerParams.builder().awsCredentials(credentialProvider.resolveCredentials())
                      .signingName(S3_EXPRESS).signingRegion(region).build());
    } else {
      InstanceProfileCredentialsProvider credentialProvider = InstanceProfileCredentialsProvider
              .builder().asyncCredentialUpdateEnabled(true).asyncThreadName(CREDENTIAL_PROVIDER_THREAD_NAME)
              .build();
      return signer.sign(req,
              AwsS3V4SignerParams.builder().awsCredentials(credentialProvider.resolveCredentials())
                      .signingName(S3_EXPRESS).signingRegion(region).build());
    }
  }

  /**
   * Generate the create session request
   * @param bucketName
   * @return SdkHttpFullRequest create session request
   * @throws Exception
   */
  private static SdkHttpFullRequest generateCreateSessionRequest(String bucketName) throws Exception {
    return SdkHttpFullRequest.builder()
        .appendHeader("x-amz-create-session-mode", "ReadWrite")
        .appendRawQueryParameter("session", "").method(SdkHttpMethod.GET)
        .uri(URI.create(S3ExpressHelper.generateBucketUrl(bucketName)))
        .build();
  }

  /**
   * Generate the session credentials from the AWS response string
   * @param awsResponse
   * @return SessionCreds session credentials
   * @throws Exception
   */
  private static SessionCreds generateSessionCreds(String awsResponse) throws Exception {
    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = builderFactory.newDocumentBuilder();
    Document xmlDocument = builder.parse(new ByteArrayInputStream(awsResponse.getBytes()));
    XPath xPath = XPathFactory.newInstance().newXPath();

    SessionCreds creds = new SessionCreds();
    String expression = "/CreateSessionResult/Credentials/SessionToken";
    creds.token = ((Node) (xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODE)))
            .getTextContent();

    expression = "/CreateSessionResult/Credentials/SecretAccessKey";
    creds.secret = ((Node) (xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODE)))
            .getTextContent();

    expression = "/CreateSessionResult/Credentials/AccessKeyId";
    creds.key = ((Node) (xPath.compile(expression).evaluate(xmlDocument, XPathConstants.NODE)))
            .getTextContent();

    return creds;
  }

  public static void main(String[] args) throws Exception {
    SessionTokenManager token = new SessionTokenManager();
    token.fetchCredentials("test");
  }
}
