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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
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
    ConnectionProvider provider = ConnectionProvider.builder("s3express").maxConnections(10)
        .maxIdleTime(Duration.ofSeconds(20)).maxLifeTime(Duration.ofSeconds(60))
        .pendingAcquireTimeout(Duration.ofSeconds(60)).evictInBackground(Duration.ofSeconds(120))
        .build();
    secureClient = HttpClient.create(provider).option(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
        .option(ChannelOption.SO_LINGER, 0).secure();
  }

  public static SessionTokenManager getInstance() {
    return mgr;
  }

  public SessionCreds getCredentials(final String bucketname) throws InterruptedException {
    ConcurrentLinkedDeque<SessionCreds> concurrentLinkedDeque = bucketCredentialMap.get(bucketname);
    if (concurrentLinkedDeque == null) {
      synchronized (bucketCredentialMap) {
        concurrentLinkedDeque = bucketCredentialMap.get(bucketname);
        if (concurrentLinkedDeque == null) {
          concurrentLinkedDeque = new ConcurrentLinkedDeque<>();
          // start the scheduled task for credential refresh
          final ConcurrentLinkedDeque<SessionCreds> concurrentLinkedDequeRef = concurrentLinkedDeque;
          es.scheduleAtFixedRate(() -> {
            try {
              SessionCreds fetchCredentials = fetchCredentials(bucketname);
              concurrentLinkedDequeRef.add(fetchCredentials);
              if (concurrentLinkedDequeRef.size() == 2) {
                // purge existing credentials
                concurrentLinkedDequeRef.poll();
              }
            } catch (Exception e) {
              e.printStackTrace();
            }
          }, 0, 4, TimeUnit.MINUTES);
          bucketCredentialMap.put(bucketname, concurrentLinkedDeque);
        }
      }
    }
    while (concurrentLinkedDeque.isEmpty()) {
      Thread.sleep(100);
    }
    return concurrentLinkedDeque.peek();
  }

  protected SessionCreds fetchCredentials(String bucketname) throws Exception {
    DefaultCredentialsProvider credentialProvider = DefaultCredentialsProvider
        .builder().asyncCredentialUpdateEnabled(true).build();
    // Sign it...
    AwsS3V4Signer signer = AwsS3V4Signer.create();
    SdkHttpFullRequest req = SdkHttpFullRequest.builder()
        .appendHeader("x-amz-create-session-mode", "ReadWrite")
        .appendRawQueryParameter("session", "").method(SdkHttpMethod.GET)
        .uri(URI.create("https://" + bucketname + ".s3express-use1-az5.us-east-1.amazonaws.com"))
        .build();
    SdkHttpFullRequest req1 = signer.sign(req,
        AwsS3V4SignerParams.builder().awsCredentials(credentialProvider.resolveCredentials())
            .signingName("s3express").signingRegion(Region.US_EAST_1).build());

    Map<String, List<String>> headers2 = req1.headers();
    String awsResponse = secureClient.headers(headers -> {
      for (Entry<String, List<String>> entry : headers2.entrySet()) {
        headers.set(entry.getKey(), entry.getValue().get(0));
      }
    }).get().uri(req.getUri()).responseSingle((response, bytes) -> bytes.asString()).block();
    
    System.out.println("CredentialResponse\n"+awsResponse+"\n\n");

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
