/**
 * Copyright 2022 Pinterest, Inc.
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
package com.pinterest.memq.commons.storage.s3.reader.client;

import static com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler.FORBIDDEN_EXCEPTION;
import static com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler.ISE_EXCEPTION;
import static com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler.NOT_FOUND_EXCEPTION;
import static com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler.OBJECT_FETCH_ERROR_KEY;
import static com.pinterest.memq.commons.storage.s3.AbstractS3StorageHandler.UNAVAILABLE_EXCEPTION;

import com.pinterest.memq.commons.storage.s3.S3Exception;

import com.codahale.metrics.MetricRegistry;
import io.netty.handler.timeout.ReadTimeoutException;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.RetrySpec;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class ReactorNettyRequestClient implements RequestClient {
  public static final String READ_TIMEOUT_MS = "readTimeoutMs";
  public static final String RESPONSE_TIMEOUT_MS = "responseTimeoutMs";
  public static final String MAX_RETRIES = "maxRetries";
  private static final Logger logger = LoggerFactory.getLogger(ReactorNettyRequestClient.class);

  private static final long DEFAULT_READ_TIMEOUT_MS = 10000;

  private final S3Presigner presigner;
  private final MetricRegistry metricRegistry;

  private HttpClient client;
  private int maxRetries = 4;
  private Duration responseTimeoutDuration;

  public ReactorNettyRequestClient(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    this.presigner = S3Presigner.builder().build();
  }

  @Override
  public void initialize(Properties properties) {
    if (properties.containsKey(MAX_RETRIES)) {
      maxRetries = Integer.parseInt(properties.getProperty(MAX_RETRIES));
    }

    Duration readTimeoutDuration;
    if (properties.containsKey(READ_TIMEOUT_MS)) {
      readTimeoutDuration = Duration.ofMillis(Long.parseLong(properties.getProperty(READ_TIMEOUT_MS)));
    } else {
      readTimeoutDuration = Duration.ofMillis(DEFAULT_READ_TIMEOUT_MS);
    }

    if (properties.containsKey(RESPONSE_TIMEOUT_MS)) {
      responseTimeoutDuration = Duration.ofMillis(Long.parseLong(properties.getProperty(RESPONSE_TIMEOUT_MS)));
    } else {
      // default to max number of attempts times read timeout plus a small buffer to avoid competing with read timeouts
      responseTimeoutDuration = readTimeoutDuration.multipliedBy(maxRetries + 1).plusMillis(100);
    }

    HttpClient client = HttpClient.create()
        .option(ChannelOption.SO_SNDBUF, 4 * 1024 * 1024)
        .option(ChannelOption.SO_LINGER, 0)
        .responseTimeout(readTimeoutDuration)
        .secure();

    this.client = client;
  }

  @Override
  public InputStream tryObjectGet(GetObjectRequest request) throws IOException {
    PresignedGetObjectRequest presignGetObject = presigner.presignGetObject(GetObjectPresignRequest
        .builder()
        .getObjectRequest(request)
        .signatureDuration(Duration.ofMinutes(60)).build());
    URL url = presignGetObject.url();
    logger.debug("Fetching URL {}", url.toString());
    try {
      return tryObjectGetInterAsStream(url.toURI(), presignGetObject.signedHeaders());
    } catch (URISyntaxException use) {
      throw new IOException(use);
    }
  }
  
  @Override
  public ByteBuf tryObjectGetAsBuffer(GetObjectRequest request) throws IOException {
    PresignedGetObjectRequest presignGetObject = presigner.presignGetObject(GetObjectPresignRequest
        .builder()
        .getObjectRequest(request)
        .signatureDuration(Duration.ofMinutes(60)).build());
    URL url = presignGetObject.url();
    logger.debug("Fetching URL{}", url.toString());
    try {
      return tryObjectGetInternal(url.toURI(), presignGetObject.signedHeaders());
    } catch (URISyntaxException use) {
      throw new IOException(use);
    }
  }
  
  protected InputStream tryObjectGetInterAsStream(URI uri,
                                                  Map<String, List<String>> headers) throws IOException {
    return new ByteBufInputStream(tryObjectGetInternal(uri, headers));
  }

  protected ByteBuf tryObjectGetInternal(URI uri, Map<String, List<String>> headers)
      throws IOException {
    try {
      ByteBuf buf = client
          .headers(
              h -> headers.forEach(h::add)
          )
          .get()
          .uri(uri)
          .responseSingle((t, u) -> {
            int code = t.status().code();
            switch (code) {
              case 200:
              case 206:
                return u;
              case 404:
                return Mono.error(NOT_FOUND_EXCEPTION);
              case 403:
                return Mono.error(FORBIDDEN_EXCEPTION);
              case 500:
                return Mono.error(ISE_EXCEPTION);
              case 503:
                return Mono.error(UNAVAILABLE_EXCEPTION);
              default:
                return Mono.error(new S3Exception(code) {
                  private static final long serialVersionUID = 1L;
                });
            }
          })
          .doOnError((t) -> {
            if (t instanceof S3Exception) {
              metricRegistry.counter(OBJECT_FETCH_ERROR_KEY + ".s3." + ((S3Exception) t).getErrorCode()).inc();
            } else if (t instanceof ReadTimeoutException) {
              metricRegistry.counter(OBJECT_FETCH_ERROR_KEY + ".timeout.read").inc();
            } else {
              metricRegistry.counter(OBJECT_FETCH_ERROR_KEY + ".other").inc();
            }
          })
          .retryWhen(RetrySpec
              .max(maxRetries)
              .filter((t) -> t instanceof ReadTimeoutException || t instanceof S3Exception.RetriableException)
              .doBeforeRetry((rs) -> logger.warn("Retrying (retry: " + (rs.totalRetries() + 1) + "/" + (maxRetries) +") , exception " + rs.failure() + " when fetching from " + uri))
          )
          .timeout(responseTimeoutDuration)
          .doOnError((t) -> {
            if (t instanceof TimeoutException) {
              metricRegistry.counter(OBJECT_FETCH_ERROR_KEY + ".timeout.response").inc();
            }
          })
          .block();
      if (buf != null) {
        return buf.retainedDuplicate();
      } else {
        return null;
      }
    } catch (RuntimeException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else if (e.getCause() instanceof ReadTimeoutException || e.getCause() instanceof TimeoutException) {
        throw new IOException(e.getCause());
      } else {
        throw new IOException(e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    presigner.close();
  }
}
