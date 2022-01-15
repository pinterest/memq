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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;

import com.codahale.metrics.MetricRegistry;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.Properties;

public class ApacheRequestClient implements RequestClient {
  private static final int MAX_ATTEMPTS = 5;
  private static final Logger logger = LoggerFactory.getLogger(ApacheRequestClient.class);
  public static final String DEFAULT_MAX_CONNECTIONS_PER_ROUTE = "defaultMaxConnectionsPerRoute";
  public static final String MAX_CONNECTIONS = "maxConnections";

  private final S3Presigner presigner;
  private final MetricRegistry metricRegistry;
  private CloseableHttpClient client;

  public ApacheRequestClient(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    this.presigner = S3Presigner.builder().build();
  }

  @Override
  public void initialize(Properties properties) {
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
    cm.setDefaultMaxPerRoute(Integer.parseInt(properties.getProperty(DEFAULT_MAX_CONNECTIONS_PER_ROUTE, "100")));
    cm.setMaxTotal(Integer.parseInt(properties.getProperty(MAX_CONNECTIONS, "500")));
    client = HttpClients.custom()
        .setServiceUnavailableRetryStrategy(new ObjectGetRetryStrategy())
        .setConnectionManager(cm)
        .build();
  }
  
  @Override
  public ByteBuf tryObjectGetAsBuffer(GetObjectRequest request) throws IOException {
    ByteBufOutputStream bos = new ByteBufOutputStream(PooledByteBufAllocator.DEFAULT.buffer());
    IOUtils.copy(tryObjectGet(request), bos);
    return bos.buffer();
  }

  @Override
  public InputStream tryObjectGet(GetObjectRequest request) throws IOException {
    PresignedGetObjectRequest presignGetObject = presigner.presignGetObject(GetObjectPresignRequest
        .builder()
        .getObjectRequest(request)
        .signatureDuration(Duration.ofMinutes(60)).build());
    URL url = presignGetObject.url();
    HttpGet actualRequest = new HttpGet(url.toString());
    presignGetObject.signedHeaders().forEach((n, v) -> v.forEach(vv -> actualRequest.addHeader(n, vv)));
    CloseableHttpResponse resp = client.execute(actualRequest);
    try {
      int code = resp.getStatusLine().getStatusCode();
      switch (code) {
        case 200:
        case 206:
          return resp.getEntity().getContent();
        case 404:
          throw NOT_FOUND_EXCEPTION;
        case 403:
          throw FORBIDDEN_EXCEPTION;
        case 500:
          throw ISE_EXCEPTION;
        case 503:
          throw UNAVAILABLE_EXCEPTION;
        default:
          throw new S3Exception(code) {
            private static final long serialVersionUID = 1L;
          };
      }
    } catch (IOException e) {
      if (e instanceof S3Exception) {
        int code = ((S3Exception) e).getErrorCode();
        metricRegistry.counter(OBJECT_FETCH_ERROR_KEY + "." + code).inc();
      }
      logger.error("Failed to get object " + request.bucket() + "/" + request.key(), e);
      resp.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    try {
      presigner.close();
    } finally {
      client.close();
    }
  }

  private static class ObjectGetRetryStrategy implements ServiceUnavailableRetryStrategy {

    @Override
    public boolean retryRequest(HttpResponse httpResponse, int i, HttpContext httpContext) {
      if (i >= MAX_ATTEMPTS) {
        logger.warn("Failed to get response from s3 after " + i + " attempts: [" + httpResponse.getStatusLine().getStatusCode() + "] " + httpResponse.getStatusLine().getReasonPhrase());
        return false;
      }

      switch (httpResponse.getStatusLine().getStatusCode()) {
        case 500:
        case 503:
          return true;
        default:
          return false;
      }
    }

    @Override
    public long getRetryInterval() {
      return 100;
    }
  }
}
