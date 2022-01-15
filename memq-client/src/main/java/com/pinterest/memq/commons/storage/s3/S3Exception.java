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
package com.pinterest.memq.commons.storage.s3;

import java.io.IOException;

public abstract class S3Exception extends IOException {

  private static final long serialVersionUID = 1L;

  private int errorCode;

  public S3Exception() {
  }

  public S3Exception(int errorCode) {
    super("S3 Exception (code: " + errorCode + ")");
    this.errorCode = errorCode;
  }

  public S3Exception(String message) {
    super(message);
  }

  public S3Exception(int errorCode, String message) {
    super("S3 Exception (code: " + errorCode + ", message: " + message + ")");
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(int errorCode) {
    this.errorCode = errorCode;
  }

  public static class RetriableException extends S3Exception {

    public RetriableException(int errorCode) {
      super(errorCode);
    }

    private static final long serialVersionUID = 1L;

  }

  public static class NotFoundException extends S3Exception {

    public NotFoundException() {
      super(404);
    }

    private static final long serialVersionUID = 1L;

  }

  public static class ForbiddenException extends S3Exception {

    public ForbiddenException() {
      super(403);
    }

    private static final long serialVersionUID = 1L;

  }

  public static class InternalServerErrorException extends RetriableException {

    public InternalServerErrorException() {
      super(500);
    }

    private static final long serialVersionUID = 1L;

  }

  public static class ServiceUnavailableException extends RetriableException {

    public ServiceUnavailableException() {
      super(503);
    }

    private static final long serialVersionUID = 1L;

  }

}
