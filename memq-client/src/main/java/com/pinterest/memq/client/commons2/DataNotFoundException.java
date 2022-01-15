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
package com.pinterest.memq.client.commons2;

public class DataNotFoundException extends Exception {

  private static final long serialVersionUID = 1L;

  public DataNotFoundException() {
    super();
  }

  public DataNotFoundException(String message,
                               Throwable cause,
                               boolean enableSuppression,
                               boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

  public DataNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public DataNotFoundException(String message) {
    super(message);
  }

  public DataNotFoundException(Throwable cause) {
    super(cause);
  }

}
