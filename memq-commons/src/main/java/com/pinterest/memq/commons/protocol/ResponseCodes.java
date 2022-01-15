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
package com.pinterest.memq.commons.protocol;

public class ResponseCodes {

  public static final short OK = 200;
  public static final short UNAUTHORIZED = 401;
  public static final short SERVICE_UNAVAILABLE = 503;
  public static final short NOT_FOUND = 404;
  public static final short INTERNAL_SERVER_ERROR = 500;
  public static final short BAD_REQUEST = 400;
  public static final short REQUEST_FAILED = 502;
  public static final short REDIRECT = 302;
  public static final short NO_DATA = 204;
  
  private ResponseCodes() {
  }

}
