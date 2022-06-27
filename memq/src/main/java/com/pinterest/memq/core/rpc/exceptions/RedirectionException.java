package com.pinterest.memq.core.rpc.exceptions;

public class RedirectionException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public RedirectionException() {
    super();
    // TODO Auto-generated constructor stub
  }
  
  public RedirectionException(String message,
                             Throwable cause,
                             boolean enableSuppression,
                             boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
    // TODO Auto-generated constructor stub
  }

  public RedirectionException(String message, Throwable cause) {
    super(message, cause);
    // TODO Auto-generated constructor stub
  }

  public RedirectionException(String message) {
    super(message);
    // TODO Auto-generated constructor stub
  }

  public RedirectionException(Throwable cause) {
    super(cause);
    // TODO Auto-generated constructor stub
  }

}
