package com.pinterest.memq.core.rpc.exceptions;

public class InternalServerErrorException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public InternalServerErrorException() {
    super();
    // TODO Auto-generated constructor stub
  }

  public InternalServerErrorException(String message,
                                 Throwable cause,
                                 boolean enableSuppression,
                                 boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
    // TODO Auto-generated constructor stub
  }

  public InternalServerErrorException(String message, Throwable cause) {
    super(message, cause);
    // TODO Auto-generated constructor stub
  }

  public InternalServerErrorException(String message) {
    super(message);
    // TODO Auto-generated constructor stub
  }

  public InternalServerErrorException(Throwable cause) {
    super(cause);
    // TODO Auto-generated constructor stub
  }

}
