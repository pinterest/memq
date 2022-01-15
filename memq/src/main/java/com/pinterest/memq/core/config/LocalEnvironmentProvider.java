package com.pinterest.memq.core.config;

public class LocalEnvironmentProvider extends EnvironmentProvider {
  
  @Override
  public String getRack() {
    return "local";
  }

  @Override
  public String getInstanceType() {
    return "2xl";
  }

  @Override
  public String getIP() {
    return "127.0.0.1";
  }

}
