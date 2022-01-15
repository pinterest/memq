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
package com.pinterest.memq.core.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Base64;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import com.pinterest.memq.commons.config.SSLConfig;

public class MemqUtils {

  // NOTE this is only designed for IPv4 only, if we ever switch to IPv6 this
  // section will need to be updated along with the tests and encoding
  public static byte[] HOST_IPV4_ADDRESS = new byte[4];
  public static Charset CHARSET = Charset.forName("utf-8");

  static {
    try {
      HOST_IPV4_ADDRESS = Inet4Address.getLocalHost().getAddress();
    } catch (Exception e) {
      // TODO: Add a metric to track failures here
      e.printStackTrace();
    }
  }

  public static String getStringFromByteAddress(byte[] address) {
    if (address != null) {
      try {
        return Inet4Address.getByAddress(address).getHostAddress();
      } catch (UnknownHostException e) {
        e.printStackTrace();
        return null;
      }
    } else {
      return "null";
    }
  }

  private MemqUtils() {
  }

  public static byte[] calculateMessageIdHash(byte[] messageIdHash, byte[] byteArray) {
    if (messageIdHash == null) {
      return byteArray;
    } else {
      for (int i = 0; i < byteArray.length; i++) {
        messageIdHash[i] = (byte) (messageIdHash[i] ^ byteArray[i]);
      }
      return messageIdHash;
    }
  }

  public static String etagToBase64(String eTag) throws DecoderException {
    byte[] decodedHex = Hex.decodeHex(eTag.toCharArray());
    String encodedHexB64 = Base64.getEncoder().encodeToString(decodedHex);
    return encodedHexB64;
  }

  public static TrustManagerFactory extractTMPFromSSLConfig(SSLConfig sslConfig) throws KeyStoreException,
                                                                                 Exception,
                                                                                 IOException,
                                                                                 NoSuchAlgorithmException,
                                                                                 CertificateException,
                                                                                 FileNotFoundException {
    KeyStore ts = KeyStore.getInstance(sslConfig.getTruststoreType());
    File trustStoreFile = new File(sslConfig.getTruststorePath());
    if (!trustStoreFile.exists()) {
      throw new Exception("Missing truststore");
    }
    ts.load(new FileInputStream(trustStoreFile), sslConfig.getTruststorePassword().toCharArray());
    TrustManagerFactory tmf = TrustManagerFactory
        .getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(ts);
    return tmf;
  }

  public static KeyManagerFactory extractKMFFromSSLConfig(SSLConfig sslConfig) throws KeyStoreException,
                                                                               Exception,
                                                                               IOException,
                                                                               NoSuchAlgorithmException,
                                                                               CertificateException,
                                                                               FileNotFoundException,
                                                                               UnrecoverableKeyException {
    KeyStore ks = KeyStore.getInstance(sslConfig.getKeystoreType());
    File keystoreFile = new File(sslConfig.getKeystorePath());
    if (!keystoreFile.exists()) {
      throw new Exception("Missing keystore");
    }
    ks.load(new FileInputStream(keystoreFile), sslConfig.getKeystorePassword().toCharArray());
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(ks, sslConfig.getKeystorePassword().toCharArray());
    return kmf;
  }
}
