package com.pinterest.memq.client.examples;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class PingPong {

  public static void main(String[] args) throws IOException {
    if (args[0].equalsIgnoreCase("server")) {
      ServerSocket sc = new ServerSocket(9096);
      Socket socket = sc.accept();
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
      while (true) {
        outputStream.writeLong(inputStream.readLong());
        outputStream.flush();
      }
    } else {
      Socket socket = new Socket(args[1], 9096);
      DataInputStream inputStream = new DataInputStream(socket.getInputStream());
      DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());
      while (true) {
        outputStream.writeLong(System.nanoTime());
        outputStream.flush();
        long readLong = inputStream.readLong();
        System.out.println(System.nanoTime() - readLong);
      }
    }
  }

}
