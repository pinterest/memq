package com.pinterest.memq.commons.storage.ll;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.google.gson.JsonObject;

public class TestArrayPartition {

  @Test
  public void testWrites() throws IOException {
    File file = new File("target/test/arraypartition");
    file.mkdirs();
    file.deleteOnExit();
    Partition part = new ArrayPartition(file.getAbsolutePath(), 0);
    for (int i = 0; i < 2000; i++) {
      JsonObject obj = new JsonObject();
      obj.addProperty(Partition.TIMESTAMP, System.currentTimeMillis());
      obj.addProperty(Partition.SIZE, 1024);
      obj.addProperty("bucket", "test-xyzabctest");
      obj.addProperty("key", "test-xyzabctest/testfwerasdsadas/asdasdadsas");
      obj.addProperty("topic", "test-xyzabctest");
      part.write(obj);
    }
  }

}
