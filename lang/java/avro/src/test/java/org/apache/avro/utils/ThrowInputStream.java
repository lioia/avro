package org.apache.avro.utils;

import java.io.IOException;
import java.io.InputStream;

public class ThrowInputStream extends InputStream {
  @Override
  public int read() throws IOException {
    throw new IOException();
  }
}
