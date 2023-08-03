package org.apache.avro.utils;

import java.io.OutputStream;
import java.nio.ByteBuffer;

public class TestParametersEncoder<T> {
  private final ExpectedResult<ByteBuffer> expected;
  private final T parameter;
  private final OutputStream output;

  public TestParametersEncoder(ExpectedResult<ByteBuffer> expected, T parameter, OutputStream output) {
    this.expected = expected;
    this.parameter = parameter;
    this.output = output;
  }

  public ExpectedResult<ByteBuffer> expected() {
    return expected;
  }

  public T parameter() {
    return parameter;
  }

  public OutputStream output() {
    return output;
  }
}
