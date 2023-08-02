package org.apache.avro.utils;

import java.io.InputStream;

public class TestParametersDecoder<T> {
  private final ExpectedResult<T> expected;
  private final InputStream input;

  public TestParametersDecoder(ExpectedResult<T> expected, InputStream input) {
    this.expected = expected;
    this.input = input;
  }

  public ExpectedResult<T> expected() {
    return expected;
  }

  public InputStream input() {
    return input;
  }
}
