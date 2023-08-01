package org.apache.avro.utils;

import java.io.InputStream;

public class TestParameters<T> {
  private final ExpectedResult<T> expected;
  private final InputStream input;

  public TestParameters(ExpectedResult<T> expected, InputStream input) {
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
