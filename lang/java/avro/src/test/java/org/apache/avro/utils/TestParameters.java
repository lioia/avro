package org.apache.avro.utils;

import java.io.InputStream;

public class TestParameters {
  private final ExpectedResult<Object> expected;
  private final InputStream input;

  public TestParameters(ExpectedResult<Object> expected, InputStream input) {
    this.expected = expected;
    this.input = input;
  }

  public ExpectedResult<Object> expected() {
    return expected;
  }

  public InputStream input() {
    return input;
  }
}
