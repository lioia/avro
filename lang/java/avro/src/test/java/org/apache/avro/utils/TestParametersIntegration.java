package org.apache.avro.utils;

public class TestParametersIntegration<T> {
  private final ExpectedResult<T> expected;
  private final T parameter;

  public TestParametersIntegration(ExpectedResult<T> expected, T parameter) {
    this.expected = expected;
    this.parameter = parameter;
  }

  public ExpectedResult<T> expected() {
    return expected;
  }

  public T parameter() {
    return parameter;
  }
}
