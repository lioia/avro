package org.apache.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestResolver {
  private final ExpectedResult<Resolver.Action.Type> expected;
  private final Schema writer;
  private final Schema reader;
  private final GenericData data;

  public TestResolver(TestParameters parameters) {
    this.expected = parameters.expected;
    this.writer = parameters.writer;
    this.reader = parameters.reader;
    this.data = parameters.data;
  }

  @Parameterized.Parameters
  public static Collection<TestParameters> getParameters() {
    return Arrays.asList(
      new TestParameters(new ExpectedResult<>(null, Exception.class), null, null, null)
    );
  }

  @Test
  public void resolve() {
    try {
      Resolver.Action result = Resolver.resolve(writer, reader, data);
      Assert.assertEquals(expected.getResult(), result.type);
    } catch (Exception e) {
      Assert.assertNotNull(this.expected.getException());
    }
  }

  public static class TestParameters {
    private final ExpectedResult<Resolver.Action.Type> expected;
    private final Schema writer;
    private final Schema reader;
    private final GenericData data;

    public TestParameters(ExpectedResult<Resolver.Action.Type> expected, Schema writer, Schema reader, GenericData data) {
      this.expected = expected;
      this.writer = writer;
      this.reader = reader;
      this.data = data;
    }
  }
}
