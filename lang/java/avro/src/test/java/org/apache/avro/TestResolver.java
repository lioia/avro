package org.apache.avro;

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

  public TestResolver(TestParameters parameters) {
    this.expected = parameters.expected;
    this.writer = parameters.writer;
    this.reader = parameters.reader;
  }

  @Parameterized.Parameters
  public static Collection<TestParameters> getParameters() {
    return Arrays.asList(
      new TestParameters(new ExpectedResult<>(null, Exception.class), null, null),
      new TestParameters(new ExpectedResult<>(Resolver.Action.Type.DO_NOTHING, null), SchemaBuilder.builder().bytesType(), SchemaBuilder.builder().bytesType())
    );
  }

  @Test
  public void resolve() {
    try {
      Resolver.Action result = Resolver.resolve(writer, reader);
      Assert.assertEquals(expected.getResult(), result.type);
    } catch (Exception e) {
      Assert.assertNotNull(this.expected.getException());
    }
  }

  public static class TestParameters {
    private final ExpectedResult<Resolver.Action.Type> expected;
    private final Schema writer;
    private final Schema reader;

    public TestParameters(ExpectedResult<Resolver.Action.Type> expected, Schema writer, Schema reader) {
      this.expected = expected;
      this.writer = writer;
      this.reader = reader;
    }
  }
}
