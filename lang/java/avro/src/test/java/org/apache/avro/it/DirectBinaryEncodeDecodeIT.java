package org.apache.avro.it;

import org.apache.avro.io.*;
import org.apache.avro.utils.ExpectedResult;
import org.apache.avro.utils.TestParametersIntegration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Arrays;

@RunWith(Enclosed.class)
public class DirectBinaryEncodeDecodeIT {
  @RunWith(Parameterized.class)
  public static class BooleanIT {
    private final ExpectedResult<Boolean> expected;
    private final boolean parameter;

    public BooleanIT(TestParametersIntegration<Boolean> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersIntegration<Boolean>> getParameters() {
      return Arrays.asList(
        new TestParametersIntegration<>(new ExpectedResult<>(true, null), true),
        new TestParametersIntegration<>(new ExpectedResult<>(false, null), false)
      );
    }

    @Test
    public void bool() {
      try {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
        encoder.writeBoolean(parameter);
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(output.toByteArray()), null);
        boolean result = decoder.readBoolean();
        Assert.assertEquals(expected.getResult(), result);
      } catch (Exception e) {
        Assert.assertNotNull(expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class IntIT {
    private final ExpectedResult<Integer> expected;
    private final int parameter;

    public IntIT(TestParametersIntegration<Integer> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersIntegration<Integer>> getParameters() {
      return Arrays.asList(
        new TestParametersIntegration<>(new ExpectedResult<>(-1, null), -1),
        new TestParametersIntegration<>(new ExpectedResult<>(0, null), 0),
        new TestParametersIntegration<>(new ExpectedResult<>(1, null), 1)
      );
    }

    @Test
    public void integer() {
      try {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(output, null);
        encoder.writeInt(parameter);
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(new ByteArrayInputStream(output.toByteArray()), null);
        Integer result = decoder.readInt();
        Assert.assertEquals(expected.getResult(), result);
      } catch (Exception e) {
        Assert.assertNotNull(expected.getException());
      }
    }
  }
}
