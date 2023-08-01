package org.apache.avro.io;

import org.apache.avro.utils.ExpectedResult;
import org.apache.avro.utils.TestParameters;
import org.apache.avro.utils.ThrowInputStream;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

@RunWith(Enclosed.class)
public class TestDirectBinaryDecoder {
  @RunWith(Parameterized.class)
  public static class TestReadBoolean {
    private final ExpectedResult<Object> expected;
    private final InputStream input;


    public TestReadBoolean(TestParameters parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters> getParameters() {
      byte[] randomData = new byte[]{0};
      while (randomData[0] == 1 || randomData[0] == 0) {
        new Random().nextBytes(randomData);
      }
      return Arrays.asList(
        new TestParameters(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters(new ExpectedResult<>(false, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParameters(new ExpectedResult<>(true, null), new ByteArrayInputStream(new byte[]{1}))
//        new TestParameters(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(randomData))
      );
    }

    @Test
    public void readBoolean() {
      try {
        BinaryDecoder decoder = new DirectBinaryDecoder(input);
        boolean result = decoder.readBoolean();
        Assert.assertEquals(expected.getResult(), result);
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestReadInt {
    private final ExpectedResult<Object> expected;
    private final InputStream input;

    public TestReadInt(TestParameters parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters> getParameters() {
      byte[] invalidValue = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
      return Arrays.asList(
        new TestParameters(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters(new ExpectedResult<>(0, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParameters(new ExpectedResult<>(-1, null), new ByteArrayInputStream(new byte[]{1})),
        new TestParameters(new ExpectedResult<>(1, null), new ByteArrayInputStream(new byte[]{2})),
        new TestParameters(new ExpectedResult<>(-2, null), new ByteArrayInputStream(new byte[]{3})),
        new TestParameters(new ExpectedResult<>(2, null), new ByteArrayInputStream(new byte[]{4})),
        new TestParameters(new ExpectedResult<>(-64, null), new ByteArrayInputStream(new byte[]{127})),
        new TestParameters(new ExpectedResult<>(64, null), new ByteArrayInputStream(new byte[]{-128, 1})),
        new TestParameters(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(invalidValue))
      );
    }

    @Test
    public void readInt() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(input);
        int value = decoder.readInt();
        Assert.assertEquals(expected.getResult(), value);
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }
}
