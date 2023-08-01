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
    private final ExpectedResult<Boolean> expected;
    private final InputStream input;

    public TestReadBoolean(TestParameters<Boolean> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters<Boolean>> getParameters() {
      byte[] randomData = new byte[]{0};
      while (randomData[0] == 1 || randomData[0] == 0) {
        new Random().nextBytes(randomData);
      }
      return Arrays.asList(
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters<>(new ExpectedResult<>(false, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParameters<>(new ExpectedResult<>(true, null), new ByteArrayInputStream(new byte[]{1}))
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
    private final ExpectedResult<Integer> expected;
    private final InputStream input;

    public TestReadInt(TestParameters<Integer> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters<Integer>> getParameters() {
      byte[] invalidValue = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
      return Arrays.asList(
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters<>(new ExpectedResult<>(0, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParameters<>(new ExpectedResult<>(-1, null), new ByteArrayInputStream(new byte[]{1})),
        new TestParameters<>(new ExpectedResult<>(1, null), new ByteArrayInputStream(new byte[]{2})),
        new TestParameters<>(new ExpectedResult<>(-2, null), new ByteArrayInputStream(new byte[]{3})),
        new TestParameters<>(new ExpectedResult<>(2, null), new ByteArrayInputStream(new byte[]{4})),
        new TestParameters<>(new ExpectedResult<>(-64, null), new ByteArrayInputStream(new byte[]{127})),
        new TestParameters<>(new ExpectedResult<>(64, null), new ByteArrayInputStream(new byte[]{-128, 1})),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(invalidValue))
      );
    }

    @Test
    public void readInt() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(input);
        Integer value = decoder.readInt();
        Assert.assertEquals(expected.getResult(), value);
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestReadLong {
    private final ExpectedResult<Long> expected;
    private final InputStream input;

    public TestReadLong(TestParameters<Long> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters<Long>> getParameters() {
      byte[] invalidValue = new byte[10];
      Arrays.fill(invalidValue, (byte) 0xFF);
      return Arrays.asList(
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters<>(new ExpectedResult<>(0L, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParameters<>(new ExpectedResult<>(-1L, null), new ByteArrayInputStream(new byte[]{1})),
        new TestParameters<>(new ExpectedResult<>(1L, null), new ByteArrayInputStream(new byte[]{2})),
        new TestParameters<>(new ExpectedResult<>(-2L, null), new ByteArrayInputStream(new byte[]{3})),
        new TestParameters<>(new ExpectedResult<>(2L, null), new ByteArrayInputStream(new byte[]{4})),
        new TestParameters<>(new ExpectedResult<>(-64L, null), new ByteArrayInputStream(new byte[]{127})),
        new TestParameters<>(new ExpectedResult<>(64L, null), new ByteArrayInputStream(new byte[]{-128, 1})),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(invalidValue))
      );
    }

    @Test
    public void readLong() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(input);
        Long value = decoder.readLong();
        Assert.assertEquals(expected.getResult(), value);
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestReadFloat {
    private final ExpectedResult<Float> expected;
    private final InputStream input;

    public TestReadFloat(TestParameters<Float> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters<Float>> getParameters() {
      // 3.14 in IEEE 754 little endian
      byte[] pi = new byte[]{(byte) 0xc3, (byte) 0xf5, (byte) 0x48, (byte) 0x40};
      return Arrays.asList(
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[]{4, 2, 0})),
        new TestParameters<>(new ExpectedResult<>(3.14f, null), new ByteArrayInputStream(pi))
      );
    }

    @Test
    public void readFloat() {
      try {
        BinaryDecoder decoder = new DirectBinaryDecoder(input);
        Float result = decoder.readFloat();
        Assert.assertEquals(expected.getResult(), result);
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestReadDouble {
    private final ExpectedResult<Double> expected;
    private final InputStream input;

    public TestReadDouble(TestParameters<Double> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParameters<Double>> getParameters() {
      // 3.14 in IEEE 754 double precision little endian
      byte[] pi = new byte[]{(byte) 0x1F, (byte) 0x85, (byte) 0xEB, (byte) 0x51, (byte) 0xB8, (byte) 0x1E, (byte) 0x09, (byte) 0x40};
      return Arrays.asList(
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ThrowInputStream()),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParameters<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5, 6, 7})),
        new TestParameters<>(new ExpectedResult<>(3.14, null), new ByteArrayInputStream(pi))
      );
    }

    @Test
    public void readDouble() {
      try {
        BinaryDecoder decoder = new DirectBinaryDecoder(input);
        Double result = decoder.readDouble();
        Assert.assertEquals(expected.getResult(), result);
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }
}
