package org.apache.avro.io;

import org.apache.avro.utils.ExpectedResult;
import org.apache.avro.utils.TestParametersDecoder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class TestDirectBinaryDecoder {
  @RunWith(Parameterized.class)
  public static class TestReadBoolean {
    private final ExpectedResult<Boolean> expected;
    private final InputStream input;

    public TestReadBoolean(TestParametersDecoder<Boolean> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersDecoder<Boolean>> getParameters() {
      byte[] randomData = new byte[]{0};
      while (randomData[0] == 1 || randomData[0] == 0) {
        new Random().nextBytes(randomData);
      }
      return Arrays.asList(
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParametersDecoder<>(new ExpectedResult<>(false, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParametersDecoder<>(new ExpectedResult<>(true, null), new ByteArrayInputStream(new byte[]{1}))
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

    public TestReadInt(TestParametersDecoder<Integer> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersDecoder<Integer>> getParameters() {
      byte[] invalidValue = new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
      return Arrays.asList(
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParametersDecoder<>(new ExpectedResult<>(0, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParametersDecoder<>(new ExpectedResult<>(-1, null), new ByteArrayInputStream(new byte[]{1})),
        new TestParametersDecoder<>(new ExpectedResult<>(1, null), new ByteArrayInputStream(new byte[]{2})),
        new TestParametersDecoder<>(new ExpectedResult<>(-2, null), new ByteArrayInputStream(new byte[]{3})),
        new TestParametersDecoder<>(new ExpectedResult<>(2, null), new ByteArrayInputStream(new byte[]{4})),
        new TestParametersDecoder<>(new ExpectedResult<>(-64, null), new ByteArrayInputStream(new byte[]{127})),
        new TestParametersDecoder<>(new ExpectedResult<>(64, null), new ByteArrayInputStream(new byte[]{-128, 1})),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(invalidValue))
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

    public TestReadLong(TestParametersDecoder<Long> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersDecoder<Long>> getParameters() {
      byte[] invalidValue = new byte[10];
      Arrays.fill(invalidValue, (byte) 0xFF);
      return Arrays.asList(
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParametersDecoder<>(new ExpectedResult<>(0L, null), new ByteArrayInputStream(new byte[]{0})),
        new TestParametersDecoder<>(new ExpectedResult<>(-1L, null), new ByteArrayInputStream(new byte[]{1})),
        new TestParametersDecoder<>(new ExpectedResult<>(1L, null), new ByteArrayInputStream(new byte[]{2})),
        new TestParametersDecoder<>(new ExpectedResult<>(-2L, null), new ByteArrayInputStream(new byte[]{3})),
        new TestParametersDecoder<>(new ExpectedResult<>(2L, null), new ByteArrayInputStream(new byte[]{4})),
        new TestParametersDecoder<>(new ExpectedResult<>(-64L, null), new ByteArrayInputStream(new byte[]{127})),
        new TestParametersDecoder<>(new ExpectedResult<>(64L, null), new ByteArrayInputStream(new byte[]{-128, 1})),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(invalidValue))
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

    public TestReadFloat(TestParametersDecoder<Float> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersDecoder<Float>> getParameters() {
      // 3.14 in IEEE 754 little endian
      byte[] pi = new byte[]{(byte) 0xc3, (byte) 0xf5, (byte) 0x48, (byte) 0x40};
      return Arrays.asList(
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[]{4, 2, 0})),
        new TestParametersDecoder<>(new ExpectedResult<>(3.14f, null), new ByteArrayInputStream(pi))
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

    public TestReadDouble(TestParametersDecoder<Double> parameters) {
      this.expected = parameters.expected();
      this.input = parameters.input();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersDecoder<Double>> getParameters() {
      // 3.14 in IEEE 754 double precision little endian
      byte[] pi = new byte[]{(byte) 0x1F, (byte) 0x85, (byte) 0xEB, (byte) 0x51, (byte) 0xB8, (byte) 0x1E, (byte) 0x09, (byte) 0x40};
      return Arrays.asList(
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), null),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[0])),
        new TestParametersDecoder<>(new ExpectedResult<>(null, Exception.class), new ByteArrayInputStream(new byte[]{1, 2, 3, 4, 5, 6, 7})),
        new TestParametersDecoder<>(new ExpectedResult<>(3.14, null), new ByteArrayInputStream(pi))
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


  @RunWith(Parameterized.class)
  public static class TestReadBytes {
    private final ExpectedResult<ByteBuffer> expected;
    private final ByteBuffer old;
    private final InputStream input;

    public TestReadBytes(TestParametersReadBytes parameters) {
      this.expected = parameters.expected;
      this.old = parameters.old;
      byte[] bytes = new byte[parameters.length.length + parameters.bytes.length];
      System.arraycopy(parameters.length, 0, bytes, 0, parameters.length.length);
      System.arraycopy(parameters.bytes, 0, bytes, parameters.length.length, parameters.bytes.length);
      input = new ByteArrayInputStream(bytes);
    }

    private static byte[] fromLong(long value) throws IOException {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      DirectBinaryEncoder encoder = new DirectBinaryEncoder(output);
      encoder.writeLong(value);
      return output.toByteArray();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersReadBytes> getParameters() throws IOException {
      byte[] length0 = new byte[]{0};
      byte[] lengthMinus1 = new byte[]{1};
      byte[] length1 = new byte[]{2};
      byte[] zeroElement = new byte[0];
      ByteBuffer zeroElementBuffer = ByteBuffer.wrap(zeroElement);
      byte[] oneElement = new byte[]{1};
      ByteBuffer oneElementBuffer = ByteBuffer.wrap(oneElement);
      byte[] twoElement = new byte[]{1, 2};
      ByteBuffer arbitraryBuffer = ByteBuffer.wrap(twoElement);
      byte[] maxLength = fromLong(BinaryDecoder.MAX_ARRAY_SIZE);
      byte[] maxLengthPlus1 = fromLong(BinaryDecoder.MAX_ARRAY_SIZE + 1);
      return Arrays.asList(
        new TestParametersReadBytes(new ExpectedResult<>(null, Exception.class), null, lengthMinus1, zeroElement),
        new TestParametersReadBytes(new ExpectedResult<>(zeroElementBuffer, null), zeroElementBuffer, length0, zeroElement),
//        new TestParametersReadBytes(new ExpectedResult<>(zeroElementBuffer, null), arbitraryBuffer, length0, oneElement),
        new TestParametersReadBytes(new ExpectedResult<>(oneElementBuffer, null), zeroElementBuffer, length1, oneElement),
        new TestParametersReadBytes(new ExpectedResult<>(zeroElementBuffer, Exception.class), zeroElementBuffer, length1, zeroElement),
        new TestParametersReadBytes(new ExpectedResult<>(oneElementBuffer, null), null, length1, twoElement),
        // Coverage Improvements
//        new TestParametersReadBytes(new ExpectedResult<>(null, Exception.class), null, maxLength, zeroElement),
        new TestParametersReadBytes(new ExpectedResult<>(null, Exception.class), zeroElementBuffer, maxLengthPlus1, oneElement)
      );
    }

    @Test
    public void readBytes() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(input);
        ByteBuffer result = decoder.readBytes(old);
        Assert.assertArrayEquals(expected.getResult().array(), result.array());
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }

    public static class TestParametersReadBytes {
      private final ExpectedResult<ByteBuffer> expected;
      private final ByteBuffer old;
      private final byte[] length;
      private final byte[] bytes;

      public TestParametersReadBytes(ExpectedResult<ByteBuffer> expected, ByteBuffer old, byte[] length, byte[] bytes) {
        this.expected = expected;
        this.old = old;
        this.length = length;
        this.bytes = bytes;
      }
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class TestReadThrowInputStream {
    @Mock
    private InputStream throwInputStream;

    @Before
    public void setup() throws IOException {
      lenient().when(throwInputStream.read()).thenThrow(IOException.class);
      lenient().when(throwInputStream.read(any())).thenThrow(IOException.class);
      lenient().when(throwInputStream.read(any(), anyInt(), anyInt())).thenThrow(IOException.class);
    }

    @Test
    public void readBooleanThrowInputStream() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(throwInputStream);
        decoder.readBoolean();
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void readIntThrowInputStream() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(throwInputStream);
        decoder.readInt();
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void readLongThrowInputStream() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(throwInputStream);
        decoder.readLong();
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void readFloatThrowInputStream() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(throwInputStream);
        decoder.readFloat();
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void readDoubleThrowInputStream() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(throwInputStream);
        decoder.readDouble();
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void readBytesThrowInputStream() {
      try {
        DirectBinaryDecoder decoder = new DirectBinaryDecoder(throwInputStream);
        decoder.readBytes(null);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }
  }
}
