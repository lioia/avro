package org.apache.avro.io;

import org.apache.avro.utils.ExpectedResult;
import org.apache.avro.utils.TestParametersEncoder;
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
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class TestDirectBinaryEncoder {
  @RunWith(Parameterized.class)
  public static class TestWriteBoolean {
    private final ExpectedResult<ByteBuffer> expected;
    private final boolean parameter;
    private final OutputStream output;

    public TestWriteBoolean(TestParametersEncoder<Boolean> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
      this.output = parameters.output();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersEncoder<Boolean>> getParameters() {
      ByteBuffer falseBuffer = ByteBuffer.allocate(1).put((byte) 0);
      ByteBuffer trueBuffer = ByteBuffer.allocate(1).put((byte) 1);
      return Arrays.asList(
        new TestParametersEncoder<>(new ExpectedResult<>(null, Exception.class), false, null),
        new TestParametersEncoder<>(new ExpectedResult<>(falseBuffer, null), false, new ByteArrayOutputStream()),
        new TestParametersEncoder<>(new ExpectedResult<>(trueBuffer, null), true, new ByteArrayOutputStream())
      );
    }

    @Test
    public void readBoolean() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(output);
        encoder.writeBoolean(parameter);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) output;
        Assert.assertArrayEquals(expected.getResult().array(), outputStream.toByteArray());
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestWriteInt {
    private final ExpectedResult<ByteBuffer> expected;
    private final int parameter;
    private final OutputStream output;

    public TestWriteInt(TestParametersEncoder<Integer> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
      this.output = parameters.output();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersEncoder<Integer>> getParameters() {
      byte[] byteMinus1 = new byte[]{0x01};
      byte[] byte64 = new byte[]{(byte) 0x80, 0x01};
      byte[] byte512 = new byte[]{(byte) 0x80, 0x08};
      byte[] byte8192 = new byte[]{(byte) 0x80, (byte) 0x80, 0x01};
      byte[] byteMinusMaxLengthMinus1 = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F};
      return Arrays.asList(
        new TestParametersEncoder<>(new ExpectedResult<>(null, Exception.class), 0, null),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byteMinus1), null), -1, new ByteArrayOutputStream()),
        // Improvements
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byte64), null), 64, new ByteArrayOutputStream()),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byte512), null), 512, new ByteArrayOutputStream()),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byte8192), null), 8192, new ByteArrayOutputStream()),
        // PIT Improvements
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byteMinusMaxLengthMinus1), null), -Integer.MAX_VALUE - 1, new ByteArrayOutputStream())
      );
    }

    @Test
    public void readInt() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(output);
        encoder.writeInt(parameter);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) output;
        Assert.assertArrayEquals(expected.getResult().array(), outputStream.toByteArray());
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestWriteLong {
    private final ExpectedResult<ByteBuffer> expected;
    private final long parameter;
    private final OutputStream output;

    public TestWriteLong(TestParametersEncoder<Long> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
      this.output = parameters.output();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersEncoder<Long>> getParameters() {
      byte[] byteMinus1 = new byte[]{0x01};
      byte[] byte64 = new byte[]{(byte) 0x80, 0x01};
      return Arrays.asList(
        new TestParametersEncoder<>(new ExpectedResult<>(null, Exception.class), 0L, null),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byteMinus1), null), -1L, new ByteArrayOutputStream()),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byte64), null), 64L, new ByteArrayOutputStream())
      );
    }

    @Test
    public void readLong() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(output);
        encoder.writeLong(parameter);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) output;
        Assert.assertArrayEquals(expected.getResult().array(), outputStream.toByteArray());
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestWriteFloat {
    private final ExpectedResult<ByteBuffer> expected;
    private final float parameter;
    private final OutputStream output;

    public TestWriteFloat(TestParametersEncoder<Float> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
      this.output = parameters.output();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersEncoder<Float>> getParameters() {
      byte[] byteMinus0dot1 = new byte[]{(byte) 0xCD, (byte) 0xCC, (byte) 0xCC, (byte) 0xBD};
      return Arrays.asList(
        new TestParametersEncoder<>(new ExpectedResult<>(null, Exception.class), 0f, null),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byteMinus0dot1), null), -0.1f, new ByteArrayOutputStream())
      );
    }

    @Test
    public void readFloat() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(output);
        encoder.writeFloat(parameter);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) output;
        Assert.assertArrayEquals(expected.getResult().array(), outputStream.toByteArray());
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestWriteDouble {
    private final ExpectedResult<ByteBuffer> expected;
    private final double parameter;
    private final OutputStream output;

    public TestWriteDouble(TestParametersEncoder<Double> parameters) {
      this.expected = parameters.expected();
      this.parameter = parameters.parameter();
      this.output = parameters.output();
    }

    @Parameterized.Parameters
    public static Collection<TestParametersEncoder<Double>> getParameters() {
      byte[] byteMinus0dot1 = new byte[]{(byte) 0x9A, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0x99, (byte) 0xB9, (byte) 0xBF};
      return Arrays.asList(
        new TestParametersEncoder<>(new ExpectedResult<>(null, Exception.class), 0.0, null),
        new TestParametersEncoder<>(new ExpectedResult<>(ByteBuffer.wrap(byteMinus0dot1), null), -0.1, new ByteArrayOutputStream())
      );
    }

    @Test
    public void readDouble() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(output);
        encoder.writeDouble(parameter);
        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) output;
        Assert.assertArrayEquals(expected.getResult().array(), outputStream.toByteArray());
      } catch (Exception e) {
        Assert.assertNotNull(this.expected.getException());
      }
    }
  }

  @RunWith(MockitoJUnitRunner.class)
  public static class TestWriteThrowOutputStream {
    @Mock
    private OutputStream throwOutputStream;

    @Before
    public void setup() throws IOException {
      lenient().doThrow(IOException.class).when(throwOutputStream).write(anyInt());
      lenient().doThrow(IOException.class).when(throwOutputStream).write(any(byte[].class));
      lenient().doThrow(IOException.class).when(throwOutputStream).write(any(byte[].class), anyInt(), anyInt());
    }

    @Test
    public void writeBooleanThrowOutputStream() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(throwOutputStream);
        encoder.writeBoolean(true);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void writeIntThrowOutputStream() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(throwOutputStream);
        encoder.writeInt(1);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void writeLongThrowOutputStream() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(throwOutputStream);
        encoder.writeLong(1L);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void writeFloatThrowOutputStream() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(throwOutputStream);
        encoder.writeFloat(0.1f);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }

    @Test
    public void writeDoubleThrowOutputStream() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(throwOutputStream);
        encoder.writeDouble(0.1);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }
  }
}
