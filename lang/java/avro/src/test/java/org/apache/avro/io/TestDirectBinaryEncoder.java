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
    public void writeBooleanThrowInputStream() {
      try {
        DirectBinaryEncoder encoder = new DirectBinaryEncoder(throwOutputStream);
        encoder.writeBoolean(true);
        Assert.fail();
      } catch (Exception ignored) {
        // Success
      }
    }
  }
}
