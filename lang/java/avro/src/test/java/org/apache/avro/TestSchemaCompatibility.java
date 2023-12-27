package org.apache.avro;

import org.apache.avro.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class TestSchemaCompatibility {
  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() {
    Schema.Field aField = new Schema.Field("a", Schema.create(Schema.Type.INT));
    Schema.Field bField = new Schema.Field("b", Schema.create(Schema.Type.FLOAT));
    Schema.Field cField = new Schema.Field("c", Schema.create(Schema.Type.INT));
//    Schema.Field dField = new Schema.Field("d", Schema.create(Schema.Type.INT));
//    dField.addAlias("a");
    Schema record1 = Schema.createRecord("Record1", null, null, false, Arrays.asList(aField, bField));

//    ExpectedResult<Schema.Field> nullResult = new ExpectedResult<>(null, null);
    ExpectedResult<Schema.Field> exception = new ExpectedResult<>(null, Exception.class);
    return Arrays.asList(new Object[][]{
      {null, null, exception},
      {Schema.create(Schema.Type.INT), cField, exception},
      {record1, aField, new ExpectedResult<>(aField, null)},
//      // Improvements
//      {record1, cField, nullResult},
    });
  }

  private final Schema writerSchema;
  private final Schema.Field readerField;
  private final ExpectedResult<Schema.Field> expected;

  public TestSchemaCompatibility(Schema writerSchema, Schema.Field readerField, ExpectedResult<Schema.Field> expected) {
    this.writerSchema = writerSchema;
    this.readerField = readerField;
    this.expected = expected;
  }

  @Test
  public void lookupWriterFieldTest() {
    try {
      Schema.Field result = SchemaCompatibility.lookupWriterField(writerSchema, readerField);
      Assert.assertEquals(expected.getResult(), result);
    } catch (Exception ignored) {
      Assert.assertNotNull(expected.getException());
    }
  }
}
