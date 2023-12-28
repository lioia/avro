package org.apache.avro;

import org.apache.avro.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Enclosed.class)
public class TestSchemaCompatibility {
  @RunWith(Parameterized.class)
  public static class TestLookupWriterField {
    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
      Schema.Field aField = new Schema.Field("a", Schema.create(Schema.Type.INT));
      Schema.Field bField = new Schema.Field("b", Schema.create(Schema.Type.FLOAT));
      Schema.Field cField = new Schema.Field("c", Schema.create(Schema.Type.INT));
      Schema.Field dField = new Schema.Field("d", Schema.create(Schema.Type.INT));
      dField.addAlias("a");
      Schema.Field eField = new Schema.Field("e", Schema.create(Schema.Type.INT));
      eField.addAlias("c");
      Schema.Field fField = new Schema.Field("f", Schema.create(Schema.Type.INT));
      fField.addAlias("a");
      fField.addAlias("b");
      Schema record1 = Schema.createRecord("Record1", null, null, false, Arrays.asList(aField, bField));

      ExpectedResult<Schema.Field> nullResult = new ExpectedResult<>(null, null);
      ExpectedResult<Schema.Field> exception = new ExpectedResult<>(null, Exception.class);
      return Arrays.asList(new Object[][]{
        {null, null, exception},
        {Schema.create(Schema.Type.INT), cField, exception},
        {record1, aField, new ExpectedResult<>(aField, null)},
        // Improvements
        {record1, cField, nullResult},
        {record1, dField, new ExpectedResult<>(aField, null)},
        {record1, eField, nullResult},
        {record1, fField, exception},
      });
    }

    private final Schema writerSchema;
    private final Schema.Field readerField;
    private final ExpectedResult<Schema.Field> expected;

    public TestLookupWriterField(Schema writerSchema, Schema.Field readerField, ExpectedResult<Schema.Field> expected) {
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

  @RunWith(Parameterized.class)
  public static class TestCheckReaderWriterCompatibility {
    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
      Schema record1 = Schema.createRecord("Record1", null, null, false, Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT)),
        new Schema.Field("b", Schema.create(Schema.Type.FLOAT))
      ));
      Schema enum1 = Schema.createEnum("Enum1", null, null, Arrays.asList("a", "b"));
      Schema array1 = Schema.createArray(Schema.create(Schema.Type.INT));
      Schema map1 = Schema.createMap(Schema.create(Schema.Type.INT));
      Schema union1 = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));
      Schema union2 = Schema.createUnion(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.BOOLEAN));
      Schema fixed1 = Schema.createFixed("Fixed1", null, null, 16);
      ExpectedResult<SchemaCompatibility.SchemaCompatibilityType> compatible = new ExpectedResult<>(SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE, null);
      ExpectedResult<SchemaCompatibility.SchemaCompatibilityType> incompatible = new ExpectedResult<>(SchemaCompatibility.SchemaCompatibilityType.INCOMPATIBLE, null);
      ExpectedResult<SchemaCompatibility.SchemaCompatibilityType> exception = new ExpectedResult<>(null, Exception.class);
      return Arrays.asList(new Object[][]{
        {null, null, exception},
        {Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.NULL), compatible},
        {Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.INT), incompatible},
        {Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.BYTES), compatible},
        {Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING), incompatible},
        {Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.LONG), compatible},
        {Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.STRING), incompatible},
        {Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.DOUBLE), compatible},
        {Schema.create(Schema.Type.BOOLEAN), Schema.create(Schema.Type.STRING), incompatible},
        {record1, record1, compatible},
        {enum1, Schema.create(Schema.Type.INT), incompatible},
        {array1, array1, compatible},
        {map1, Schema.create(Schema.Type.INT), incompatible},
        {union1, union1, compatible},
        {fixed1, Schema.create(Schema.Type.INT), incompatible},
        // Improvements
        {map1, map1, compatible},
        {fixed1, fixed1, compatible},
        {enum1, enum1, compatible},
        {union1, union2, incompatible},
        {Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT), incompatible},
        {Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.FLOAT), incompatible},
        {Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.FLOAT), compatible},
        {Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.FLOAT), incompatible},
        {array1, Schema.create(Schema.Type.FLOAT), incompatible},
        {record1, Schema.create(Schema.Type.FLOAT), incompatible},
        {Schema.create(Schema.Type.INT), union1, incompatible},
        {Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.INT), compatible},
        {Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.INT), compatible},
        {Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.LONG), compatible},
        {Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.INT), compatible},
        {Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.LONG), compatible},
        {Schema.create(Schema.Type.DOUBLE), Schema.create(Schema.Type.STRING), incompatible},
        {Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.STRING), compatible},
        {Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.BYTES), compatible},
      });
    }

    private final Schema reader;
    private final Schema writer;
    private final ExpectedResult<SchemaCompatibility.SchemaCompatibilityType> expected;

    public TestCheckReaderWriterCompatibility(Schema reader, Schema writer, ExpectedResult<SchemaCompatibility.SchemaCompatibilityType> expected) {
      this.reader = reader;
      this.writer = writer;
      this.expected = expected;
    }

    @Test
    public void checkReaderWriterCompatibilityTest() {
      try {
        SchemaCompatibility.SchemaPairCompatibility result = SchemaCompatibility.checkReaderWriterCompatibility(reader, writer);
        Assert.assertEquals(expected.getResult(), result.getType());
      } catch (Exception ignored) {
        Assert.assertNotNull(expected.getException());
      }
    }
  }
}
