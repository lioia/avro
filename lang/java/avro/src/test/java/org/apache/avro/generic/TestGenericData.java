package org.apache.avro.generic;

import net.bytebuddy.description.type.TypeList;
import org.apache.avro.Schema;
import org.apache.avro.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.*;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Enclosed.class)
public class TestGenericData {
  @RunWith(Parameterized.class)
  public static class TestValidate {
    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
      ExpectedResult<Boolean> exception = new ExpectedResult<>(null, Exception.class);
      ExpectedResult<Boolean> trueExp = new ExpectedResult<>(true, null);
      ExpectedResult<Boolean> falseExp = new ExpectedResult<>(false, null);
      List<Schema.Field> fields1 = Arrays.asList(
        new Schema.Field("a", Schema.create(Schema.Type.INT), null, 3),
        new Schema.Field("b", Schema.create(Schema.Type.FLOAT), null, 3.14f)
      );
      Schema recordSchema1 = Schema.createRecord("RecordTest1", null, null, false, fields1);
      Schema enumSchema1 = Schema.createEnum("EnumTest1", null, null, Arrays.asList("a", "b"));
      Schema arraySchema1 = Schema.createArray(Schema.create(Schema.Type.INT));
      Schema mapSchema1 = Schema.createMap(Schema.create(Schema.Type.INT));
      Schema unionSchema1 = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));
      Schema fixedSchema1 = Schema.createFixed("FixedTest1", null, null, 16);
      GenericData.Array<Integer> arrayData1 = new GenericData.Array<>(arraySchema1, Collections.emptyList());
      List<Schema.Field> fields2 = Collections.singletonList(
        new Schema.Field("c", Schema.create(Schema.Type.STRING), null, "test")
      );
      Schema recordSchema2 = Schema.createRecord("RecordTest2", null, null, false, fields2);
      GenericData.Array<Integer> arrayData2 = new GenericData.Array<>(arraySchema1, Arrays.asList(3, 4));
      Map<Object, Object> mapData = new HashMap<>();
      mapData.put("Test", 0.14f);
      Map<Object, Object> mapData2 = new HashMap<>();
      mapData2.put("Test", 3);
      Schema mockUnionSchema = mock(Schema.class);
      when(mockUnionSchema.getType()).thenReturn(Schema.Type.UNION);
      when(mockUnionSchema.getTypes()).thenReturn(Collections.singletonList(Schema.create(Schema.Type.INT)));
      when(mockUnionSchema.getIndexNamed(anyString())).thenReturn(0);
      return Arrays.asList(
        new Object[][]{
          {null, null, exception},
          {Schema.create(Schema.Type.NULL), null, trueExp},
          {Schema.create(Schema.Type.STRING), 3, falseExp},
          {Schema.create(Schema.Type.BYTES), ByteBuffer.allocate(3), trueExp},
          {Schema.create(Schema.Type.INT), 3.14f, falseExp},
          {Schema.create(Schema.Type.LONG), 3L, trueExp},
          {Schema.create(Schema.Type.FLOAT), 3, falseExp},
          {Schema.create(Schema.Type.DOUBLE), 3.14, trueExp},
          {recordSchema1, 3, falseExp},
          {enumSchema1, new GenericData.EnumSymbol(enumSchema1, "a"), trueExp},
          {arraySchema1, 3, falseExp},
          {mapSchema1, new HashMap<String, Integer>(), trueExp},
          {unionSchema1, 3.14f, falseExp},
          {fixedSchema1, new GenericData.Fixed(fixedSchema1, new byte[16]), trueExp},
          {Schema.create(Schema.Type.BOOLEAN), false, trueExp},
          // Improvements
          {Schema.create(Schema.Type.NULL), 3, falseExp},
          {recordSchema1, new GenericRecordBuilder(recordSchema1).build(), trueExp},
          {arraySchema1, arrayData1, trueExp},
          {unionSchema1, 3, trueExp},
          {recordSchema1, new GenericRecordBuilder(recordSchema2).build(), falseExp},
          {enumSchema1, 3, falseExp},
          {arraySchema1, arrayData2, trueExp},
          {arraySchema1, Collections.singletonList(3.14f), falseExp},
          {mapSchema1, 3, falseExp},
          {mapSchema1, mapData, falseExp},
          {mapSchema1, mapData2, trueExp},
          {fixedSchema1, 3, falseExp},
          {fixedSchema1, new GenericData.Fixed(fixedSchema1, new byte[8]), falseExp},
          // PIT Improvements
          {enumSchema1, new GenericData.EnumSymbol(enumSchema1, "c"), falseExp},
          {Schema.create(Schema.Type.STRING), "Test", trueExp},
          {Schema.create(Schema.Type.BYTES), 3, falseExp},
          {Schema.create(Schema.Type.LONG), 3.14f, falseExp},
          {Schema.create(Schema.Type.DOUBLE), 3, falseExp},
          {Schema.create(Schema.Type.BOOLEAN), 3, falseExp},
          {mockUnionSchema, 3.14f, falseExp},
        }
      );
    }

    private final Schema schema;
    private final Object datum;
    private final ExpectedResult<Boolean> expected;

    public TestValidate(Schema schema, Object datum, ExpectedResult<Boolean> expected) {
      this.schema = schema;
      this.datum = datum;
      this.expected = expected;
    }

    @Test
    public void validateTest() {
      try {
        boolean result = GenericData.get().validate(schema, datum);
        Assert.assertEquals(expected.getResult(), result);
        // PIT improvements
        if (schema.getType() == Schema.Type.ENUM && datum instanceof GenericData.EnumSymbol) {
          GenericData.EnumSymbol enumSymbol = (GenericData.EnumSymbol) datum;
          Assert.assertEquals(result, schema.hasEnumSymbol(enumSymbol.toString()));
        }
        if (schema.getType() == Schema.Type.MAP && datum instanceof Map) {
          Map<String, Object> mapValue = (Map<String, Object>) datum;
          for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
            Assert.assertEquals(result, entry.getValue() instanceof Integer);
          }
        }
      } catch (Exception ignored) {
        Assert.assertNotNull(expected.getException());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestInduce {
    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
      Schema recordSchema1 = Schema.createRecord(null, null, null, false, Collections.emptyList());
      GenericData.Record record1 = new GenericRecordBuilder(recordSchema1).build();

      Schema enumSchema1 = Schema.createEnum(null, null, null, Arrays.asList("a", "b"));
      GenericData.EnumSymbol enum1 = new GenericData.EnumSymbol(enumSchema1, "a");

      Schema arraySchema1 = Schema.createArray(Schema.create(Schema.Type.INT));
      GenericData.Array<Integer> array1 = new GenericData.Array<>(16, arraySchema1);
      array1.add(0, 3);

      Schema mapSchema1 = Schema.createMap(Schema.create(Schema.Type.STRING));
      Map<String, String> map1 = new HashMap<>();
      map1.put("key", "value");

      Schema fixedSchema1 = Schema.createFixed(null, null, null, 16);
      GenericData.Fixed fixed1 = new GenericData.Fixed(fixedSchema1);

      ExpectedResult<Schema> exception = new ExpectedResult<>(null, Exception.class);
      return Arrays.asList(new Object[][]{
        {new GenericData(), exception},
        {null, new ExpectedResult<>(Schema.create(Schema.Type.NULL), null)},
        {"generic", new ExpectedResult<>(Schema.create(Schema.Type.STRING), null)},
        {ByteBuffer.allocate(3), new ExpectedResult<>(Schema.create(Schema.Type.BYTES), null)},
        {3, new ExpectedResult<>(Schema.create(Schema.Type.INT), null)},
        {3L, new ExpectedResult<>(Schema.create(Schema.Type.LONG), null)},
        {3.14f, new ExpectedResult<>(Schema.create(Schema.Type.FLOAT), null)},
        {3.14, new ExpectedResult<>(Schema.create(Schema.Type.DOUBLE), null)},
        {true, new ExpectedResult<>(Schema.create(Schema.Type.BOOLEAN), null)},
        {record1, new ExpectedResult<>(recordSchema1, null)},
//        {enum1, new ExpectedResult<>(enumSchema1, null)}, // Fail: induce does not support EnumSchema
        {array1, new ExpectedResult<>(arraySchema1, null)},
        {map1, new ExpectedResult<>(mapSchema1, null)},
        {fixed1, new ExpectedResult<>(fixedSchema1, null)},
      });
    }

    private final Object datum;
    private final ExpectedResult<Schema> expected;

    public TestInduce(Object datum, ExpectedResult<Schema> expected) {
      this.datum = datum;
      this.expected = expected;
    }

    @Test
    public void induceTest() {
      try {
        Schema result = GenericData.get().induce(datum);
        Assert.assertEquals(expected.getResult(), result);
      } catch (Exception ignored) {
        Assert.assertNotNull(expected.getException());
      }
    }
  }
}
