package org.apache.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.avro.utils.ExpectedResult;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.apache.avro.SchemaCompatibility.*;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class TestSchema {
  private static JsonNode createNodeFromString(String str) throws JsonProcessingException {
    ObjectMapper mapper = new JsonMapper();
    return mapper.readTree(str);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getParameters() throws JsonProcessingException {
    ExpectedResult<SchemaCompatibilityType> exception = new ExpectedResult<>(null, Exception.class);
    Schema.Names validNames = new Schema.Names("com.example");
    Schema.Names mockNames = mock(Schema.Names.class);
    when(mockNames.space()).thenThrow(RuntimeException.class);

    List<Schema.Field> recordFields = Arrays.asList(
      new Schema.Field("a", Schema.create(Schema.Type.INT)),
      new Schema.Field("b", Schema.create(Schema.Type.FLOAT))
    );
    Schema recordSchema = Schema.createRecord("RecordTest", null, "com.example", false, recordFields);
    Schema enumSchema = Schema.createEnum("EnumTest", null, "com.example", Arrays.asList("a", "b"));
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    Schema unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));
    Schema fixedSchema = Schema.createFixed("FixedTest", null, "com.example", 16);
    List<Schema.Field> recordFields2 = Collections.singletonList(new Schema.Field("a", Schema.create(Schema.Type.BOOLEAN), null, true, Schema.Field.Order.IGNORE));
    Schema recordSchema2 = Schema.createRecord("RecordTest2", null, "com.example", false, recordFields2);
    List<Schema.Field> recordFields3 = Collections.singletonList(new Schema.Field("pi", Schema.create(Schema.Type.DOUBLE), null, 3.14));
    Schema recordSchema3 = Schema.createRecord("RecordTest3", null, "com.example", false, recordFields3);
    List<Schema.Field> recordFields4 = Collections.singletonList(new Schema.Field("twoPi", Schema.create(Schema.Type.FLOAT), null, 6.28));
    Schema recordSchema4 = Schema.createRecord("RecordTest4", null, "com.example", false, recordFields4);
    List<Schema.Field> recordFields5 = Collections.singletonList(new Schema.Field("twoPi", Schema.create(Schema.Type.FLOAT), null, 6.28));
    Schema recordSchema5 = Schema.createRecord("RecordTest5", null, "com.example", false, recordFields5);
    Schema.Field uuidField = new Schema.Field("uuid", Schema.create(Schema.Type.STRING));
    uuidField.schema().setLogicalType(LogicalTypes.uuid());
    uuidField.schema().addProp("logicalType", "uuid");
    uuidField.addProp("additional", "value");
    Schema recordSchema6 = Schema.createRecord("RecordTest6", null, "com.example", false, Collections.singletonList(uuidField));
    Schema.Field field7 = new Schema.Field("uuidFake", Schema.create(Schema.Type.STRING));
    field7.addProp("logicalType", "uuid");
    Schema recordSchema7 = Schema.createRecord("RecordTest7", null, "com.example", false, Collections.singletonList(field7));
    Schema enumSchema2 = Schema.createEnum("EnumTest2", null, "com.example", Arrays.asList("c", "d"));
    Schema errorSchema = Schema.createRecord("ErrorTest", null, "com.example", true, Collections.singletonList(new Schema.Field("ErrorField", Schema.create(Schema.Type.STRING))));
    errorSchema.addAlias("alias");
    Schema recordSchema9 = Schema.createRecord("RecordTest9", null, "com.example", false, Collections.singletonList(new Schema.Field("Field9", Schema.create(Schema.Type.STRING))));
    recordSchema9.addProp("additional", "value");
    Schema enumSchema3 = Schema.createEnum("EnumTest3", null, "com.example", Arrays.asList("a", "b"));
    enumSchema3.addAlias("Enum3");
    enumSchema3.addProp("additional", "value");
    Schema arraySchema2 = Schema.createArray(Schema.create(Schema.Type.INT));
    arraySchema2.addProp("additional", "value");
    Schema mapSchema2 = Schema.createMap(Schema.create(Schema.Type.INT));
    mapSchema2.addProp("additional", "value");
    Schema fixedSchema2 = Schema.createFixed("FixedTest2", null, "com.example", 16);
    fixedSchema2.addAlias("Fixed2");
    fixedSchema2.addProp("additional", "value");
    Schema.Field field10 = new Schema.Field("Field10", Schema.createUnion(enumSchema));
    Schema recordSchema10 = Schema.createRecord("RecordTest10", null, "com.example", false, Collections.singletonList(field10));
    return Arrays.asList(
      new Object[][]{
        {null, null, exception},
        {createNodeFromString("\"generic invalid string\""), mockNames, exception},
        {createNodeFromString("{\"type\": \"unknown type\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"null\"}"), null, exception},
        {createNodeFromString("{\"type\": \"string\"}"), mockNames, exception},
        {createNodeFromString("{\"type\": \"bytes\"}"), validNames, new ExpectedResult<>(Schema.create(Schema.Type.BYTES), null)},
        {createNodeFromString("{\"type\": \"int\"}"), null, exception},
        {createNodeFromString("{\"type\": \"long\"}"), mockNames, exception},
        {createNodeFromString("{\"type\": \"float\"}"), validNames, new ExpectedResult<>(Schema.create(Schema.Type.FLOAT), null)},
        {createNodeFromString("{\"type\": \"double\"}"), null, exception},
        {createNodeFromString("{\"type\": \"boolean\"}"), mockNames, exception},
        {createNodeFromString("{\"type\": \"record\", \"name\": \"RecordTest\", \"namespace\": \"com.example\", \"fields\": [{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"float\"}]}"), validNames, new ExpectedResult<>(recordSchema, null)},
        {createNodeFromString("{\"type\": \"enum\", \"name\": \"EnumTest\", \"symbols\": [\"a\", \"b\"]}"), null, exception},
        {createNodeFromString("{\"type\": \"array\", \"items\": \"int\"}"), mockNames, exception},
        {createNodeFromString("{\"type\": \"map\", \"values\": \"int\"}"), validNames, new ExpectedResult<>(mapSchema, null)},
        {createNodeFromString("[\"int\", \"string\"]"), null, exception},
        {createNodeFromString("{\"type\": \"fixed\", \"size\": 16, \"name\": \"FixedTest\"}"), mockNames, exception},
        // Improvements
        {createNodeFromString("{\"type\": \"enum\", \"name\": \"EnumTest\", \"symbols\": [\"a\", \"b\"]}"), validNames, new ExpectedResult<>(enumSchema, null)},
        {createNodeFromString("{\"type\": \"array\", \"items\": \"int\"}"), validNames, new ExpectedResult<>(arraySchema, null)},
        {createNodeFromString("{\"type\": \"fixed\", \"size\": 16, \"name\": \"FixedTest\"}"), validNames, new ExpectedResult<>(fixedSchema, null)},
        {createNodeFromString("[\"int\", \"string\"]"), validNames, new ExpectedResult<>(unionSchema, null)},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr1\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr2\", \"fields\": \"error\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr3\", \"fields\": [{\"name\":\"a\",\"no-type\":\"error\"}]}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr4\", \"fields\": [{\"name\":\"a\",\"type\": 0}]}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr5\", \"fields\": [{\"name\":\"a\", \"type\": \"invalid\"}]}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTest2\", \"fields\": [{\"name\": \"a\", \"type\": \"boolean\", \"order\": \"ignore\", \"default\": true}]}"), validNames, new ExpectedResult<>(recordSchema2, null)},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr6\", \"fields\": [{\"name\": \"a\", \"type\": \"boolean\", \"default\": 0}]}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTestErr7\", \"fields\": [{\"name\": \"a\", \"type\": \"int\", \"order\": \"invalid\"}]}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTest3\", \"fields\": [{\"name\": \"pi\", \"type\": \"double\", \"default\": \"3.14\"}]}"), validNames, new ExpectedResult<>(recordSchema3, null)},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTest4\", \"fields\": [{\"name\": \"twoPi\", \"type\": \"float\", \"default\": \"6.28\"}]}"), validNames, new ExpectedResult<>(recordSchema4, null)},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTest5\", \"fields\": [{\"name\": \"twoPi\", \"type\": \"float\", \"default\": 6.28}]}"), validNames, new ExpectedResult<>(recordSchema5, null)},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTest6\",\"fields\": [{\"name\": \"uuid\",\"type\": {\"type\": \"string\", \"logicalType\": \"uuid\"},\"additional\": \"value\"}]}"), validNames, new ExpectedResult<>(recordSchema6, null)},
        {createNodeFromString("{\"type\": \"record\",\"name\": \"RecordTest7\",\"fields\": [{\"name\": \"uuidFake\",\"type\": \"string\", \"logicalType\": \"uuid\"}]}"), validNames, new ExpectedResult<>(recordSchema7, null)},
        {createNodeFromString("{\"type\": \"enum\", \"name\": \"EnumTestErr1\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"enum\", \"name\": \"EnumTestErr2\", \"symbols\": \"error\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"enum\", \"name\": \"EnumTest2\", \"symbols\": [\"c\", \"d\"], \"default\": \"d\"}"), validNames, new ExpectedResult<>(enumSchema2, null)},
        {createNodeFromString("{\"type\": \"array\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"map\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"fixed\", \"name\": \"FixedTestErr3\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"fixed\", \"size\": \"test\", \"name\": \"FixedTestErr2\"}"), validNames, exception},
        {createNodeFromString("{\"type\": \"record\", \"name\": \"RecordTestErr8\", \"fields\": [{\"name\": \"Internal\", \"type\": [\"int\", {\"type\": \"RecordTest7\"}, {\"type\": \"unknown\"}]}]}"), validNames, exception},
        {createNodeFromString("{\"type\": \"error\", \"name\": \"ErrorTest\", \"aliases\": [\"alias\"], \"fields\": [{\"name\": \"ErrorField\", \"type\": \"string\"}]}"), validNames, new ExpectedResult<>(errorSchema, null)},
        // ba-dua specific improvements
        {createNodeFromString("{\"type\": \"record\", \"name\": \"RecordTest9\", \"additional\": \"value\" ,\"fields\": [{\"name\": \"Field9\", \"type\": \"string\"}]}"), validNames, new ExpectedResult<>(recordSchema9, null)},
        {createNodeFromString("{\"type\": \"enum\", \"name\": \"EnumTest3\", \"aliases\": [\"Enum3\"], \"additional\": \"value\", \"symbols\": [\"a\", \"b\"]}"), validNames, new ExpectedResult<>(enumSchema3, null)},
        {createNodeFromString("{\"type\": \"array\", \"additional\": \"value\", \"items\": \"int\"}"), validNames, new ExpectedResult<>(arraySchema2, null)},
        {createNodeFromString("{\"type\": \"map\", \"additional\": \"value\", \"values\": \"int\"}"), validNames, new ExpectedResult<>(mapSchema2, null)},
        {createNodeFromString("{\"type\": \"fixed\", \"size\": 16, \"name\": \"FixedTest2\", \"aliases\": [\"Fixed2\"], \"additional\": \"value\"}"), validNames, new ExpectedResult<>(fixedSchema2, null)},
        // PIT improvements
//        {createNodeFromString("{\"type\": \"record\", \"name\": \"RecordTest10\", \"fields\": [{\"name\": \"Field10\", \"type\": [{\"type\": \"EnumTest\"}]}]}"), validNames, new ExpectedResult<>(recordSchema10, null)},
      }
    );
  }

  private final JsonNode node;
  private final Schema.Names names;
  private final ExpectedResult<Schema> expected;

  public TestSchema(JsonNode node, Schema.Names names, ExpectedResult<Schema> expected) {
    this.node = node;
    this.names = names;
    this.expected = expected;
  }

  @Test
  public void parseTest() {
    try {
      Schema result = Schema.parse(node, names);
      Assert.assertEquals(expected.getResult(), result);
      // PIT improvements
      // Checking if it was added to names (only if it's a NamedSchema)
      if (!Schema.PRIMITIVES.containsKey(expected.getResult().getType().getName())
        && expected.getResult().getType() != Schema.Type.MAP
        && expected.getResult().getType() != Schema.Type.ARRAY
        && expected.getResult().getType() != Schema.Type.UNION) {
        // L1723,1724,1774,1775,1791,1792
        Assert.assertTrue(names.contains(result));
        // L1814,1818
        Assert.assertEquals(expected.getResult().getAliases().size(), result.getAliases().size());
        Assert.assertTrue(expected.getResult().getAliases().containsAll(result.getAliases()));
        Assert.assertTrue(result.getAliases().containsAll(expected.getResult().getAliases()));
      }
    } catch (Exception ignored) {
      Assert.assertNotNull(expected.getException());
    }
  }
}
