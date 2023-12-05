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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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
    Schema enumSchema = Schema.createEnum("EnumTest", null, null, Arrays.asList("a", "b"));
    Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.INT));
    Schema unionSchema = Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.STRING));
    Schema fixedSchema = Schema.createFixed("FixedTest", null, null, 16);
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
      }
    );
  }

  private final JsonNode node;
  private final Schema.Names names;
  private final ExpectedResult<Schema> expected; // true if compatible; false otherwise

  public TestSchema(JsonNode node, Schema.Names names, ExpectedResult<Schema> expected) {
    this.node = node;
    this.names = names;
    this.expected = expected;
  }

  @Test
  public void parseTest() {
    try {
      Schema result = Schema.parse(node, names);
      // TODO: better check
      Assert.assertEquals(expected.getResult(), result);
    } catch (Exception ignored) {
      Assert.assertNotNull(expected.getException());
    }
  }
}
