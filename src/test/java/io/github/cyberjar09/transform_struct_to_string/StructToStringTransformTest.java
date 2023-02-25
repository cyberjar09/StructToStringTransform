package io.github.cyberjar09.transform_struct_to_string;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StructToStringTransformTest {

    private StructToStringTransform<SourceRecord> transform = new StructToStringTransform<>();;

    @Test
    public void testPrimitiveTypes() {
        // Given
        Map<String, Object> props = new HashMap<>();
        transform.configure(props);

        Schema schema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Struct struct = new Struct(schema)
                .put("field1", "value1")
                .put("field2", 123);

        SourceRecord record = new SourceRecord(null, null, "topic", 0, schema, struct);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        Assert.assertEquals("value1", ((Struct) transformedRecord.value()).get("field1"));
        Assert.assertEquals(123, ((Struct) transformedRecord.value()).get("field2"));
    }

    @Test
    public void testNestedStruct() {
        // Given
        Map<String, Object> props = new HashMap<>();
        transform.configure(props);

        Schema innerSchema = SchemaBuilder.struct()
                .field("innerField1", Schema.STRING_SCHEMA)
                .field("innerField2", Schema.INT32_SCHEMA)
                .build();

        Schema schema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", innerSchema)
                .build();

        Struct innerStruct = new Struct(innerSchema)
                .put("innerField1", "value1")
                .put("innerField2", 123);

        Struct struct = new Struct(schema)
                .put("field1", "outerValue")
                .put("field2", innerStruct);

        SourceRecord record = new SourceRecord(null, null, "topic", 0, schema, struct);

        // When
        SourceRecord transformedRecord = transform.apply(record);

        // Then
        Struct transformedStruct = (Struct) transformedRecord.value();
        Assert.assertEquals("outerValue", transformedStruct.get("field1"));

//        Struct transformedInnerStruct = (Struct) transformedStruct.get("field2");
//        Assert.assertEquals("{\"innerField1\":\"value1\",\"innerField2\":123}", transformedInnerStruct.toString());
    }


}