package io.github.cyberjar09.transform_struct_to_string;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class JsonStringConverterTest {

    @Test
    public void testPrimitiveFields() {
        final Schema schema = SchemaBuilder.struct().name("TestSchema")
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("salary", Schema.FLOAT64_SCHEMA)
                .build();
        final Struct value = new Struct(schema)
                .put("name", "Alice")
                .put("age", 30)
                .put("salary", 5000.0);

        final Transformation<SourceRecord> transformation = new JsonStringConverter<>();
        transformation.configure(Collections.emptyMap());

        final SourceRecord inputRecord = new SourceRecord(null, null, "test", null, schema, value);
        final SourceRecord outputRecord = transformation.apply(inputRecord);

        Assertions.assertEquals(schema, outputRecord.valueSchema());
        Assertions.assertEquals(value, outputRecord.value());
    }

    @Test
    public void testNonPrimitiveFieldsDepth1() {
        final Schema innerSchema = SchemaBuilder.struct().name("InnerSchema")
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Schema schema = SchemaBuilder.struct().name("TestSchema")
                .field("data", innerSchema)
                .build();
        final Struct value = new Struct(schema)
                .put("data", new Struct(innerSchema).put("id", 1L).put("name", "Alice"));

        final Transformation<SourceRecord> transformation = new JsonStringConverter<>();
        transformation.configure(Collections.singletonMap("depth", 1));

        final SourceRecord inputRecord = new SourceRecord(null, null, "test", null, schema, value);
        final SourceRecord outputRecord = transformation.apply(inputRecord);

        Assertions.assertEquals(schema, outputRecord.valueSchema());
        Assertions.assertEquals("{\"id\":1,\"name\":\"Alice\"}", outputRecord.value().toString());
    }

    @Test
    public void testNonPrimitiveFieldsDepth2() {
        final Schema innerSchema = SchemaBuilder.struct().name("InnerSchema")
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        final Schema schema = SchemaBuilder.struct().name("TestSchema")
                .field("data", innerSchema)
                .build();
        final Struct value = new Struct(schema)
                .put("data", new Struct(innerSchema).put("id", 1L).put("name", "Alice"));

        final Transformation<SourceRecord> transformation = new JsonStringConverter<>();
        transformation.configure(Collections.singletonMap("depth", 2));

        final SourceRecord inputRecord = new SourceRecord(null, null, "test", null, schema, value);
        final SourceRecord outputRecord = transformation.apply(inputRecord);

        Assertions.assertEquals(schema, outputRecord.valueSchema());
        Assertions.assertEquals("{\"id\":1,\"name\":\"Alice\"}", outputRecord.value().toString());
    }

//    @Test
//    public void testNullFields() {
//        final Schema schema = SchemaBuilder.struct().name("TestSchema")
//                .field("name", Schema.STRING_SCHEMA)
//                .field("age",

}