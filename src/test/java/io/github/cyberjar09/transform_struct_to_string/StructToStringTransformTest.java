package io.github.cyberjar09.transform_struct_to_string;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class StructToStringTransformTest {

    private StructToStringTransform<SinkRecord> transform = new StructToStringTransform<>();

    @AfterEach
    public void teardown() {
        transform.close();
    }

    /**
     * tests were based on kafka connect data structures which were output to console like so:
     *
     * record >>>
     * SinkRecord{kafkaOffset=0, timestampType=CreateTime}
     * ConnectRecord{
     *   topic='customer-documents.public.file_upload',
     *   kafkaPartition=0,
     *   key=Struct{id=1},
     *   keySchema=Schema{customer_documents.public.file_upload.Key:STRUCT},
     *   value=Struct{
     *     after=Struct{id=1,path=6173d302f79909248339a7f3/3b66defa-540f-46ad-9714-cb03949dee5d.png,uploaded_by=6173d302f79909248339a7f3,created_at=1677041835617167,document_id=17036a79-dc05-467a-ae9a-f1adb61a5242},
     *     source=Struct{version=2.1.2.Final,connector=postgresql,name=customer-documents,ts_ms=1677309630652,snapshot=first_in_data_collection,db=customer-documents,sequence=[null,"7524849974880"],schema=public,table=file_upload,txId=6588179,lsn=7524849974880},
     *     op=r,
     *     ts_ms=1677309633623
     *   },
     *   valueSchema=Schema{customer_documents.public.file_upload.Envelope:STRUCT},
     *   timestamp=1677309638888,
     *   headers=ConnectHeaders(headers=)
     * }
     *
     * record.valueSchema.fields >>>
     * [Field{name=before, index=0, schema=Schema{customer_documents.public.file_upload.Value:STRUCT}}, Field{name=after, index=1, schema=Schema{customer_documents.public.file_upload.Value:STRUCT}}, Field{name=source, index=2, schema=Schema{io.debezium.connector.postgresql.Source:STRUCT}}, Field{name=op, index=3, schema=Schema{STRING}}, Field{name=ts_ms, index=4, schema=Schema{INT64}}, Field{name=transaction, index=5, schema=Schema{event.block:STRUCT}}]
     */

    @Test
    public void testPrimitiveTypes() {
        // Given
        Schema valueSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Struct value = new Struct(valueSchema)
                .put("field1", "value1")
                .put("field2", 123);

        Schema expectedValueSchema = SchemaBuilder.struct().name("JsonValue")
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.INT32_SCHEMA)
                .build();

        Struct expectedValue = new Struct(valueSchema)
                .put("field1", "value1")
                .put("field2", 123);

        SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);

        // When
        SinkRecord transformedRecord = transform.apply(record);

        // Then
        Assert.assertEquals(expectedValueSchema, transformedRecord.valueSchema());
        Assert.assertEquals(expectedValue.toString(), transformedRecord.value().toString());
    }

    @Test
    public void testNestedStruct() {
        // Given
        Schema innerValueSchema = SchemaBuilder.struct()
                .field("innerField1", Schema.STRING_SCHEMA)
                .field("innerField2", Schema.INT32_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", innerValueSchema)
                .build();

        Struct innerValue = new Struct(innerValueSchema)
                .put("innerField1", "value1")
                .put("innerField2", 123);

        Struct value = new Struct(valueSchema)
                .put("field1", "outerValue")
                .put("field2", innerValue);

        SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

        // When
        SinkRecord transformedRecord = transform.apply(record);

        // Expectation
        Schema expectedValueSchema = SchemaBuilder.struct().name("JsonValue")
                .field("field1", Schema.STRING_SCHEMA)
                .field("field2", Schema.STRING_SCHEMA)
                .build();

        String expectedInnerValue = "{\"innerField1\":\"value1\",\"innerField2\":123}";

        Struct expectedValue = new Struct(expectedValueSchema)
                .put("field1", "outerValue")
                .put("field2", expectedInnerValue);

        // Then
        Assert.assertEquals(expectedValueSchema.toString(), transformedRecord.valueSchema().toString());
        Assert.assertEquals(expectedValue, transformedRecord.value());
    }

    /**
     * example input and output should be like below:
     *
     * {
     *   "op":"r"
     *   "ts_ms":{
     *     "long":1677309633509
     *   }
     *   "before":NULL
     *   "after":{
     *     "customer_documents.public.document.Value":{
     *       "user_id":"63f70ea063fc59c6d4b2acfc"
     *       "is_active":true
     *       "expire_at":NULL
     *       "id":118
     *       "updated_at":1677135699026018
     *       "type":"IdentityCard"
     *       "document_id":"a24bee53-7ce5-4f6c-b2f7-a29e55dc0fed"
     *     }
     *   }
     *   "source":{
     *     "schema":"public"
     *     "name":"customer-documents"
     *     "lsn":{
     *       "long":7524849974880
     *     }
     *     "ts_ms":1677309630652
     *     "xmin":NULL
     *   }
     * }
     *
     * {
     *   "op":"r",
     *   "ts_ms":"{\"long\":1677309633509}",
     *   "before":null,
     *   "after":"{\"customer_documents.public.document.Value\":{\"user_id\":\"63f70ea063fc59c6d4b2acfc\",\"is_active\":true,\"expire_at\":null,\"id\":118,\"updated_at\":1677135699026018,\"type\":\"IdentityCard\",\"document_id\":\"a24bee53-7ce5-4f6c-b2f7-a29e55dc0fed\"}}",
     *   "source":"{\"schema\":\"public\",\"name\":\"customer-documents\",\"lsn\":{\"long\":7524849974880},\"ts_ms\":1677309630652,\"xmin\":null}"
     * }
     */

    @Test
    public void testNestedStructWithFieldName() {
        Map<String, ?> configMap = Map.of("debug", false, "fields.exclude", "source, extra");
        transform.configure(configMap);
        // Given
        Schema innerValueSchemaForSource = SchemaBuilder.struct()
                .field("schema", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT32_SCHEMA)
                .build();

        Schema innerValueSchemaForExtra = SchemaBuilder.struct()
                .field("a", Schema.STRING_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .build();

        Schema innerValueSchemaForBefore = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("user_id", Schema.STRING_SCHEMA)
                .build();

        Schema innerValueSchemaForAfter = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("user_id", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("op", Schema.STRING_SCHEMA)
                .field("source", innerValueSchemaForSource)
                .field("extra", innerValueSchemaForExtra)
                .field("before", innerValueSchemaForBefore)
                .field("after", innerValueSchemaForAfter)
                .build();

        Struct innerValueForSource = new Struct(innerValueSchemaForSource)
                .put("schema", "public")
                .put("ts_ms", 123);

        Struct innerValueForExtra = new Struct(innerValueSchemaForExtra)
                .put("a", "z")
                .put("b", 0);

        Struct innerValueForBefore = new Struct(innerValueSchemaForBefore)
                .put("id", "1")
                .put("user_id", "2");

        Struct innerValueForAfter = new Struct(innerValueSchemaForAfter)
                .put("id", "1")
                .put("user_id", "2");

        Struct value = new Struct(valueSchema)
                .put("op", "r")
                .put("source", innerValueForSource)
                .put("extra", innerValueForExtra)
                .put("before", innerValueForBefore)
                .put("after", innerValueForAfter);

        SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

        // When
        SinkRecord transformedRecord = transform.apply(record);

        // Expectation
        Schema expectedValueSchema = SchemaBuilder.struct().name("JsonValue")
                .field("op", Schema.STRING_SCHEMA)
                .field("source", innerValueSchemaForSource)
                .field("extra", innerValueSchemaForExtra)
                .field("before", Schema.STRING_SCHEMA)
                .field("after",  Schema.STRING_SCHEMA)
                .build();

        String expectedInnerValueForBefore = "{\"id\":\"1\",\"user_id\":\"2\"}";
        String expectedInnerValueForAfter = "{\"id\":\"1\",\"user_id\":\"2\"}";

        // Then
        Assert.assertEquals(fieldToString(expectedValueSchema.field("before")), fieldToString(transformedRecord.valueSchema().field("before")));
        Assert.assertEquals(fieldToString(expectedValueSchema.field("after")), fieldToString(transformedRecord.valueSchema().field("after")));
        Assert.assertEquals(fieldToString(expectedValueSchema.field("source")), fieldToString(transformedRecord.valueSchema().field("source")));
        Assert.assertEquals(fieldToString(expectedValueSchema.field("extra")), fieldToString(transformedRecord.valueSchema().field("extra")));

        Assert.assertEquals(expectedInnerValueForBefore.toString(), ((Struct) transformedRecord.value()).get("before").toString());
        Assert.assertEquals(expectedInnerValueForAfter.toString(), ((Struct) transformedRecord.value()).get("after").toString());
        Assert.assertEquals(innerValueForSource.toString(), ((Struct) transformedRecord.value()).get("source").toString());
        Assert.assertEquals(innerValueForExtra.toString(), ((Struct) transformedRecord.value()).get("extra").toString());
    }

    private static String fieldToString(Field field) {
        if(field == null) return null;
        String[] split = field.toString().split(", ");
        String joinedString = String.join(", ", Arrays.asList(split[0], split[2]));
        return joinedString;
    }

    @Test
    public void testSchemaPropogationWhenValueDoesNotExist() {
        Map<String, ?> configMap = Map.of("debug", true, "fields.exclude", "");
        transform.configure(configMap);

        // Given
        Schema innerValueSchemaExistsButNoValue = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("user_id", Schema.STRING_SCHEMA)
                .build();

        Schema innerValueSchemaAndValueExists = SchemaBuilder.struct()
                .field("id", Schema.STRING_SCHEMA)
                .field("user_id", Schema.STRING_SCHEMA)
                .build();

        Schema valueSchema = SchemaBuilder.struct()
                .field("op", Schema.STRING_SCHEMA)
                .field("schemaExistsButNoValue", innerValueSchemaExistsButNoValue)
                .field("bothSchemaAndValueExists", innerValueSchemaAndValueExists)
                .build();

        Struct innerValueForAfter = new Struct(innerValueSchemaAndValueExists)
                .put("id", "1")
                .put("user_id", "2");

        Struct value = new Struct(valueSchema)
                .put("op", "r")
                .put("bothSchemaAndValueExists", innerValueForAfter);

        SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

        // When
        SinkRecord transformedRecord = transform.apply(record);

        // Expectation
        Schema expectedValueSchema = SchemaBuilder.struct().name("JsonValue")
                .field("op", Schema.STRING_SCHEMA)
                .field("bothSchemaAndValueExists",  Schema.STRING_SCHEMA)
                .build();
//        System.out.println("==== expectedValueSchema: " + expectedValueSchema.fields());

        // Then
//        Assert.assertEquals(expectedValueSchema.fields().toString(), transformedRecord.valueSchema().fields().toString());
        Assert.assertEquals(fieldToString(expectedValueSchema.field("op")), fieldToString(transformedRecord.valueSchema().field("op")));
        Assert.assertEquals(fieldToString(expectedValueSchema.field("bothSchemaAndValueExists")), fieldToString(transformedRecord.valueSchema().field("bothSchemaAndValueExists")));
        Assert.assertEquals(null, fieldToString(transformedRecord.valueSchema().field("schemaExistsButNoValue")));
    }

}