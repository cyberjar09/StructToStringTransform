package io.github.cyberjar09.transform_struct_to_string;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

public class StructToStringTransform<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String EXCLUDE_FIELDS_CONFIG = "fields.exclude";
    private static final String DEBUG = "debug";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(EXCLUDE_FIELDS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH,
                    "List of top level field names to exclude from conversion to JSON strings")
            .define(DEBUG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW, "Show debug output of records");

    private List<String> fieldNamesToExcludeFromTransform;
    private boolean isDebugModeEnabled;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        fieldNamesToExcludeFromTransform = config.getList(EXCLUDE_FIELDS_CONFIG);
        isDebugModeEnabled = config.getBoolean(DEBUG);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to release
    }

    @Override
    public R apply(R record) {
        printRecordForDebugging(record, "inputRecord");

        Object objectValue = record.value();
        if (objectValue instanceof Struct) {
            final Struct value = (Struct) objectValue;
            final Schema valueSchema = record.valueSchema();

            final Map<String, Object> updatedValue = new HashMap<>();
            final Map<String, Schema> updatedSchema = new HashMap<>();

            valueSchema.fields().forEach(field -> {
                final String fieldName = field.name();
                final Schema fieldSchema = field.schema();
                final Object fieldValue = value.get(fieldName);

                updatedSchema.put(fieldName, fieldSchema);
                if (fieldValue == null) {
                    updatedValue.put(fieldName, null);
                } else if (fieldSchema.type().isPrimitive()) {
                    updatedValue.put(fieldName, fieldValue);
                } else if (fieldNamesToExcludeFromTransform != null && fieldNamesToExcludeFromTransform.contains(fieldName)){
                    updatedValue.put(fieldName, fieldValue);
                } else {
                    String jsonString = null;
                    try {
                        updatedSchema.put(fieldName, Schema.STRING_SCHEMA);
                        jsonString = JsonUtils.toJsonString(fieldValue);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    updatedValue.put(fieldName, jsonString);
                }
            });

            SchemaBuilder resultSchemaBuilder = SchemaBuilder.struct().name("JsonValue");
            updatedSchema.forEach((fieldName, fieldSchema) -> {
                resultSchemaBuilder.field(fieldName, fieldSchema);
            });
            Schema resultSchema = resultSchemaBuilder.build();

            Struct resultValue = new Struct(resultSchema);

            updatedValue.forEach((k, v) -> {
                resultValue.put(k, v);
            });

            R newRecord = record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    resultSchema,
                    resultValue,
                    record.timestamp());

            printRecordForDebugging(newRecord, "newRecord");

            return newRecord;
        } else {
            return record;
        }
    }

    private void printRecordForDebugging(R record, String recordName) {
        if (isDebugModeEnabled) {
            System.out.println("-------");
            System.out.println(recordName + " >>> " + record);
            System.out.println(recordName + ".valueSchema.fields >>> " + record.valueSchema().fields());
            System.out.println(recordName + ".valueSchema.defaultValue >>> " + record.valueSchema().defaultValue());
            System.out.println(recordName + ".valueSchema.name >>> " + record.valueSchema().name());
            System.out.println(recordName + ".valueSchema.version >>> " + record.valueSchema().version());
            System.out.println(recordName + ".valueSchema.doc >>> " + record.valueSchema().doc());
            System.out.println(recordName + ".valueSchema.parameters >>> " + record.valueSchema().parameters());
            System.out.println("-------");
        }
    }
}

