package io.github.cyberjar09.transform_struct_to_string;

import org.apache.kafka.common.config.ConfigDef;
//import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

public class JsonStringConverter<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String DEPTH_CONFIG = "depth";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(DEPTH_CONFIG, ConfigDef.Type.INT, 1, ConfigDef.Range.between(1, 10),
                    ConfigDef.Importance.HIGH, "Depth of the JSON string conversion");

    private int depth;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        depth = config.getInt(DEPTH_CONFIG);
    }

    @Override
    public R apply(R record) {
        final Object value = record.value();
        if (value instanceof Struct) {
            final Struct structValue = (Struct) value;
            final Schema schema = record.valueSchema();

            final Map<String, Object> result = new HashMap<>();
            for (Field field : schema.fields()) {
                final String fieldName = field.name();
                final Schema fieldSchema = field.schema();
                final Object fieldValue = structValue.get(fieldName);

                if (fieldValue == null) {
                    result.put(fieldName, null);
                } else if (fieldSchema.type().isPrimitive()) {
                    result.put(fieldName, fieldValue);
//                } else if (depth > 1) {
//                    result.put(fieldName, Utils.toJsonString(fieldValue));
                } else {
                    result.put(fieldName, fieldValue.toString());
                }
            }

            Schema resultSchema = SchemaBuilder.struct().name("JsonValue"); // .fields().addAll(schema.fields())
            Struct resultValue = new Struct(resultSchema);

            result.forEach((k, v) -> {
                resultValue.put(k, v);
            });

            return record.newRecord(record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
//                    SchemaBuilder.struct().name("JsonValue").fields().addAll(schema.fields()),
//                    new Struct(SchemaBuilder.struct().name("JsonValue").fields().addAll(schema.fields())).putAll(result),
                    resultSchema,
                    resultValue,
                    record.timestamp());
        } else {
            return record;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}

