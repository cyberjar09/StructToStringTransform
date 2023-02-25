package io.github.cyberjar09.transform_struct_to_string;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StructToStringTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
        // No resources to release
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // No configuration required
    }

    @Override
    public R apply(R record) {
        Struct value = (Struct) record.value();

        value.schema().fields().forEach(f -> {
            List nonPrimitiveTypes = Arrays.asList(Schema.Type.STRUCT, Schema.Type.ARRAY, Schema.Type.MAP);
            if (nonPrimitiveTypes.contains(f.schema().type())){
                Struct fieldValue = value.getStruct(f.name());
                Struct transformedFieldValue = getJsonStruct(fieldValue);
                value.put(f, transformedFieldValue);
            }
        });
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), value, record.timestamp());
    }

    private Struct getJsonStruct(Struct struct) {
        Struct jsonStruct = new Struct(struct.schema());
        for (Field field : struct.schema().fields()) {
            Object fieldValue = struct.get(field);
            if (fieldValue instanceof Struct) {
                fieldValue = toJsonString((Struct) fieldValue);
            }
            jsonStruct.put(field.name(), fieldValue);
        }
        return jsonStruct;
    }

    private String toJsonString(Struct struct) {
        return struct.schema().fields().stream()
                .map(field -> "\"" + field.name() + "\":" + struct.get(field))
                .collect(Collectors.joining(",", "{", "}"));
    }
}

