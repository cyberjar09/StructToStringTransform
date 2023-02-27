package io.github.cyberjar09.transform_struct_to_string;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;

public class JsonUtils {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        // Register a JsonSerializer for the Struct class
        SimpleModule module = new SimpleModule();
        module.addSerializer(Struct.class, new JsonSerializer<Struct>() {
            @Override
            public void serialize(Struct value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                ObjectNode node = objectMapper.createObjectNode();
                value.schema().fields().forEach(field -> {
                    String fieldName = field.name();
                    Object fieldValue = value.get(fieldName);
                    if (fieldValue != null) {
                        node.set(fieldName, objectMapper.valueToTree(fieldValue));
                    }
                });
                gen.writeTree(node);
            }
        });
        objectMapper.registerModule(module);
    }

    public static String toJsonString(Object value) throws JsonProcessingException {
        return objectMapper.writeValueAsString(value);
    }
}
