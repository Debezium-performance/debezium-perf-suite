package io.debezium.performance.testsuite.deserializer;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import io.debezium.performance.testsuite.model.DebeziumTimestamps;

import java.io.IOException;

public class DebeziumTimestampDeserializer extends JsonDeserializer<DebeziumTimestamps> {
    @Override
    public DebeziumTimestamps deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JacksonException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        long transactionTime = node.get("payload").get("source").get("ts_ms").asLong(-1);
        long debeziumReadTime = node.get("payload").get("ts_ms").asLong(-1);
        return new DebeziumTimestamps(transactionTime, debeziumReadTime);
    }
}
