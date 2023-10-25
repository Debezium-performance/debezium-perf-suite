package io.debezium.performance.testsuite.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.performance.testsuite.model.DebeziumTimestamps;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaRecordParser {

    static ObjectMapper mapper = new ObjectMapper();

    public static TimeResults parseTimeResults(ConsumerRecord<String, String> record) throws JsonProcessingException {
        long kafkaTimestamp = record.timestamp();
        DebeziumTimestamps debeziumTimestamps = mapper.readValue(record.value(), DebeziumTimestamps.class);
        return new TimeResults(debeziumTimestamps, kafkaTimestamp);
    }
}
