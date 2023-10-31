package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static io.debezium.performance.testsuite.ConfigProperties.KAFKA_TEST_TOPIC;

public class BasicMongoPrintTest {

    private final Logger LOG = LoggerFactory.getLogger(BasicMongoPrintTest.class);

    @Test
    public void BasicMongoTest() {
        BareDmtController dmt = new BareDmtController();
        KafkaConsumerController consumer = new KafkaConsumerController();
        LOG.info(dmt.generateMongoBulkLoad(30, 10000, 2).toString());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        //TODO: Change and refactor this piece of code so it works in between tests with multiple records in topics
        Consumer<String,String> consumer1 = consumer.getConsumer();
        consumer1.subscribe(Collections.singletonList(KAFKA_TEST_TOPIC));
        List<ConsumerRecord<String, String>> collection = new ArrayList<>();
        while (collection.size() < 30) {
        ConsumerRecords<String, String> records = consumer1.poll(Duration.of(100, ChronoUnit.SECONDS));
            records.forEach(collection::add);
        }
        LOG.info(String.valueOf(collection.size()));
        int i = 1;
        for (ConsumerRecord<String, String> record : collection) {
            TimeResults results;
            try {
                results = KafkaRecordParser.parseTimeResults(record);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            LOG.info(i++ + " " + results);
        }
    }
}
