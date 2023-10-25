package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.debezium.performance.testsuite.ConfigProperties.KAFKA_TEST_TOPIC;
import static org.junit.Assert.assertFalse;

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
        ConsumerRecords<String, String> records = consumer.getRecords(KAFKA_TEST_TOPIC);
        assertFalse(records.isEmpty());
        int i = 1;
        for (ConsumerRecord<String, String> record : records.records(KAFKA_TEST_TOPIC)) {
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
