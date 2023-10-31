package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

import static io.debezium.performance.testsuite.ConfigProperties.KAFKA_TEST_TOPIC;

public class BasicSqlPrintTest {

    private final Logger LOG = LoggerFactory.getLogger(BasicSqlPrintTest.class);

    @Test
    public void BasicMysqlTest() {
        int count = 32;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSqlBatchLoad(count, 10000).toString());
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        int i = 1;
        for (ConsumerRecord<String, String> record : records) {
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
