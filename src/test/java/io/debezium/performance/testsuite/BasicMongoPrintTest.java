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
    public void test1() {
        int count = 100000;
        int size = 10000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 10000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
    }

    @Test
    public void test2() {
        int count = 10000;
        int size = 100000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 10000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
    }

    @Test
    public void test3() {
        int count = 1000;
        int size = 1000000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 10000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
    }

    @Test
    public void test4() {
        int count = 100;
        int size = 10000000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 10000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
    }

    @Test
    public void test5() {
        int count = 100;
        int size = 10000000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 10000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
    }

    private void printResults(List<ConsumerRecord<String, String>> records){
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
