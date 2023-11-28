package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.exporter.PostgresExporter;
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
    public void test1() {
        int count = 10000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController kafkaController = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSqlBatchLoad(count, 1000000).toString());
        List<ConsumerRecord<String, String>> records = kafkaController.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 1, count, 0);
    }

    @Test
    public void test2() {
        int count = 100000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSqlBatchLoad(count, 1000000).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 2, count, 0);
    }

    @Test
    public void test3() {
        int count = 1000000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSqlBatchLoad(count, 1000000).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 3, count, 0);
    }

    @Test
    public void test4() {
        int count = 2000000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSqlBatchLoad(count, 1000000).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 4, count, 0);
    }

    @Test
    public void test5() {
        int count = 10000000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSqlBatchLoad(count, 1000000).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 5, count,0);
    }

    @Test
    public void sizedTest6() {
        int count = 10000000;
        int size = 25;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateSizedSqlBatchLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 6, count,0);
    }

    private void exportResults(List<ConsumerRecord<String, String>> records, int testNumber, int messageCount, int messageSize){
        TestDataAggregator aggregator = new TestDataAggregator(messageCount, messageSize, testNumber);
        for (ConsumerRecord<String, String> record : records) {
            TimeResults results;
            try {
                results = KafkaRecordParser.parseTimeResults(record);
                aggregator.addResult(results);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        new PostgresExporter(aggregator).export();
    }
}
