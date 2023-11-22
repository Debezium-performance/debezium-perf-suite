package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.exporter.CsvExporter;
import io.debezium.performance.testsuite.exporter.PostgresExporter;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static io.debezium.performance.testsuite.ConfigProperties.KAFKA_TEST_TOPIC;

public class BasicMongoPrintTest {

    private final Logger LOG = LoggerFactory.getLogger(BasicMongoPrintTest.class);

    @BeforeClass
    public static void clearTopic() {

        KafkaConsumerController.getInstance().deleteAndRecreateTopic(KAFKA_TEST_TOPIC);
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test1() {
        int count = 10000;
        int size = 25000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController kafkaController = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = kafkaController.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 1, count, size);
    }

    @Test
    public void test2() {
        int count = 100000;
        int size = 2500;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        exportResults(records, 2, count, size);
    }

    @Test
    public void test3() {
        int count = 1000000;
        int size = 250;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        exportResults(records, 3, count, size);
    }

    @Test
    public void test4() {
        int count = 2000000;
        int size = 125;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        exportResults(records,4, count, size);
    }

    @Test
    public void test5() {
        int count = 10000000;
        int size = 25;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        exportResults(records, 5, count, size);
    }

    @Test
    @Ignore
    public void csvSmallTest() {
        int count = 100000;
        int size = 25;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        exportResultsCsv(records, 6, count, size);
    }

    @Test
    @Ignore
    public void postgresSmallTest() {
        int count = 100000;
        int size = 25;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        exportResults(records, 7, count, size);
    }

    @Test
    @Ignore
    public void test5WithoutConsume() {
        int count = 10000000;
        int size = 25;
        BareDmtController dmt = BareDmtController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
    }

    @Test
    @Ignore
    public void test5OnlyConsume() {
        int count = 10000000;
        int size = 25;
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        exportResults(records, 5, count, size);
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

    private void exportResultsCsv(List<ConsumerRecord<String, String>> records, int testNumber, int messageCount, int messageSize){
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
        new CsvExporter(aggregator).export();
    }

}
