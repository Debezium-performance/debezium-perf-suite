package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.BeforeClass;
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
        int count = 100;
        int size = 100000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController kafkaController = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = kafkaController.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        printResultsCsv(records, 1, count, size);
    }

    @Test
    public void test2() {
        int count = 1000;
        int size = 10000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
//        printResults(records);
        printResultsCsv(records, 2, count, size);
    }

    @Test
    public void test3() {
        int count = 10000;
        int size = 1000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResultsCsv(records, 3, count, size);
    }

    @Test
    public void test4() {
        int count = 100000;
        int size = 100;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResultsCsv(records,4, count, size);
    }

    @Test
    public void test5() {
        int count = 1000000;
        int size = 10;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResultsCsv(records, 5, count, size);
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

    private void printResultsCsv(List<ConsumerRecord<String, String>> records, int testNumber, int messageCount, int messageSize){
        DataAggregator aggregator = new DataAggregator(messageCount, messageSize);
        for (ConsumerRecord<String, String> record : records) {
            TimeResults results;
            try {
                results = KafkaRecordParser.parseTimeResults(record);
                aggregator.addResult(results);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        CsvExporter.exportToCSVFiles(aggregator, testNumber);
    }

}
