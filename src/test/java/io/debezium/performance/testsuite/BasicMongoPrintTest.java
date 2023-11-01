package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.deserializer.KafkaRecordParser;
import io.debezium.performance.testsuite.dmt.BareDmtController;
import io.debezium.performance.testsuite.model.TimeResults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
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
    //[main] INFO io.debezium.performance.testsuite.BasicMongoPrintTest - 999 Database transaction time=2-10-31 07:57:08.000, Debezium receive time=2-10-31 07:58:00.759, Kafka receive time=2-10-31 07:58:01.257, Debezium read speed=52759, Debezium process speed=498
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

    private void printResultsCsv(List<ConsumerRecord<String, String>> records){
        int i = 1;
        List<String[]> dataline = new ArrayList<>();
        List<String[]> dataline2 = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            TimeResults results;
            try {
                results = KafkaRecordParser.parseTimeResults(record);
                dataline.add(new String[] {String.valueOf(results.getDebeziumStartTime()), String.valueOf(results.getDebeziumReadSpeed())});
                dataline2.add(new String[]{String.valueOf(results.getKafkaReceiveTime()), String.valueOf(results.getDebeziumProcessSpeed())});
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
//            LOG.info(i++ + " " + results);
        }
        try(PrintWriter pw = new PrintWriter("test_read.csv")) {
            dataline.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try(PrintWriter pw = new PrintWriter("test_process.csv")) {
            dataline2.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    public String convertToCSV(String[] data) {
        return String.join(",", data);
    }
}
