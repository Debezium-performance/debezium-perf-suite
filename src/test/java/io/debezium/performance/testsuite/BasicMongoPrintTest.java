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

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
        // count +1 because of collection create event <- NOT TRUE CURRENTLY
        List<ConsumerRecord<String, String>> records = kafkaController.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
        printResultsCsv(records, 1);
    }

    @Test
    public void test2() {
        int count = 1000;
        int size = 10000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResults(records);
        printResultsCsv(records, 2);
    }

    @Test
    public void test3() {
        int count = 10000;
        int size = 1000;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResultsCsv(records, 3);
    }

    @Test
    public void test4() {
        int count = 100000;
        int size = 100;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResultsCsv(records,4);
    }

    @Test
    public void test5() {
        int count = 1000000;
        int size = 10;
        BareDmtController dmt = BareDmtController.getInstance();
        KafkaConsumerController consumer = KafkaConsumerController.getInstance();
        LOG.info(dmt.generateMongoBulkLoad(count, 1000000, size).toString());
        List<ConsumerRecord<String, String>> records = consumer.getRecords(KAFKA_TEST_TOPIC, count);
        printResultsCsv(records, 5);
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

    private void printResultsCsv(List<ConsumerRecord<String, String>> records, int testNumber){
        int i = 1;
        List<String[]> dataLine = new ArrayList<>();
        dataLine.add(new String[]{"Debezium read timestamp", "Debezium read speed"});
        List<String[]> dataLine2 = new ArrayList<>();
        dataLine2.add(new String[]{"Kafka receive timestamp", "Debezium process speed"});
        List<String[]> dataLine3 = new ArrayList<>();
        dataLine3.add(new String[]{"Second", "Number of read messages"});
        long lastSecond = -1L;
        int count = 0;
        long currentSecond;
        for (ConsumerRecord<String, String> record : records) {
            TimeResults results;
            try {
                results = KafkaRecordParser.parseTimeResults(record);
                dataLine.add(new String[] {String.valueOf(results.getDebeziumStartTime()), String.valueOf(results.getDebeziumReadSpeed())});
                dataLine2.add(new String[]{String.valueOf(results.getKafkaReceiveTime()), String.valueOf(results.getDebeziumProcessSpeed())});
                currentSecond = Instant.ofEpochMilli(results.getDebeziumStartTime()).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
                if (lastSecond == currentSecond) {
                    count++;
                } else {
                    dataLine3.add(new String[]{String.valueOf(lastSecond), String.valueOf(count)});
                    lastSecond = currentSecond;
                    count = 1;
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
//            LOG.info(i++ + " " + results);
        }
        try(PrintWriter pw = new PrintWriter(String.format("test_read%s.csv", testNumber))) {
            dataLine.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try(PrintWriter pw = new PrintWriter(String.format("test_process%s.csv", testNumber))) {
            dataLine2.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try(PrintWriter pw = new PrintWriter(String.format("test_read_per_second%s.csv", testNumber))) {
            dataLine3.stream()
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
