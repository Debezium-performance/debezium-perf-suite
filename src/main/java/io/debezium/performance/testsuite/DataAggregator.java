package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.model.TimeResults;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataAggregator {
    Map<Long, Integer> transactionsPerSecond;
    Map<Long, Integer> readsPerSecond;
    Map<Long, Integer> sendsPerSecond;

    Map<TimeResults, Integer> allResultsWithCount;

    int messageCount;
    int messageSize;
    int testNumber;

    public DataAggregator(int messageCount, int messageSize, int testNumber) {
        transactionsPerSecond = new HashMap<>();
        readsPerSecond = new HashMap<>();
        sendsPerSecond = new HashMap<>();
        allResultsWithCount = new HashMap<>();
        this.messageCount = messageCount;
        this.messageSize = messageSize;
        this.testNumber = testNumber;
    }

    public void addResult(TimeResults result) {
        Long transactionSecond = roundToSeconds(result.getDatabaseTransactionTime());
        Long readSecond = roundToSeconds(result.getDebeziumStartTime());
        Long sendSecond = roundToSeconds(result.getKafkaReceiveTime());
        transactionsPerSecond.merge(transactionSecond, 1, (f1, f2) -> f1 + 1);
        readsPerSecond.merge(readSecond, 1, (f1, f2) -> f1 + 1);
        sendsPerSecond.merge(sendSecond, 1, (f1, f2) -> f1 + 1);
        allResultsWithCount.merge(result, 1, (f1, f2) -> f1 + 1);
    }

    public List<List<String>> getTransactionsPerSecondAsList() {
        return getAsListOfStringArrays(Arrays.asList("Seconds", "Number of read messages"), transactionsPerSecond);
    }
    public List<List<String>> getReadsPerSecondAsList() {
        return getAsListOfStringArrays(Arrays.asList("Seconds", "Number of read messages"), readsPerSecond);
    }
    public List<List<String>> getSendsPerSecondAsList() {
        return getAsListOfStringArrays(Arrays.asList("Seconds", "Number of read messages"), sendsPerSecond);
    }

    public List<List<String>> getAllResultsWithHeadersAsList() {
        List<List<String>> list = new ArrayList<>();
        list.add(getCountAndSizeHeader());
        list.add(Arrays.asList("Transaction timestamp", "Debezium read timestamp", "Kafka receive timestamp", "Debezium read speed", "Debezium process speed", "Message count"));
        for (var resultWithCount : allResultsWithCount.entrySet()) {
            List<String> row = resultWithCount.getKey().getAllValuesWithSqlTimestampAsList();
            row.add(resultWithCount.getValue().toString());
            list.add(row);
        }
        return list;
    }

    public Map<Long, Integer> getTransactionsPerSecond() {
        return transactionsPerSecond;
    }

    public Map<Long, Integer> getReadsPerSecond() {
        return readsPerSecond;
    }

    public Map<Long, Integer> getSendsPerSecond() {
        return sendsPerSecond;
    }

    public int getMessageCount() {
        return messageCount;
    }

    public int getMessageSize() {
        return messageSize;
    }

    public int getTestNumber() {
        return testNumber;
    }

    public void setTestNumber(int testNumber) {
        this.testNumber = testNumber;
    }

    public Map<TimeResults, Integer> getAllResultsWithCount() {
        return allResultsWithCount;
    }

    private Long roundToSeconds(Long time) {
        return Instant.ofEpochMilli(time).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
    }

    private List<List<String>> getAsListOfStringArrays(List<String> headers, Map<Long, Integer> map) {
        List<List<String>> list = new ArrayList<>();
        list.add(getCountAndSizeHeader());
        list.add(headers);
        map.forEach((second, count) -> list.add(Arrays.asList(new Timestamp(second).toString(), count.toString())));
        return list;
    }

    private List<String> getCountAndSizeHeader(){
        return Arrays.asList("Message count:", String.valueOf(messageCount), "Message size (bytes):", String.valueOf(messageSize));
    }
}
