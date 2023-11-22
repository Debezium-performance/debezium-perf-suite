package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.model.TimeResults;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

public class TestDataAggregator {
    Map<Long, Integer> transactionsPerSecond;
    Map<Long, Integer> readsPerSecond;
    Map<Long, Integer> sendsPerSecond;

    Map<TimeResults, Integer> allResultsWithCount;

    int messageCount;
    int messageSize;
    int testNumber;

    public TestDataAggregator(int messageCount, int messageSize, int testNumber) {
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

    public Map<TimeResults, Integer> getAllResultsWithCount() {
        return allResultsWithCount;
    }

    private Long roundToSeconds(Long time) {
        return Instant.ofEpochMilli(time).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
    }
}
