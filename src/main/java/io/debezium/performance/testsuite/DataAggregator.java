package io.debezium.performance.testsuite;

import io.debezium.performance.testsuite.model.TimeResults;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataAggregator {
    Map<Long, Integer> transactionsPerSecond;
    Map<Long, Integer> readsPerSecond;
    Map<Long, Integer> sendsPerSecond;

    List<TimeResults> allResults;

    int messageCount;
    int messageSize;

    public DataAggregator(int messageCount, int messageSize) {
        transactionsPerSecond = new HashMap<>();
        readsPerSecond = new HashMap<>();
        sendsPerSecond = new HashMap<>();
        allResults = new ArrayList<>();
        this.messageCount = messageCount;
        this.messageSize = messageSize;
    }

    public void addResult(TimeResults result) {
        Long transactionSecond = roundToSeconds(result.getDatabaseTransactionTime());
        Long readSecond = roundToSeconds(result.getDebeziumStartTime());
        Long sendSecond = roundToSeconds(result.getKafkaReceiveTime());
        transactionsPerSecond.merge(transactionSecond, 1, (f1, f2) -> f1 + 1);
        readsPerSecond.merge(readSecond, 1, (f1, f2) -> f1 + 1);
        sendsPerSecond.merge(sendSecond, 1, (f1, f2) -> f1 + 1);
        allResults.add(result);
    }

    public List<String[]> getTransactionsPerSecond() {
        return getAsListOfStringArrays(new String[]{"Seconds", "Number of db transactions"}, transactionsPerSecond);
    }

    public List<String[]> getReadsPerSecond() {
        return getAsListOfStringArrays(new String[]{"Seconds", "Number of read messages"}, readsPerSecond);
    }
    public List<String[]> getSendsPerSecond() {
        return getAsListOfStringArrays(new String[]{"Seconds", "Number of sent messages"}, sendsPerSecond);
    }

    public List<String[]> getAllResults() {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"Transaction timestamp", "Debezium read timestamp", "Kafka receive timestamp", "Debezium read speed", "Debezium process speed "});
        allResults.forEach(result -> list.add(result.getAllValues()));
        return list;
    }

    public String[] getCountAndSize() {
        return new String[]{String.valueOf(messageCount), String.valueOf(messageSize)};
    }

    private Long roundToSeconds(Long time) {
        return Instant.ofEpochMilli(time).truncatedTo(ChronoUnit.SECONDS).toEpochMilli();
    }

    private List<String[]> getAsListOfStringArrays(String[] headers, Map<Long, Integer> map) {
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"Message count:", String.valueOf(messageCount), "Message size (bytes):", String.valueOf(messageSize)});
        list.add(headers);
        map.forEach((second, count) -> list.add(new String[]{second.toString(), count.toString()}));
        return list;
    }
}
