package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.TestDataAggregator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CsvExporter implements Exporter {

    TestDataAggregator testDataAggregator;
    String currentDateTime;

    public CsvExporter(TestDataAggregator dataAggregator) {
        this.testDataAggregator = dataAggregator;
        Date time = new Date(Instant.now().toEpochMilli());
        SimpleDateFormat formatter = new SimpleDateFormat("yy_M_d_hh_mm");
        this.currentDateTime = formatter.format(time);
    }

    public void export(TestDataAggregator aggregator, int testNumber) {
        new File(currentDateTime).mkdirs();
        writeCsvToFile(getTransactionsPerSecondCsv(), generateFileName("transactions-per-second"));
        writeCsvToFile(getReadsPerSecondCsv(), generateFileName("reads-per-second"));
        writeCsvToFile(getSendsPerSecondCsv(), generateFileName("sends-per-second"));
        writeCsvToFile(getAllResultsCsv(), generateFileName("total-results-counts"));
    }

    private void writeCsvToFile(List<String> lines, String fileName) {
        try(PrintWriter pw = new PrintWriter(fileName)) {
            lines.forEach(pw::println);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertListToCSVRow(List<String> data) {
        return String.join(",", data);
    }

    private List<String> getTransactionsPerSecondCsv() {
        return getCsvWithHeader(Arrays.asList("Second", "Number of transactions"), testDataAggregator.getTransactionsPerSecond());
    }

    private List<String> getReadsPerSecondCsv() {
        return getCsvWithHeader(Arrays.asList("Second", "Number of read messages"), testDataAggregator.getReadsPerSecond());
    }

    private List<String> getSendsPerSecondCsv() {
        return getCsvWithHeader(Arrays.asList("Second", "Number of sent messages"), testDataAggregator.getSendsPerSecond());
    }

    private List<String> getAllResultsCsv() {
        List<String> header = Arrays.asList("Transaction timestamp", "Debezium read timestamp",
                "Kafka receive timestamp", "Debezium read speed",
                "Debezium process speed", "Message count");
        List<List<String>> list = new ArrayList<>();
        list.add(getCountAndSizeHeader());
        list.add(header);
        for (var resultWithCount : testDataAggregator.getAllResultsWithCount().entrySet()) {
            List<String> row = resultWithCount.getKey().getAllValuesWithSqlTimestampAsList();
            row.add(resultWithCount.getValue().toString());
            list.add(row);
        }
        return list.stream().map(this::convertListToCSVRow).collect(Collectors.toList());
    }

    private List<String> getCsvWithHeader(List<String> headers, Map<Long, Integer> data) {
        List<List<String>> list = new ArrayList<>();
        list.add(getCountAndSizeHeader());
        list.add(headers);
        data.forEach((second, count) -> list.add(Arrays.asList(new Timestamp(second).toString(), count.toString())));
        return list.stream().map(this::convertListToCSVRow).collect(Collectors.toList());
    }

    private String generateFileName(String baseName) {
        return currentDateTime + "/" +
                baseName +
                testDataAggregator.getTestNumber() +
                ".csv";
    }

    private List<String> getCountAndSizeHeader() {
        return Arrays.asList("Message count:", String.valueOf(testDataAggregator.getMessageCount()), "Message size (bytes):", String.valueOf(testDataAggregator.getMessageSize()));
    }

}
