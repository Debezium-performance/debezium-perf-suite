package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.DataAggregator;

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

public class CsvExporter implements Exporter {

    DataAggregator dataAggregator;
    String currentDateTime;

    public CsvExporter(DataAggregator dataAggregator) {
        this.dataAggregator = dataAggregator;
        Date time = new Date(Instant.now().toEpochMilli());
        SimpleDateFormat formatter = new SimpleDateFormat("yy_M_d_hh_mm");
        this.currentDateTime = formatter.format(time);
    }

    public void export(DataAggregator aggregator, int testNumber) {
        new File(currentDateTime).mkdirs();
        writeToFile(getTransactionsPerSecondCsv(), generateFileName("transactions-per-second"));
        writeToFile(aggregator.getReadsPerSecondAsList(), String.format("%s/reads-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getSendsPerSecondAsList(), String.format("%s/sends-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getAllResultsWithHeadersAsList(), String.format("%s/total-results-counts%s.csv", currentDateTime, testNumber));
    }

    private void writeToFile(List<List<String>> lines, String fileName) {
        try(PrintWriter pw = new PrintWriter(fileName)) {
            lines.stream()
                    .map(this::convertListToCSVRow)
                    .forEach(pw::println);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertListToCSVRow(List<String> data) {
        return String.join(",", data);
    }

    private List<List<String>> getTransactionsPerSecondCsv() {
        return getCsvWithHeader(Arrays.asList("Seconds", "Number of read messages"), dataAggregator.getTransactionsPerSecond());
    }

    private List<List<String>> getCsvWithHeader(List<String> headers, Map<Long, Integer> map) {
        List<List<String>> list = new ArrayList<>();
        list.add(getCountAndSizeHeader());
        list.add(headers);
        map.forEach((second, count) -> list.add(Arrays.asList(new Timestamp(second).toString(), count.toString())));
        return list;
    }

    private String generateFileName(String baseName) {
        return currentDateTime + "/" +
                baseName +
                dataAggregator.getTestNumber() +
                ".csv";
    }

    private List<String> getCountAndSizeHeader(){
        return Arrays.asList("Message count:", String.valueOf(dataAggregator.getMessageCount()), "Message size (bytes):", String.valueOf(dataAggregator.getMessageSize()));
    }

}
