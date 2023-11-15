package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.DataAggregator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvExporter implements Exporter {
    public void export(DataAggregator aggregator, int testNumber) {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("dd-MM-HHmm");
        String currentDateTime = LocalDateTime.now().format(format);
        new File(currentDateTime).mkdirs();
        writeToFile(aggregator.getTransactionsPerSecond(), String.format("%s/transactions-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getReadsPerSecond(), String.format("%s/reads-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getSendsPerSecond(), String.format("%s/sends-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getAllResultsAsStrings(), String.format("%s/total-results%s.csv", currentDateTime, testNumber));
    }
    public void writeToFile(List<String[]> lines, String fileName) {
        try(PrintWriter pw = new PrintWriter(fileName)) {
            lines.stream()
                    .map(this::convertToCSV)
                    .forEach(pw::println);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private String convertToCSV(String[] data) {
        return String.join(",", data);
    }

}
