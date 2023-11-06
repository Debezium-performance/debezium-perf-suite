package io.debezium.performance.testsuite;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;

public class CsvExporter {

    public static void exportToCSVFiles(DataAggregator aggregator, int testNumber) {
        writeToFile(aggregator.getTransactionsPerSecond(), String.format("transactions-per-second%s.csv", testNumber));
        writeToFile(aggregator.getReadsPerSecond(), String.format("reads-per-second%s.csv", testNumber));
        writeToFile(aggregator.getSendsPerSecond(), String.format("sends-per-second%s.csv", testNumber));
        writeToFile(aggregator.getAllResults(), String.format("total-results%s.csv", testNumber));
    }
    public static void writeToFile(List<String[]> lines, String fileName) {
        try(PrintWriter pw = new PrintWriter(fileName)) {
            lines.stream()
                    .map(CsvExporter::convertToCSV)
                    .forEach(pw::println);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static String convertToCSV(String[] data) {
        return String.join(",", data);
    }

}
