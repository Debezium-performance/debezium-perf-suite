package io.debezium.performance.testsuite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class CsvExporter {

    public static void exportToCSVFiles(DataAggregator aggregator, int testNumber) {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("dd-MM-HHmm");
        String currentDateTime = LocalDateTime.now().format(format);
        new File(currentDateTime).mkdirs();
        writeToFile(aggregator.getTransactionsPerSecond(), String.format("%s/transactions-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getReadsPerSecond(), String.format("%s/reads-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getSendsPerSecond(), String.format("%s/sends-per-second%s.csv", currentDateTime, testNumber));
        writeToFile(aggregator.getAllResults(), String.format("%s/total-results%s.csv", currentDateTime, testNumber));
    }
    public static void writeToFile(List<String[]> lines, String fileName) {
        try(PrintWriter pw = new PrintWriter(fileName)) {
            lines.stream()
                    .map(CsvExporter::convertToCSV)
                    .forEach(pw::println);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static String convertToCSV(String[] data) {
        return String.join(",", data);
    }

}
