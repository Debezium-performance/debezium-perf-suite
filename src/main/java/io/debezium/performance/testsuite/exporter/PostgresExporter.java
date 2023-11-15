package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.DataAggregator;
import io.debezium.performance.testsuite.model.TimeResults;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import static io.debezium.performance.testsuite.ConfigProperties.RESULT_DATABASE;

public class PostgresExporter implements Exporter {
    String time;

    String testNumber;
    @Override
    public void export(DataAggregator dataAggregator, int testNumber) {
        Date time = new Date(Instant.now().toEpochMilli());
        SimpleDateFormat formatter = new SimpleDateFormat("u_M_d_hh_mm");
        this.time = formatter.format(time);
        this.testNumber = String.valueOf(testNumber);
        try (Connection con = DriverManager.getConnection(RESULT_DATABASE);
             PreparedStatement ps = con.prepareStatement(createTableSql())) {
            ps.executeQuery();
            for (int i = 0; i < dataAggregator.getAllResultsAsStrings().size(); i++) {
                PreparedStatement insert = con.prepareStatement(insertRowSql(dataAggregator.getAllResults().get(i)));
                insert.executeQuery();
                insert.close();
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String createTableSql() {
        String sql = "create table \"" + getTableName() +
                "(" +
                "    \"Debezium read speed\"     integer," +
                "    \"Debezium process speed\"  integer," +
                "    \"Transaction timestamp\"   timestamp," +
                "    \"Debezium read timestamp\" timestamp," +
                "    \"Kafka receive timestamp\" timestamp" +
                ");";
        return sql;
    }

    private String insertRowSql(TimeResults results) {
        String sql ="INSERT INTO public." + getTableName() + " (\"Debezium read speed\", \"Debezium process speed\", \"Transaction timestamp\"," +
                "                                   \"Debezium read timestamp\", \"Kafka receive timestamp\")" +
                "VALUES ("+ results.getDebeziumReadSpeed() +", "+ results.getDebeziumProcessSpeed() +", " +
                "'"+ new Timestamp(results.getDatabaseTransactionTime()) +"', '" + new Timestamp(results.getDebeziumStartTime()) + "'," +
                "'" + new Timestamp(results.getKafkaReceiveTime())+ "')";
        return sql;
    }

    private String getTableName() {
        return this.time + "_" + testNumber +"_totalResults";
    }
}
