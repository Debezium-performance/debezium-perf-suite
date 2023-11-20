package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.DataAggregator;
import io.debezium.performance.testsuite.consumer.KafkaConsumerController;
import io.debezium.performance.testsuite.model.TimeResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;

import static io.debezium.performance.testsuite.ConfigProperties.RESULT_DATABASE;

public class PostgresExporter implements Exporter {

    private final Logger LOG = LoggerFactory.getLogger(PostgresExporter.class);
    String time;

    String testNumber;
    @Override
    public void export(DataAggregator dataAggregator, int testNumber) {
        Date time = new Date(Instant.now().toEpochMilli());
        SimpleDateFormat formatter = new SimpleDateFormat("yy_M_d_hh_mm");
        this.time = formatter.format(time);
        this.testNumber = String.valueOf(testNumber);
        try (Connection con = DriverManager.getConnection(RESULT_DATABASE);
             PreparedStatement ps = con.prepareStatement(createTableSql())) {
            ps.executeUpdate();
            LOG.info("Created table " + getTableName());
            Statement stmt = con.createStatement();
            int batchCounter = 0;
//            int testCounter = 0;
//            for (var entry: dataAggregator.getAllResultsWithCount().entrySet()) {
//                LOG.info(entry.getKey().toString());
//                LOG.info(entry.getValue().toString());
//                if (testCounter == 10) {
//                    break;
//                }
//                testCounter++;
//            }
            for (var entry: dataAggregator.getAllResultsWithCount().entrySet()) {
                stmt.addBatch(insertRowSql(entry.getKey(), entry.getValue()));
                if (batchCounter % 100000 == 0) {
                    stmt.executeBatch();
                }
                batchCounter++;
            }
            stmt.executeBatch();
            stmt.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String createTableSql() {
        String sql = "create table \"" + getTableName() +
                "\"(" +
                "    \"Debezium read speed\"     integer," +
                "    \"Debezium process speed\"  integer," +
                "    \"Transaction timestamp\"   timestamp," +
                "    \"Debezium read timestamp\" timestamp," +
                "    \"Kafka receive timestamp\" timestamp," +
                "    \"Message count\" integer" +
                ");";
        return sql;
    }

    private String insertRowSql(TimeResults results, Integer count) {
        String sql ="INSERT INTO \"" + getTableName() + "\" (\"Debezium read speed\", \"Debezium process speed\", \"Transaction timestamp\"," +
                "                                   \"Debezium read timestamp\", \"Kafka receive timestamp\", \"Message count\")" +
                "VALUES ("+ results.getDebeziumReadSpeed() +", "+ results.getDebeziumProcessSpeed() +", " +
                "'"+ new Timestamp(results.getDatabaseTransactionTime()) +"', '" + new Timestamp(results.getDebeziumStartTime()) + "'," +
                "'" + new Timestamp(results.getKafkaReceiveTime()) + "', '" + count.toString() +  "')";
        return sql;
    }

    private String getTableName() {
        return this.time + "_" + testNumber +"_totalResults";
    }
}
