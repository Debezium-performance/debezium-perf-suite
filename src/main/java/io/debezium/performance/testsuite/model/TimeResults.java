package io.debezium.performance.testsuite.model;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeResults {
    private DebeziumTimestamps debeziumTimestamps;
    private long kafkaReceiveTime;

    public TimeResults() {
        kafkaReceiveTime = -1;
    }

    public TimeResults(long databaseTransactionTime, long debeziumStartTime, long kafkaCreateTime) {
        debeziumTimestamps = new DebeziumTimestamps(databaseTransactionTime, debeziumStartTime);
        kafkaReceiveTime = kafkaCreateTime;
    }

    public TimeResults(DebeziumTimestamps debeziumTimestamps, long kafkaReceiveTime) {
        this.debeziumTimestamps = debeziumTimestamps;
        this.kafkaReceiveTime = kafkaReceiveTime;
    }

    public long getDatabaseTransactionTime() {
        return debeziumTimestamps.getDatabaseTransactionTime();
    }

    public void setDatabaseTransactionTime(long databaseTransactionTime) {
        this.debeziumTimestamps.setDatabaseTransactionTime(databaseTransactionTime);
    }

    public long getDebeziumStartTime() {
        return debeziumTimestamps.getDebeziumReadTime();
    }

    public void setDebeziumStartTime(long debeziumStartTime) {
        this.debeziumTimestamps.setDebeziumReadTime(debeziumStartTime);
    }

    public long getKafkaReceiveTime() {
        return kafkaReceiveTime;
    }

    public void setKafkaReceiveTime(long kafkaReceiveTime) {
        this.kafkaReceiveTime = kafkaReceiveTime;
    }

    public long getDebeziumProcessSpeed() {
        if (debeziumTimestamps == null || getDebeziumStartTime() <= 0 || kafkaReceiveTime <= 0) {
            return -1;
        }
        return kafkaReceiveTime - getDebeziumStartTime();
    }

    public long getDebeziumReadSpeed() {
        return debeziumTimestamps.getDebeziumReadSpeed();
    }

    public String[] getAllValues() {
        return new String[]{
                String.valueOf(getDatabaseTransactionTime()),
                String.valueOf(getDebeziumStartTime()),
                String.valueOf(getKafkaReceiveTime()),
                String.valueOf(getDebeziumReadSpeed()),
                String.valueOf(getDebeziumProcessSpeed()),
        };
    }

    public String[] getAllValuesWithSqlTimestamp() {
        return new String[]{
                String.valueOf(new Timestamp(getDatabaseTransactionTime()).toString()),
                String.valueOf(new Timestamp(getDebeziumStartTime()).toString()),
                String.valueOf(new Timestamp(getKafkaReceiveTime()).toString()),
                String.valueOf(getDebeziumReadSpeed()),
                String.valueOf(getDebeziumProcessSpeed()),
        };
    }

    private String getDateFromMs(long ms) {
        Date time = new Date(ms);
        SimpleDateFormat formatter = new SimpleDateFormat("u-M-d hh:mm:ss.SSS");
        return formatter.format(time);
    }

    @Override
    public String toString() {
        return "Database transaction time=" + getDateFromMs(getDatabaseTransactionTime()) +
                ", Debezium receive time=" + getDateFromMs(getDebeziumStartTime()) +
                ", Kafka receive time=" + getDateFromMs(getKafkaReceiveTime()) +
                ", Debezium read speed=" + debeziumTimestamps.getDebeziumReadSpeed() +
                ", Debezium process speed=" + getDebeziumProcessSpeed();
    }
}
