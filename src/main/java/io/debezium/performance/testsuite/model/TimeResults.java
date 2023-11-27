package io.debezium.performance.testsuite.model;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class TimeResults {
    private final DebeziumTimestamps debeziumTimestamps;
    private final long kafkaReceiveTime;

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

    public long getDebeziumStartTime() {
        return debeziumTimestamps.getDebeziumReadTime();
    }

    public long getKafkaReceiveTime() {
        return kafkaReceiveTime;
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

    public List<String> getAllValuesWithSqlTimestampAsList() {
        List<String> list =  new ArrayList<> ();
        list.add(String.valueOf(new Timestamp(getDatabaseTransactionTime()).toString()));
        list.add(String.valueOf(new Timestamp(getDebeziumStartTime()).toString()));
        list.add(String.valueOf(new Timestamp(getKafkaReceiveTime()).toString()));
        list.add(String.valueOf(getDebeziumReadSpeed()));
        list.add(String.valueOf(getDebeziumProcessSpeed()));
        return list;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimeResults results = (TimeResults) o;
        return kafkaReceiveTime == results.kafkaReceiveTime && Objects.equals(debeziumTimestamps, results.debeziumTimestamps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(debeziumTimestamps, kafkaReceiveTime);
    }
}
