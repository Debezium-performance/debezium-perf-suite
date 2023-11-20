package io.debezium.performance.testsuite.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.debezium.performance.testsuite.deserializer.DebeziumTimestampDeserializer;

import java.util.Objects;

@JsonDeserialize(using = DebeziumTimestampDeserializer.class)
public class DebeziumTimestamps {
    private long databaseTransactionTime;
    private long debeziumReadTime;

    public DebeziumTimestamps() {
        databaseTransactionTime = -1;
        debeziumReadTime = -1;
    }

    public DebeziumTimestamps(long databaseTransactionTime, long debeziumReadTime) {
        this.databaseTransactionTime = databaseTransactionTime;
        this.debeziumReadTime = debeziumReadTime;
    }

    public long getDatabaseTransactionTime() {
        return databaseTransactionTime;
    }

    public void setDatabaseTransactionTime(long databaseTransactionTime) {
        this.databaseTransactionTime = databaseTransactionTime;
    }

    public long getDebeziumReadTime() {
        return debeziumReadTime;
    }

    public void setDebeziumReadTime(long debeziumReadTime) {
        this.debeziumReadTime = debeziumReadTime;
    }

    public long getDebeziumReadSpeed() {
        if (databaseTransactionTime <= 0 || debeziumReadTime <= 0) {
            return -1;
        }
        return debeziumReadTime - databaseTransactionTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DebeziumTimestamps that = (DebeziumTimestamps) o;
        return databaseTransactionTime == that.databaseTransactionTime && debeziumReadTime == that.debeziumReadTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseTransactionTime, debeziumReadTime);
    }
}
