package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.TestDataAggregator;

public interface Exporter {

    void export(TestDataAggregator dataAggregator, int testNumber);
}
