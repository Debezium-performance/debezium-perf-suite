package io.debezium.performance.testsuite.exporter;

import io.debezium.performance.testsuite.DataAggregator;

public interface Exporter {

    void export(DataAggregator dataAggregator, int testNumber);
}
