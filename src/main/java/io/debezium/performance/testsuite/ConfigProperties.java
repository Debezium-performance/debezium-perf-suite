package io.debezium.performance.testsuite;

public final class ConfigProperties {
    private  ConfigProperties() {}

    public static final String DMT_URL = System.getProperty("test.dmt.url");
    public static final String KAFKA_BOOTSTRAP_SERVERS = System.getProperty("test.kafka.bootstrap.servers");

    public static final String KAFKA_TEST_TOPIC = System.getProperty("test.kafka.topic");
}
