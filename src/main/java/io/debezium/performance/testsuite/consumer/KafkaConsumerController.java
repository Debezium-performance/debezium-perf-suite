package io.debezium.performance.testsuite.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static io.debezium.performance.testsuite.ConfigProperties.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RECEIVE_BUFFER_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaConsumerController {
    private final Logger LOG = LoggerFactory.getLogger(KafkaConsumerController.class);
    private static KafkaConsumerController instance;
    private final Consumer<String, String> consumer;
    private KafkaConsumerController() {
        consumer = getNewDefaultConsumer();
    }

    public static KafkaConsumerController getInstance() {
        if (instance == null) {
            instance = new KafkaConsumerController();
        }
        return instance;
    }

    public List<ConsumerRecord<String, String>> getRecords(String topic, int count) {
        List<ConsumerRecord<String, String>> collection = new ArrayList<>();
        consumer.subscribe(Collections.singleton(topic));
        while (collection.size() < count) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.SECONDS));
            records.forEach(collection::add);
        }
        return collection;
    }

    public static Consumer<String, String> getNewConsumer(Properties consumerProperties) {
        return new KafkaConsumer<>(getDefaultConsumerProperties());
    }

    public static Consumer<String, String> getNewDefaultConsumer() {
        return new KafkaConsumer<>(getDefaultConsumerProperties());
    }

    public static Properties getDefaultConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put(GROUP_ID_CONFIG, "2");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(MAX_PARTITION_FETCH_BYTES_CONFIG , "1073741824");
        consumerProps.put(FETCH_MAX_BYTES_CONFIG, "1073741824");
        consumerProps.put(FETCH_MAX_WAIT_MS_CONFIG, "10000");
        consumerProps.put(FETCH_MIN_BYTES_CONFIG, "1073741824");
        consumerProps.put(RECEIVE_BUFFER_CONFIG, "1073741824");
        return consumerProps;
    }
}
