package io.debezium.performance.testsuite.consumer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.debezium.performance.testsuite.ConfigProperties.KAFKA_BOOTSTRAP_SERVERS;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG;
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
    private Consumer<String, String> consumer;
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
            LOG.info("{}",collection.size());
        }
        return collection;
    }

    public void deleteAndRecreateTopic(String topic) {
        try (AdminClient client = AdminClient.create(getDefaultAdminProperties())) {
            Config topicConfig = getTopicConfig(client, topic);
            TopicDescription topicDescription = getTopicDescription(client, topic);

            NewTopic newTopic = new NewTopic(topic, topicDescription.partitions().size(), (short) topicDescription.partitions().get(0).replicas().size());
            newTopic.configs(transformConfig(topicConfig));
            LOG.info("Starting topic deletion");
            DeleteTopicsResult result = client.deleteTopics(Collections.singleton(topic));
            Thread.sleep(60000);
            if (result.all().isDone()) {
                LOG.info("Deletion complete");
            } else {
                LOG.info("Deletion NOT complete");
            }
            result.all().get(100, TimeUnit.SECONDS);
            LOG.info("Starting topic creation");
            client.createTopics(Collections.singleton(newTopic)).all().get(100, TimeUnit.SECONDS);
            LOG.info("Successfully recreated topic");
            consumer = getNewDefaultConsumer();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Consumer<String, String> getNewConsumer(Properties consumerProperties) {
        return new KafkaConsumer<>(consumerProperties);
    }

    public static Consumer<String, String> getNewDefaultConsumer() {
        return new KafkaConsumer<>(getDefaultConsumerProperties());
    }

    public static Properties getDefaultConsumerProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        consumerProps.put(GROUP_ID_CONFIG, "2");
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
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
    public static Properties getDefaultAdminProperties() {
        Properties adminProps = new Properties();
        adminProps.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        adminProps.put(CLIENT_ID_CONFIG, "perf-suite-admin-client");
        return adminProps;
    }

    private Map<String, String> transformConfig(Config config) {
        Map<String, String> map = new HashMap<>();
        for (ConfigEntry currentConfig : config.entries()) {
            map.put(currentConfig.name(), currentConfig.value());
        }
        return map;
    }

    private Config getTopicConfig(AdminClient client, String topic) throws ExecutionException, InterruptedException {
        Collection<ConfigResource> cr = Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC,topic));
        DescribeConfigsResult ConfigsResult = client.describeConfigs(cr);
        return (Config) ConfigsResult.all().get().values().toArray()[0];
    }

    private TopicDescription getTopicDescription(AdminClient client, String topic) throws ExecutionException, InterruptedException {
        DescribeTopicsResult result = client.describeTopics(List.of(topic));
        Map<String, KafkaFuture<TopicDescription>>  values = result.topicNameValues();
        KafkaFuture<TopicDescription> topicDescription = values.get(topic);
        return topicDescription.get();
    }
}
