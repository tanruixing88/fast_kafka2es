package core;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import config.Config;

/**
 * @author tanruixing  
 * Created on 2019-03-20
 */
public class Kafka {
    private static final Logger logger = LoggerFactory.getLogger(Process.class);
    private static Properties properties;

    private static String broker;
    private static String group;
    private static String topic;
    private static HashMap<String, String> groupConfigHashMap = new HashMap<>();

    private KafkaConsumer<byte[], byte[]> kafkaConsumer;

    public Kafka() {

    }

    public Kafka(Config config) {
        this.broker = config.selectOneBroker();
        this.group = config.getGroup();
        this.topic = config.getTopics();
        this.groupConfigHashMap = config.getGroupConfigHashMap();
    }

    public void initKafkaProperties() {
        properties = new Properties();
        properties.put("bootstrap.servers", broker);
        properties.put("group.id", group);
        properties.put("enable.auto.commit", true);
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        for (Map.Entry<String, String> entry : this.groupConfigHashMap.entrySet()) {
            logger.info("group config key:{} value:{}", entry.getKey(), entry.getValue());
            properties.put(entry.getKey(), entry.getValue());
        }
    }

    public void initKafkaConsumer() {
        kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
    }

    public void consumerTopicData(ES es, ProcMsg procMsg) {
        while (!Process.isShutdownFlag()) {
            ConsumerRecords<byte[], byte[]> consumerRecords = kafkaConsumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                procMsg.input2output(consumerRecord.value(), es);
            }
        }
    }
}
