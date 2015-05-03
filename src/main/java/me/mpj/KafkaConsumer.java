package me.mpj;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumer {

    private ConsumerConnector _connector;
    public KafkaStream<byte[], byte[]> stream;

    public KafkaConsumer(String topic, String group) {

        _connector = Consumer.createJavaConsumerConnector(makeConfig(group));

        // Extremely verbose way of saying our only topic should have one thread
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                _connector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // Since this consumer only has a single thread, we just get the first
        // stream and return an iterator for that
        stream = streams.get(0);
    }

    public void commitOffsets() {
        Boolean retryOnFailure = true;
        _connector.commitOffsets(retryOnFailure);
    }

    private ConsumerConfig makeConfig(String group) {

        final Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.99.100:49157");
        props.put("group.id", group);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        //props.put("consumer.timeout.ms", "10");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");

        // Use kafka for offset storage instead of slow zookeeper
        // http://stackoverflow.com/q/30005390/304262
        props.put("offsets.storage", "kafka");

        return new ConsumerConfig(props);
    }

    public void shutdown() {
        _connector.shutdown();
    }
}
