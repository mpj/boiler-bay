package me.mpj;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Wrap the KafkaConsumer in a facade that reduces API
 * surface a bit, with presets suitable for event sourcing.
 */
public class KafkaConsumer {

    public enum AutoOffsetReset {
        smallest,
        largest
    }

    private ConsumerConnector _connector;
    public KafkaStream<byte[], byte[]> stream;

    public KafkaConsumer(String topic, String group, AutoOffsetReset reset) {

        _connector = Consumer.createJavaConsumerConnector(makeConfig(group, reset));

        // The code below is an extremely verbose way of saying our only topic should have one thread.
        // I *think* that it shouldn't be a problem to add more threads, but it requires
        // some serious thinking to make sure we're doing things right, and since a projection
        // is limited to consuming one message at a time (because it cares about ordering) anyway,
        // I imagine the performance gains to very limited for that case. For consumer groups that don't care about
        // ordering, attaching multiple sockets on the same event should work fine in the current solution.
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
        Boolean retryOnFailure = false;
        _connector.commitOffsets(retryOnFailure);
    }

    private ConsumerConfig makeConfig(String group, AutoOffsetReset reset) {

        final Properties props = new Properties();
        props.put("zookeeper.connect", "zk:2181");
        props.put("group.id", group);

        // this property determines if the consumer starts from
        // the head (beginning of log) or the tail (at the end, listening only for new)
        props.put("auto.offset.reset", reset.toString());

        // Use kafka for offset storage instead of slow zookeeper.
        // New feature in 0.8.2
        // http://stackoverflow.com/q/30005390/304262
        props.put("offsets.storage", "kafka");

        // Don't delete stuff, just compact
        props.put("log.cleanup.policy", "compact");




        // Boiler Bay always requires manual commits
        props.put("auto.commit.enable", "false");

        return new ConsumerConfig(props);
    }

    public void shutdown() {
        _connector.shutdown();
    }
}
