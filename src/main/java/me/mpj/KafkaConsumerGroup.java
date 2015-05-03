package me.mpj;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;



public class KafkaConsumerGroup {

    private ConsumerConnector consumerConnector;

    public void shutdown() {
        if (consumerConnector != null) consumerConnector.shutdown();
    }

    public void run(String topic, String zookeeper, String groupId) {

        // Config
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        //props.put("consumer.timeout.ms", "10");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        //
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        ConsumerIterator<byte[], byte[]> it = streams.get(0).iterator();
        // hasNext is blocking and is really only used to detect when the
        // consumer is shutdown so that we can exit cleanly
        while (it.hasNext())
            System.out.println("Printing: " + new String(it.next().message()));


        consumerConnector.commitOffsets();
        System.out.println("Shutting down thread.");

    }


    public static void main(String[] args) {


        //String zooKeeper = args[0];
        //String groupId = args[1];
        //String topic = args[2];
        //int threads = Integer.parseInt(args[3]);

        String zooKeeper = "localhost:2181";
        String groupId = "myGrousp122ss2";
        String topic = "maintopic";

        KafkaConsumerGroup consumerGroup = new KafkaConsumerGroup();

        consumerGroup.run(topic, zooKeeper, groupId);

        try {
            Thread.sleep(10000);
        } catch (InterruptedException ie) {

        }
        consumerGroup.shutdown();
    }
}