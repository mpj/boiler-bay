package me.mpj;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.HashMap;
import java.util.concurrent.ExecutionException;

public class KafkaProducer {

    private final org.apache.kafka.clients.producer.KafkaProducer _producer;

    public KafkaProducer() {
        java.util.Map<java.lang.String,java.lang.Object> configs = new HashMap<>();

        // kafka brokers
        configs.put("bootstrap.servers", "kfk:9092,kfk:9093,kfk:9094");

        configs.put("key.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class);
        configs.put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class);

        // Require highest durability before returning
        configs.put("request.required.acks", "-1");
        _producer = new org.apache.kafka.clients.producer.KafkaProducer(configs);
    }

    public RecordMetadata send(String topic, String partitionKey, String body) throws ExecutionException, InterruptedException {
        // We don't provide a partition integer to ProducerRecord, which makes the Kafka
        // client pick the partition by hashing the partitionKey
        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, partitionKey.getBytes(), body.getBytes());
        final java.util.concurrent.Future<RecordMetadata> future = _producer.send(record);
        final RecordMetadata recordMetadata = future.get();
        return recordMetadata;
    }

    public void shutdown() {
        _producer.close();
    }

}
