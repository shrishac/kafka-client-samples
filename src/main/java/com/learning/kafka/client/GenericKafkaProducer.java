package com.learning.kafka.client;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A generic class to produce messages to Kafka using Kafka clients
 * default String and ByteArray serializers
 */
public class GenericKafkaProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(GenericKafkaProducer.class);
    private Properties kafkaProperties;
    private KafkaProducer<K, V> producer;


    public GenericKafkaProducer(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
        this.producer = new KafkaProducer<K, V>(this.kafkaProperties);
    }

    public CompletableFuture<Void> send(String topic, K key, V message) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, null, key, message);
        this.producer.send(record, ((metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed posting message to kafka", exception);
                completableFuture.completeExceptionally(exception);
                return;
            }
            logger.info("Successfully posted message to kafka");
            completableFuture.complete(null);
        }));
        return completableFuture;
    }
}
