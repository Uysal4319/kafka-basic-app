package com.uysal.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import static java.text.MessageFormat.format;
import static java.time.Duration.ofMillis;

public class BasicConsumer {

    public static void main(String[] args) {

        Properties consumerProperties = KafkaProperties.consumerProperties();
        org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProperties);

        consumer.subscribe(Arrays.asList("topic10"));

        System.out.println(format("Start consuming from topic: {0}", "topic10"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(format("Message: offset: {0}, key: {1}, value: {2}, partition: {3}",
                        record.offset(), record.key(), record.value(), record.partition()));
                consumer.commitAsync();
            }

        }
    }

}
