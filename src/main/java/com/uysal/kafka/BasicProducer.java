package com.uysal.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class BasicProducer {

    public static void main(String[] args) {
        int count = 60;
        String msgText = "Test Value ";

        Properties props = KafkaProperties.producerProperties();

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("Started producing messages");
        for (int i = 0; i < count; i++) {
            String msg = msgText + " index: " + i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
                    "topic10",
                    Integer.toString(i),
                    msg);
            producer.send(producerRecord);
        }
        System.out.println(count + " messages sent successfully");

        producer.close();
    }

}
