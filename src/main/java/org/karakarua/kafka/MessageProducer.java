package org.karakarua.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MessageProducer {

    public static void main(String[] args) {
        Properties conf = new Properties();
        conf.put("bootstrap.servers", "localhost:9092");
        conf.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Object, Object> producer = new KafkaProducer<>(conf);

        String message = "hello kafka";

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);
        executorService.scheduleAtFixedRate(() -> {
            System.out.println("producing message: " + message);
            producer.send(new ProducerRecord<>("my-topic", message));
        }, 0, 10, TimeUnit.SECONDS);
        producer.close();
    }
}
