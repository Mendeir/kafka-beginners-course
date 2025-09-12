package io.conduktor.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        // Connection to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        for (int batch = 0; batch < 10; batch++) {
            // Creation of Producer
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                for (int counter = 0; counter < 30; counter++) {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                log.info("Received new metadata \n" +
                                        "Topic: " + recordMetadata.topic() + "\n" +
                                        "Partition: " + recordMetadata.partition() + "\n" +
                                        "Offset: " + recordMetadata.offset() + "\n" +
                                        "Timestamp: " + recordMetadata.timestamp() + "\n");
                            } else {
                                log.error("Error while producing", e);
                            }
                        }
                    });
                }

                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                producer.flush();
            }

        }
    }
}
