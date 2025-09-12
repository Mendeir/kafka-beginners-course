package io.conduktor.demos;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        // Connection to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        for (int batch = 0; batch < 2; batch++) {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
                for (int counter = 0; counter < 10; counter++) {
                    String topic = "demo_java";
                    String key = "id_" + counter;
                    String value = "hello world " + counter;

                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                    producer.send(producerRecord, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e == null) {
                                log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                            } else {
                                log.error("Error while producing", e);
                            }
                        }
                    });
                }
            }
        }

    }
}
