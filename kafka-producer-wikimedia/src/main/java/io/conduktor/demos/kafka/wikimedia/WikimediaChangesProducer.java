package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";

        // Connection to localhost
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            String topic = "wikimedia.recentchange";
            BackgroundEventHandler backgroundEventHandler = new WikimediaChangeHandler(producer, topic);

            Map<String, String> headers = new HashMap<>();
            headers.put("User-Agent", "KafkaProducerApp/1.0 (student@example.com)");
            headers.put("Accept", "text/event-stream");

            String url = "https://stream.wikimedia.org/v2/stream/recentchange";
            HttpConnectStrategy connectStrategy = HttpConnectStrategy.http(URI.create(url))
                    .header("User-Agent","my-kafka-producer/1.0(contact: admin@ourcompany.com)");
            EventSource.Builder eventSourceBuilder = new EventSource.Builder(connectStrategy);


            BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(backgroundEventHandler, eventSourceBuilder);
            BackgroundEventSource backgroundEventSource = builder.build();

            backgroundEventSource.start();

            TimeUnit.MINUTES.sleep(10);
        }
    }
}
