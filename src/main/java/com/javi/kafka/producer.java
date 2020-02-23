package com.javi.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        for (int i = 0; i < 10; i++) {
            String topic = "kedise";
            String value = "< " + i + " >";
            String key = "id_" + i;
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            log.info("Key ->>>> " + key);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {

                        System.out.println("=== Rx metadata ===" +
                            "\ntopic -> " + recordMetadata.topic() +
                            "\npartition -> " + recordMetadata.partition() +
                            "\noffset -> " + recordMetadata.offset() +
                            "\ntime -> " + recordMetadata.timestamp()
                        );
                    } else {
                        log.error("Producing", e);
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
