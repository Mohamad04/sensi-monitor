package com.sensimonitor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws Exception {
        // 1. create the Properties and put the 4 settings
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 2. create the ObjectMapper
        ObjectMapper mapper = new ObjectMapper();
        // 3. try-with-resources: create the KafkaProducer
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            //    3a. create one Trade
            Trade trade = new Trade(
                    "T0001",
                    "Murex",
                    "Rates",
                    "EUR",
                    0.01,
                    0.05,
                    "2026-11-18"
            );
            //    3b. convert it to JSON
            String json = mapper.writeValueAsString(trade);
            //    3c. create the ProducerRecord (topic "trades.raw", key = tradeId, value = json)
            ProducerRecord<String, String> record = new ProducerRecord<>("trades.raw",trade.tradeId() ,json);
            //    3d. send it WITH a callback that prints partition + offset, or the error
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("Error: " + exception.getMessage());
                }
                else {
                    System.out.println("Offset = "+metadata.offset() + ", Partition = " + metadata.partition() + ", topic = " + metadata.topic());
                }
            });

        }

    }
}