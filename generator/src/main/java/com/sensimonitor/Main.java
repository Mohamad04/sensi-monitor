package com.sensimonitor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class Main {
    private static final String TOPIC = "trades.raw";
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // 2. create the ObjectMapper
        ObjectMapper mapper = new ObjectMapper();

        Map<String, String> prefixes = new HashMap<>();
        prefixes.put("MUREX", "MX");
        prefixes.put("SUMMIT", "SM");
        prefixes.put("CALYPSO", "CL");

        Map<String, Integer> volumes = Map.of("MUREX", 500, "SUMMIT", 300, "CALYPSO", 200);

        TradeGenerator tradeGenerator = new TradeGenerator();
        List<String> sources = List.of("MUREX", "SUMMIT", "CALYPSO");

        int sent = 0;


        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String source : sources) {
                int volume = volumes.get(source);
                String prefix = prefixes.get(source);
                for (int i = 1; i <= volume; i++) {
                    Trade trade = tradeGenerator.randomTrade(source, prefix, i);

                    String json = mapper.writeValueAsString(trade);

                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC,trade.tradeId() ,json);

                    producer.send(record, (recordMetadata, e) -> {
                        if (e != null) {
                            System.err.println("Failed to send " + trade.tradeId() + ": " + e.getMessage());
                        }
                    });
                    sent++;
                }
            }
        }
        System.out.println("Sent " + sent + " trades");
    }
}