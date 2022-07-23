package com.nitin.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@Profile("prodH2Server")
public class Producer {
    private static final String TOPIC = "RGB";

    @Autowired
    private KafkaTemplate<String, User> kafkaTemplate;

    public void sendMessage(String key, User value) {
        log.info(String.format("Producing message: %s - %s", key, value));
        this.kafkaTemplate.send(TOPIC, key, value);
    }
}
