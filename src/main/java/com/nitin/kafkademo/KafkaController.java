package com.nitin.kafkademo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping(value = "/kafka")
@Profile("prodH2Server")
public class KafkaController {

    private final Producer producer;

    @Autowired
    KafkaController(Producer producer) {
        this.producer = producer;
    }

    @PostMapping(value = "/publish")
    public void sendMessageToKafkaTopic(@RequestBody RestRequest message) {
        this.producer.sendMessage(message.getKey(), message.getValue());
    }
}
