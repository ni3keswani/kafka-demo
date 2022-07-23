package com.nitin.kafkademo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
@Slf4j
@Profile("consumer")
public class Consumer implements ConsumerSeekAware {
    ConsumerSeekCallback consumerSeekCallback = null;
    Map<TopicPartition, Long> assignments = null;

    @Autowired
    TPORepository tpoRepository;

    private static final String TOPIC = "RGB";
    @KafkaListener(topics = TOPIC, groupId = "${spring.kafka.consumer.group-id}")
    public void consume(User message,
                        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                        @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
                        @Header(KafkaHeaders.OFFSET)Long offset) throws IOException {
        log.info(String.format("Key - %s, In Partition %d, offset - %d, Consumed message -> %s", key, partition, offset, message));
        TopicPartitionOffset topicPartitionOffset = tpoRepository.getByTopicAndPartition(TOPIC, partition);
        if(topicPartitionOffset!=null) {
            topicPartitionOffset.setTopic(TOPIC);
            topicPartitionOffset.setPartition(partition);
            topicPartitionOffset.setPoffset(offset);
            tpoRepository.save(topicPartitionOffset);
        } else {
            topicPartitionOffset = new TopicPartitionOffset();
            topicPartitionOffset.setTopic(TOPIC);
            topicPartitionOffset.setPartition(partition);
            topicPartitionOffset.setPoffset(offset);
            tpoRepository.save(topicPartitionOffset);
        }
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        ConsumerSeekAware.super.registerSeekCallback(callback);
        consumerSeekCallback = callback;
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        ConsumerSeekAware.super.onPartitionsAssigned(assignments, callback);
        this.assignments = assignments;
        // save to database.
        /*assignments.entrySet().forEach(e -> {
            TopicPartitionOffset topicPartitionOffset = new TopicPartitionOffset();
            topicPartitionOffset.setTopic(e.getKey().topic());
            topicPartitionOffset.setPartition(e.getKey().partition());
            topicPartitionOffset.setPoffset(e.getValue());
            tpoRepository.save(topicPartitionOffset);
        });*/
        // Log offsets
        assignments.entrySet().forEach(e -> {
            log.info(String.format("Received: Topic- %s, Partition- %d, Offset- %d", e.getKey().topic(), e.getKey().partition(), e.getValue()));
        });
        // seek offset
        assignments.entrySet().forEach(e -> {
            Long offset = tpoRepository.getOffsetByTopicAndPartition(e.getKey().topic(), e.getKey().partition());
            callback.seek(e.getKey().topic(), e.getKey().partition(), offset==null ? 0 : offset);
        });
    }
}
