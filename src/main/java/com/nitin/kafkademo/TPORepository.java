package com.nitin.kafkademo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TPORepository extends JpaRepository<TopicPartitionOffset, Long> {
    Long getOffsetByTopicAndPartition(String topic, Integer partition);
    TopicPartitionOffset getByTopicAndPartition(String topic, Integer partition);
}
