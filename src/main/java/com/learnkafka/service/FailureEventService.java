package com.learnkafka.service;

import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureEventService {
    private FailureRecordRepository failureRecordRepository;

    public FailureEventService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveRecord(ConsumerRecord<Integer,String> consumerRecord, Exception exception, String status) {
    var record = FailureRecord.builder()
            .id(null)
            .topic(consumerRecord.topic())
            .key_value(consumerRecord.key())
            .errorRecord(consumerRecord.value())
            .partition(consumerRecord.partition())
            .offset_value(consumerRecord.offset())
            .exception(exception.getCause().getMessage())
            .status(status)
            .build();
    failureRecordRepository.save(record);
    }
}
