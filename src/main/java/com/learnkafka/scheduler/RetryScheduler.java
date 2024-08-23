package com.learnkafka.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.learnkafka.config.LibraryEventsConsumerConfig;
import com.learnkafka.entity.FailureRecord;
import com.learnkafka.jpa.FailureRecordRepository;
import com.learnkafka.jpa.LibraryEventsRepository;
import com.learnkafka.service.FailureEventService;
import com.learnkafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RetryScheduler {

    private LibraryEventsService libraryEventsService;
    private FailureRecordRepository failureRecordRepository;

    public RetryScheduler(LibraryEventsService libraryEventsService, FailureRecordRepository failureRecordRepository) {
        this.libraryEventsService = libraryEventsService;
        this.failureRecordRepository = failureRecordRepository;
    }

    @Scheduled(fixedRate = 1000)
    public void retryFailedRecords() {
        log.info("Pooling for the failed records");
        failureRecordRepository.findAllByStatus(LibraryEventsConsumerConfig.RETRY)
                .forEach(failureRecord -> {
                    var consumerRecord = buildConsumerRecord(failureRecord);
                    try {
                        libraryEventsService.processLibraryEvent(consumerRecord);
                        failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
                        failureRecordRepository.save(failureRecord);
                    } catch (Exception e) {
                        log.error("Exception in retryFailedRecords  block {} ", e.getMessage(), e);
                    }
                });
        log.info("failed records processed.");
    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {
        return new ConsumerRecord<>(failureRecord.getTopic()
                , failureRecord.getPartition()
                , failureRecord.getOffset_value()
                , failureRecord.getKey_value()
                , failureRecord.getErrorRecord()
        );
    }
}
