package com.poc.location.reactiveconsumer.locationmasterdataconsumer.service;


import com.apmm.javabackendpoc.kafka.avro.model.Location;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Service
@Slf4j
public class ReactiveKafkaConsumerService implements CommandLineRunner {

    private final ReactiveKafkaConsumerTemplate<String, Location > reactiveKafkaConsumerTemplate;

    private final LogAccessor logAccessor = new LogAccessor(LogFactory.getLog(getClass()));

    public ReactiveKafkaConsumerService(ReactiveKafkaConsumerTemplate<String, Location> reactiveKafkaConsumerTemplate) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        onNEventReceived();
    }

    public Disposable onNEventReceived() {
        return reactiveKafkaConsumerTemplate
                .receive()
                .doOnError(error -> {
                    log.error("Error receiving event, Will retry", error);
                })
                .retryWhen(
                        Retry.fixedDelay(Long.MAX_VALUE, Duration.ZERO.withSeconds(5))
                )
                .doOnNext(consumerRecord -> {
                            log.info("received key={}, value={} from topic={}, offset={}", consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord.offset());
                        }
                )
                .concatMap(this::handleEvent)
                .doOnNext(event -> {
                    log.info("successfully consumed {}", event);
                }).subscribe(
                        record-> record.receiverOffset().commit()
                );
    }


    public Mono<ReceiverRecord<String, Location>> handleEvent(ReceiverRecord<String, Location> record) {

        return Mono.just(record).<Location>handle((result, sink) -> {
                        List<DeserializationException> exceptionList = getExceptionList(result);
                        if (!CollectionUtils.isEmpty(exceptionList)) {
                            logError(exceptionList);
                        } else {
                            if (Objects.nonNull(result.value())) {
                                sink.next(result.value());
                            }
                        }
                }).flatMap(this::processRecord)
                .doOnError(error -> {
                    log.error("Error Processing event: key {}", record.key(), error);
                }).onErrorResume(error-> Mono.empty())
                .then(Mono.just(record));
    }


    private Mono<Location> processRecord(Location location) {
        log.info("Processing : {}", location);

        //return Mono.error(new IOException("Input Output Exception"));
        return Mono.just(location);
    }
    private void logError(final List<DeserializationException> exceptionList) {
        exceptionList.forEach(error -> {
            log.error("Deserialization Error: ", error);
        });
    }

    private List<DeserializationException> getExceptionList(ConsumerRecord<String, Location> record) {
        final List<DeserializationException> exceptionList = new ArrayList<>();

        DeserializationException valueException = ListenerUtils.getExceptionFromHeader(record, SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, logAccessor);
        DeserializationException keyException = ListenerUtils.getExceptionFromHeader(record, SerializationUtils.KEY_DESERIALIZER_EXCEPTION_HEADER, logAccessor);

        if (!ObjectUtils.isEmpty(valueException)) {
            exceptionList.add(valueException);
        }

        if (!ObjectUtils.isEmpty(keyException)) {
            exceptionList.add(keyException);
        }

        return exceptionList;
    }

}
