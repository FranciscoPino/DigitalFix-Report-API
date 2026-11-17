package cl.duoc.digitalfix.report.infrastructure;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.kafka.support.KafkaHeaders;

@EnabledIfEnvironmentVariable(named = "DIGITALFIX_KAFKA_TEST", matches = "true")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class LocalKafkaIntegrationTest {
    @MockitoBean
    JdbcReportRepository repository;

    @Value("${spring.kafka.bootstrap-servers}")
    String bootstrapServers;

    @Value("${KAFKA_WORKORDERS_TOPIC:workorders.events}")
    String topic;

    @Value("${KAFKA_DLT_TOPIC:workorders.events.report.DLT}")
    String dltTopic;

    @Test
    void consumesFromKafka() throws Exception {
        UUID orderId = UUID.randomUUID();
        doThrow(new IllegalStateException("temporary database failure")).doNothing()
                .when(repository).projectWorkOrder(orderId);
        String message = """
                {"eventId":"%s","eventType":"WORK_ORDER_CLOSED","aggregateType":"WORK_ORDER",
                 "aggregateId":"%s","createdAt":"2026-01-01T10:00:00Z","payload":{"status":"CLOSED"}}
                """.formatted(UUID.randomUUID(), orderId);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                "bootstrap.servers", bootstrapServers,
                "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class))) {
            producer.send(new ProducerRecord<>(topic, orderId.toString(), message)).get(10, TimeUnit.SECONDS);
            verify(repository, timeout(30000).times(2)).projectWorkOrder(orderId);
        }
    }

    @Test
    void sendsFailedRecordToConsumerDltAfterThreeRetries() throws Exception {
        UUID orderId = UUID.randomUUID();
        doThrow(new IllegalStateException("database failure")).when(repository).projectWorkOrder(orderId);
        String message = """
                {"eventId":"%s","eventType":"WORK_ORDER_CLOSED","aggregateType":"WORK_ORDER",
                 "aggregateId":"%s","createdAt":"2026-01-01T10:00:00Z","payload":{"status":"CLOSED"}}
                """.formatted(UUID.randomUUID(), orderId);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                    "bootstrap.servers", bootstrapServers, "key.serializer", StringSerializer.class,
                    "value.serializer", StringSerializer.class));
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    "bootstrap.servers", bootstrapServers, "group.id", UUID.randomUUID().toString(),
                    "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class))) {
            TopicPartition partition = new TopicPartition(dltTopic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            producer.send(new ProducerRecord<>(topic, 0, orderId.toString(), message)).get(10, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            boolean found = false;
            while (System.nanoTime() < deadline && !found) {
                for (var record : consumer.poll(Duration.ofSeconds(1))) {
                    if (orderId.toString().equals(record.key())) {
                        assertEquals(message, record.value());
                        assertNotNull(record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN));
                        found = true;
                    }
                }
            }
            assertTrue(found, "Failed record was not sent to DLT");
            verify(repository, timeout(5000).times(4)).projectWorkOrder(orderId);
        }
    }
}
