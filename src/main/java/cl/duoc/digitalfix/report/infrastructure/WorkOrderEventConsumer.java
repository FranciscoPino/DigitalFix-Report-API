package cl.duoc.digitalfix.report.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class WorkOrderEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(WorkOrderEventConsumer.class);
    private final ObjectMapper objectMapper;
    private final JdbcReportRepository repository;

    public WorkOrderEventConsumer(ObjectMapper objectMapper, JdbcReportRepository repository) {
        this.objectMapper = objectMapper;
        this.repository = repository;
    }

    @KafkaListener(topics = "${KAFKA_WORKORDERS_TOPIC:workorders.events}")
    public void consume(String message) throws Exception {
        JsonNode event = objectMapper.readTree(message);
        if (event == null || !event.isObject() || !event.path("aggregateType").asText().equals("WORK_ORDER")
                || !event.path("aggregateId").isTextual() || !event.path("eventId").isTextual()
                || !event.path("eventType").isTextual() || event.path("eventType").asText().isBlank()
                || !event.path("createdAt").isTextual() || !event.path("payload").isObject()) {
            throw new IllegalArgumentException("Invalid work order event");
        }
        UUID eventId = UUID.fromString(event.path("eventId").asText());
        UUID workOrderId = UUID.fromString(event.path("aggregateId").asText());
        OffsetDateTime.parse(event.path("createdAt").asText());
        repository.projectWorkOrder(workOrderId);
        log.info("Projected work order eventId={} workOrderId={}", eventId, workOrderId);
    }
}
