package cl.duoc.digitalfix.report.infrastructure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class WorkOrderEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(WorkOrderEventConsumer.class);
    private static final Set<String> STATUSES = Set.of("CREATED", "ASSIGNED", "EN_ROUTE", "IN_PROGRESS", "CLOSED", "CANCELLED");
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
        JsonNode payload = event.path("payload");
        JsonNode status = payload.path("status");
        JsonNode payloadOrderId = payload.path("workOrderId");
        if (!status.isTextual() || !STATUSES.contains(status.asText())
                || !event.path("eventType").asText().equals("WORK_ORDER_" + status.asText())
                || (!payloadOrderId.isMissingNode() && (!payloadOrderId.isTextual()
                    || !workOrderId.equals(UUID.fromString(payloadOrderId.asText()))))) {
            throw new IllegalArgumentException("Invalid work order event payload");
        }
        JsonNode version = event.path("schemaVersion");
        if (!version.isMissingNode() && (!version.isInt() || version.intValue() != 1)) {
            throw new IllegalArgumentException("Unsupported event schema version");
        }
        repository.projectWorkOrder(workOrderId);
        log.info("Projected work order eventId={} workOrderId={}", eventId, workOrderId);
    }
}
