package cl.duoc.digitalfix.report.infrastructure;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

class WorkOrderEventConsumerTest {
    private static final UUID ORDER_ID = UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    private static final String EVENT = """
            {"eventId":"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb","eventType":"WORK_ORDER_CLOSED",
             "aggregateType":"WORK_ORDER","aggregateId":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
             "createdAt":"2026-01-01T10:00:00Z","payload":{"status":"CLOSED"}}
            """;

    @Test
    void projectsValidEventAndPropagatesDatabaseFailures() throws Exception {
        JdbcReportRepository repository = mock(JdbcReportRepository.class);
        WorkOrderEventConsumer consumer = new WorkOrderEventConsumer(new ObjectMapper(), repository);
        consumer.consume(EVENT);
        verify(repository).projectWorkOrder(ORDER_ID);
        doThrow(new IllegalStateException("database unavailable")).when(repository).projectWorkOrder(ORDER_ID);
        assertThrows(IllegalStateException.class, () -> consumer.consume(EVENT));
    }

    @Test
    void rejectsMalformedEventWithoutProjecting() {
        JdbcReportRepository repository = mock(JdbcReportRepository.class);
        WorkOrderEventConsumer consumer = new WorkOrderEventConsumer(new ObjectMapper(), repository);
        assertThrows(Exception.class, () -> consumer.consume(EVENT.replace("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "bad")));
        assertThrows(Exception.class, () -> consumer.consume(EVENT.replace("2026-01-01T10:00:00Z", "bad")));
        verifyNoInteractions(repository);
    }

    @Test
    void missingOrderFailsButAnOlderProjectionIsSafeToIgnore() {
        JdbcTemplate jdbc = mock(JdbcTemplate.class);
        JdbcReportRepository repository = new JdbcReportRepository(jdbc);
        when(jdbc.update(anyString(), eq(ORDER_ID))).thenReturn(0);
        when(jdbc.queryForObject(anyString(), eq(Boolean.class), eq(ORDER_ID))).thenReturn(false, true);
        assertThrows(IllegalStateException.class, () -> repository.projectWorkOrder(ORDER_ID));
        repository.projectWorkOrder(ORDER_ID);
    }
}
