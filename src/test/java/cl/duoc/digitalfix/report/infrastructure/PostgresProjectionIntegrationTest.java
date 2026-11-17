package cl.duoc.digitalfix.report.infrastructure;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

@EnabledIfEnvironmentVariable(named = "DIGITALFIX_TEST_DB_URL", matches = "jdbc:postgresql:.*")
class PostgresProjectionIntegrationTest {
    @Test
    void upsertsCurrentStateWithoutRegressingAndRejectsMissingOrders() throws Exception {
        try (Connection connection = DriverManager.getConnection(System.getenv("DIGITALFIX_TEST_DB_URL"), "postgres", "test")) {
            connection.setAutoCommit(false);
            JdbcTemplate jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            jdbc.execute("CREATE SCHEMA workorders");
            jdbc.execute("CREATE SCHEMA reporting");
            jdbc.execute("CREATE TYPE workorders.order_status AS ENUM ('CREATED', 'CLOSED')");
            jdbc.execute("""
                    CREATE TABLE workorders.work_orders (work_order_id uuid PRIMARY KEY, created_at timestamptz NOT NULL,
                    assigned_at timestamptz, closed_at timestamptz, status workorders.order_status NOT NULL,
                    service_id uuid NOT NULL, updated_at timestamptz NOT NULL)
                    """);
            jdbc.execute("""
                    CREATE TABLE reporting.order_kpis (work_order_id uuid PRIMARY KEY, created_at timestamptz NOT NULL,
                    assigned_at timestamptz, closed_at timestamptz, final_status workorders.order_status NOT NULL,
                    service_id uuid NOT NULL, last_event_at timestamptz NOT NULL)
                    """);
            UUID orderId = UUID.randomUUID();
            jdbc.update("""
                    INSERT INTO workorders.work_orders VALUES (?, '2026-01-01T10:00:00Z', NULL, NULL,
                    'CREATED', ?, '2026-01-01T10:00:00Z')
                    """, orderId, UUID.randomUUID());
            JdbcReportRepository repository = new JdbcReportRepository(jdbc);
            repository.projectWorkOrder(orderId);
            repository.projectWorkOrder(orderId);
            jdbc.update("""
                    UPDATE workorders.work_orders SET status = 'CLOSED', closed_at = '2026-01-01T12:00:00Z',
                    updated_at = '2026-01-01T12:00:00Z' WHERE work_order_id = ?
                    """, orderId);
            repository.projectWorkOrder(orderId);
            jdbc.update("UPDATE workorders.work_orders SET updated_at = '2026-01-01T09:00:00Z', status = 'CREATED' WHERE work_order_id = ?", orderId);
            repository.projectWorkOrder(orderId);
            assertEquals("CLOSED", jdbc.queryForObject("SELECT final_status::text FROM reporting.order_kpis WHERE work_order_id = ?", String.class, orderId));
            assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM reporting.order_kpis", Integer.class));
            assertThrows(IllegalStateException.class, () -> repository.projectWorkOrder(UUID.randomUUID()));
            connection.rollback();
        }
    }
}
