package cl.duoc.digitalfix.report.infrastructure;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcReportRepository {
    private final JdbcTemplate jdbc;

    public JdbcReportRepository(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    public void projectWorkOrder(UUID workOrderId) {
        int updated = jdbc.update("""
                INSERT INTO reporting.order_kpis
                    (work_order_id, created_at, assigned_at, closed_at, final_status, service_id, last_event_at)
                SELECT work_order_id, created_at, assigned_at, closed_at, status, service_id, updated_at
                FROM workorders.work_orders WHERE work_order_id = ?
                ON CONFLICT (work_order_id) DO UPDATE SET
                    created_at = EXCLUDED.created_at,
                    assigned_at = EXCLUDED.assigned_at,
                    closed_at = EXCLUDED.closed_at,
                    final_status = EXCLUDED.final_status,
                    service_id = EXCLUDED.service_id,
                    last_event_at = EXCLUDED.last_event_at
                WHERE reporting.order_kpis.last_event_at <= EXCLUDED.last_event_at
                """, workOrderId);
        if (updated == 0 && !Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT EXISTS(SELECT 1 FROM workorders.work_orders WHERE work_order_id = ?)",
                Boolean.class, workOrderId))) {
            throw new IllegalStateException("Work order not found for projection: " + workOrderId);
        }
    }

    public Map<String, Object> kpis() {
        Integer active = jdbc.queryForObject("SELECT COUNT(*) FROM workorders.work_orders WHERE status NOT IN ('CLOSED', 'CANCELLED')", Integer.class);
        Double resolution = jdbc.queryForObject("SELECT COALESCE(AVG(resolution_minutes), 0) FROM reporting.order_kpis", Double.class);
        Integer orders = jdbc.queryForObject("SELECT COUNT(*) FROM workorders.work_orders WHERE created_at >= NOW() - INTERVAL '24 hours'", Integer.class);
        return Map.of("ordersPerHour", orders == null ? 0 : orders, "averageResolutionMinutes", resolution == null ? 0 : resolution,
                "activeOrders", active == null ? 0 : active, "source", "PostgreSQL/Kafka projection");
    }

    public List<Map<String, Object>> topServices() {
        return jdbc.queryForList("""
                SELECT service_id AS "serviceId", COUNT(*) AS total
                FROM workorders.work_orders
                WHERE created_at >= NOW() - INTERVAL '7 days'
                GROUP BY service_id ORDER BY total DESC LIMIT 10
                """);
    }
}