package cl.duoc.digitalfix.report.interfaces;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import cl.duoc.digitalfix.report.infrastructure.JdbcReportRepository;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/report")
@PreAuthorize("@securityProperties.disabled() or hasAnyAuthority('Admin', 'ROLE_Admin')")
public class ReportController {
    private final JdbcReportRepository repository;

    public ReportController(JdbcReportRepository repository) { this.repository = repository; }
    @GetMapping("/kpis")
    public Map<String, Object> kpis(@RequestParam(defaultValue = "last24h") String range) {
        return new java.util.HashMap<>(repository.kpis()) {{ put("range", range); }};
    }

    @GetMapping("/top-services")
    public List<Map<String, Object>> topServices(@RequestParam(defaultValue = "last7d") String range) {
        return repository.topServices();
    }
}
