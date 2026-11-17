# ms-digitalfix-report

API de consulta para indicadores operacionales DigitalFix. No modifica órdenes ni catálogo.

## Arquitectura y patrón

Bounded context de reportería con interfaz REST y repositorio JDBC. El consumidor Kafka actualiza `reporting.order_kpis` consultando el estado actual de `workorders.work_orders` en PostgreSQL. Las demás consultas existentes siguen leyendo directamente `workorders.work_orders`. Los eventos repetidos son seguros y una proyección antigua no sobrescribe una más reciente.

## Configuración local

Configura `.env`: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_SSL_MODE=require`. El servicio escucha en `8084`.

Kafka usa `KAFKA_BOOTSTRAP_SERVERS=localhost:19092,localhost:19093,localhost:19094`, `KAFKA_WORKORDERS_TOPIC=workorders.events`, `KAFKA_DLT_TOPIC=workorders.events.report.DLT` y `KAFKA_GROUP_ID=digitalfix-report` por defecto. Son configurables como variables de entorno. Para levantar el clúster y sus tópicos, consulta [Kafka local](../kafka/README.md).

### Contrato del evento

Cada mensaje es JSON, con clave Kafka igual al UUID de la orden para preservar su orden por partición:

```json
{
  "eventId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
  "eventType": "WORK_ORDER_CLOSED",
  "aggregateType": "WORK_ORDER",
  "aggregateId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "createdAt": "2026-01-01T10:00:00Z",
  "payload": {"workOrderId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "status": "CLOSED", "changedBy": "user-1"}
}
```

El contrato corresponde a los campos de `integration.outbox_events`: `event_id`, `event_type`, `aggregate_type`, `aggregate_id`, `created_at` y `payload`. Usa un `eventId` estable para cada evento, también al reintentar. La orden debe existir en la base compartida antes de publicar; la proyección usa su estado actual, no el estado histórico del mensaje.

Workorders publica desde la outbox. Report confirma el offset tras procesar cada registro; después de tres reintentos de 5 segundos envía fallos a su DLT. Si falla esa publicación, no se confirma el registro. `earliest` se aplica a grupos nuevos; no reinicia offsets existentes.

## Ejecutar y probar

```powershell
mvn clean verify
mvn spring-boot:run
```

Las pruebas opcionales de integración con Kafka requieren tópicos de entrada y DLT de prueba ya creados, con el mismo número de particiones. Usan un repositorio simulado y no escriben en PostgreSQL:

```powershell
$env:DIGITALFIX_KAFKA_TEST = "true"
$env:KAFKA_WORKORDERS_TOPIC = "<topic-de-prueba>"
$env:KAFKA_DLT_TOPIC = "<topic-dlt-de-prueba>"
$env:KAFKA_GROUP_ID = "digitalfix-report-test-$([guid]::NewGuid())"
mvn test
```

Para probar además la persistencia, `DIGITALFIX_TEST_DB_URL` debe apuntar a una base PostgreSQL **vacía y desechable**, con usuario `postgres` y contraseña `test`. Las pruebas crean esquemas dentro de una transacción que revierten al finalizar. Sin esta variable se omiten.

Prueba los GET desde Postman contra `http://localhost:8080` (BFF) con Bearer token cuando esté habilitada la seguridad, o directamente contra `http://localhost:8084` en desarrollo.

| Método | Ruta | Descripción |
|---|---|---|
| GET | `/api/report/kpis?range=last24h` | Órdenes últimas 24h, activas y tiempo medio de resolución |
| GET | `/api/report/top-services?range=last7d` | Servicios más solicitados en los últimos 7 días |

Ejemplo:

```powershell
curl.exe "http://localhost:8080/api/report/kpis?range=last24h" -H "Authorization: Bearer $env:ACCESS_TOKEN"
```

`range` se acepta como parámetro; por ahora las ventanas usadas por las consultas son fijas (24 horas y 7 días).
