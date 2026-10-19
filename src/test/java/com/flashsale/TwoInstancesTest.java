package com.flashsale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import com.flashsale.common.ApiHeaders;
import com.flashsale.inventory.ConcurrencyStrategy;
import com.flashsale.inventory.StockGate;
import com.flashsale.support.Concurrently;

/**
 * Phase 3's "done when: correctness holds with 2 instances". Two complete copies of the app run in this JVM,
 * each with its own Tomcat port, its own Spring beans, its own in-memory state (SaleCache's map, its own
 * scheduler jobs, its own Kafka consumer) and its own connection pool, sharing only what production shares:
 *   - 100 buyers, half holding on instance 1 and paying on instance 2 (and the other way round): no oversell,
 *     with every safe strategy, and Redis's gate still equals PostgreSQL
 *   - every order is announced exactly once, with both instances' relays and consumers running
 *   - one buyer's rate limit is shared: hitting both instances doesn't double it
 *   - nginx: spreads requests over both, turns away a burst from one IP (429 IP_RATE_LIMITED), and keeps
 *     serving (GETs and POSTs) when an instance dies
 *
 * Not an IntegrationTest: that one starts ONE app with a fake clock. This class needs two apps and the real clock.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TwoInstancesTest {

    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"));
    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.1"));

    private static final HttpClient HTTP = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private static ConfigurableApplicationContext app1;
    private static ConfigurableApplicationContext app2;
    private static GenericContainer<?> nginx;
    private static JdbcTemplate jdbc;
    private static StringRedisTemplate redis;

    @BeforeAll
    static void startTheServicesTwoAppsAndNginx() {
        Startables.deepStart(POSTGRES, REDIS, KAFKA).join();
        app1 = startApp("app1");
        app2 = startApp("app2");
        jdbc = app1.getBean(JdbcTemplate.class);
        redis = app1.getBean(StringRedisTemplate.class);

        nginx = new GenericContainer<>(DockerImageName.parse("nginx:1.27-alpine"))
                .withCopyFileToContainer(MountableFile.forHostPath("nginx/templates/default.conf.template"),
                        "/etc/nginx/templates/default.conf.template")
                .withExtraHost("host.docker.internal", "host-gateway")
                .withCommand("sh", "-c", "HOST=$(getent ahostsv4 host.docker.internal | head -1 | cut -d' ' -f1)"
                        + " && export UPSTREAM_1=$HOST:" + port(app1) + " UPSTREAM_2=$HOST:" + port(app2)
                        + " IP_RATE=20r/s IP_BURST=40"
                        + " && exec /docker-entrypoint.sh nginx -g 'daemon off;'")
                .withExposedPorts(80)
                .waitingFor(Wait.forListeningPort());
        nginx.start();
    }

    @AfterAll
    static void stopEverything() {
        for (ConfigurableApplicationContext app : new ConfigurableApplicationContext[] {app1, app2}) {
            if (app != null && app.isActive()) {
                app.close();
            }
        }
        if (nginx != null) {
            nginx.stop();
        }
    }

    @Order(1)
    @ParameterizedTest
    @EnumSource(value = ConcurrencyStrategy.class, names = {"PESSIMISTIC", "OPTIMISTIC", "CONDITIONAL_UPDATE",
            "REDIS_LOCK"})
    void buyersSpreadOverBothInstancesNeverOversell(ConcurrencyStrategy strategy) {
        int units = 20;
        long saleItemId = createSale(strategy.name(), units, null);

        // 100 buyers at once. Buyer i holds on one instance and pays on the OTHER one.
        List<Concurrently.Outcome<Integer>> buyers = Concurrently.run(100, i -> {
            String buyer = strategy + "-buyer-" + i;
            HttpResponse<String> hold = post(i % 2 == 0 ? app1 : app2, "/api/holds", buyer,
                    Map.of(ApiHeaders.CONCURRENCY_STRATEGY, strategy.name()),
                    "{\"items\":[{\"saleItemId\":" + saleItemId + ",\"quantity\":1}]}");
            if (hold.statusCode() == 201) {
                long holdId = json(hold).get("holdId").asLong();
                HttpResponse<String> pay = post(i % 2 == 0 ? app2 : app1, "/api/holds/" + holdId + "/pay", buyer,
                        Map.of(), "{\"provider\":\"STRIPE\",\"paymentToken\":\"tok_success\"}");
                assertThat(pay.statusCode()).as("paying on the other instance").isEqualTo(200);
            }
            return hold.statusCode();
        });

        assertThat(Concurrently.failures(buyers)).isEmpty();
        Map<Integer, Long> statuses = buyers.stream()
                .collect(Collectors.groupingBy(Concurrently.Outcome::value, Collectors.counting()));
        // 201 held, 409 sold out; 503 = "busy, retry" (OPTIMISTIC out of attempts, REDIS_LOCK out of time) is allowed
        assertThat(statuses.keySet()).isSubsetOf(Set.of(201, 409, 503));
        long sold = statuses.getOrDefault(201, 0L);
        int available = jdbc.queryForObject("SELECT available_stock FROM sale_items WHERE id = ?", Integer.class,
                saleItemId);
        Integer orders = jdbc.queryForObject("""
                SELECT COUNT(*) FROM orders o JOIN hold_items hi ON hi.hold_id = o.hold_id
                 WHERE hi.sale_item_id = ?""", Integer.class, saleItemId);

        assertThat(sold).as("never more than the stock").isLessThanOrEqualTo(units);
        assertThat(available).as("stock left = stock - sold").isEqualTo(units - (int) sold);
        assertThat(orders).as("one order per hold, paid on the other instance").isEqualTo((int) sold);
        if (!statuses.containsKey(503)) {
            assertThat(sold).as("with nobody told to retry, every unit sells").isEqualTo(units);
        }
        // Redis's gate (shared by both instances) agrees with PostgreSQL
        long saleId = jdbc.queryForObject("SELECT sale_id FROM sale_items WHERE id = ?", Long.class, saleItemId);
        assertThat(redis.opsForValue().get(StockGate.key(saleId, saleItemId))).isEqualTo(String.valueOf(available));
    }

    @Order(2)
    @Test
    void everyOrderIsAnnouncedExactlyOnceByTwoRelaysAndTwoConsumers() throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            await().atMost(Duration.ofSeconds(30)).until(() -> admin.describeConsumerGroups(List.of("notifications"))
                    .all().get(5, TimeUnit.SECONDS).get("notifications").members().size() == 2);
        }
        int orders = jdbc.queryForObject("SELECT COUNT(*) FROM orders", Integer.class);
        assertThat(orders).isPositive();

        // Both relays run every 500 ms (SKIP LOCKED); both consumers read their partitions
        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE status = 'SENT'", Integer.class) == orders);
        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.queryForObject(
                "SELECT COUNT(*) FROM notifications", Integer.class) == orders);
        assertThat(jdbc.queryForObject("SELECT COUNT(DISTINCT order_id) FROM notifications", Integer.class))
                .as("each order once").isEqualTo(orders);
    }

    @Order(3)
    @Test
    void aBuyersRateLimitIsSharedByBothInstances() {
        long saleItemId = createSale("rate-limit", 1, null);

        // 20 writes at once from one buyer, alternating between the instances. An in-memory limiter would allow 10 per
        // instance (20 in all); the shared bucket allows 10 (plus at most one token that drips in meanwhile).
        List<Concurrently.Outcome<Integer>> requests = Concurrently.run(20, i -> post(i % 2 == 0 ? app1 : app2,
                "/api/holds", "greedy-bob", Map.of(),
                "{\"items\":[{\"saleItemId\":" + saleItemId + ",\"quantity\":1}]}").statusCode());

        long limited = requests.stream().filter(outcome -> outcome.value() == 429).count();
        assertThat(limited).as("429s out of 20").isBetween(9L, 10L);
    }

    @Order(4)
    @Test
    void nginxSpreadsRequestsOverBothInstances() throws Exception {   // the first request nginx ever gets: see below
        long saleId = jdbc.queryForObject("SELECT MIN(id) FROM sales", Long.class);
        List<String> answeredBy = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // A NEW connection per request, like 10 different browsers, on an nginx that has served nothing yet.
            // That is when a missing "zone" shows: each new connection lands on a fresh worker whose own turn counter
            // still points at app1. (With one shared connection, or after the 100-request burst has moved every
            // worker's counter, this test passed even with the zone removed: both were tried.)
            try (HttpClient browser = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
                HttpResponse<String> page = browser.send(HttpRequest.newBuilder(URI.create(
                        nginxUrl("/api/sales/" + saleId))).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(page.statusCode()).isEqualTo(200);
                answeredBy.add(page.headers().firstValue("X-Upstream").orElseThrow());
            }
        }
        assertThat(Set.copyOf(answeredBy)).as("both instances answered").hasSize(2);
    }

    @Order(5)
    @Test
    void nginxTurnsAwayABurstFromOneIpAddress() throws InterruptedException {
        waitForTheIpBucketToEmpty();
        long saleId = jdbc.queryForObject("SELECT MIN(id) FROM sales", Long.class);
        List<Concurrently.Outcome<HttpResponse<String>>> burst =
                Concurrently.run(100, i -> get(nginxUrl("/api/sales/" + saleId)));

        List<HttpResponse<String>> limited = burst.stream().map(Concurrently.Outcome::value)
                .filter(response -> response.statusCode() == 429).toList();
        long ok = burst.stream().filter(outcome -> outcome.value().statusCode() == 200).count();
        assertThat(ok).as("the burst allowance (40) and the first request get through").isGreaterThanOrEqualTo(41);
        assertThat(limited).as("the rest is turned away").isNotEmpty();
        HttpResponse<String> answer = limited.getFirst();
        assertThat(json(answer).get("code").asString()).isEqualTo("IP_RATE_LIMITED");
        assertThat(answer.headers().firstValue("Retry-After")).hasValue("1");
        assertThat(answer.headers().firstValue("X-Upstream")).as("answered by nginx, not an app").hasValue("nginx");
    }

    @Order(6)
    @Test
    void whenAnInstanceDiesNginxSendsEverythingToTheOther() throws InterruptedException {
        long saleItemId = createSale("failover", 5, null);
        long saleId = jdbc.queryForObject("SELECT sale_id FROM sale_items WHERE id = ?", Long.class, saleItemId);
        waitForTheIpBucketToEmpty();
        String app2Port = ":" + port(app2);

        app1.close();   // like "docker compose stop app1": its port stops accepting connections

        for (int i = 0; i < 10; i++) {
            HttpResponse<String> page = get(nginxUrl("/api/sales/" + saleId));
            assertThat(page.statusCode()).isEqualTo(200);
            assertThat(page.headers().firstValue("X-Upstream").orElseThrow()).endsWith(app2Port);
        }
        // A POST works too: nginx retries a POST on the other instance only when the first one never received it
        // (the connection was refused), which is the case here
        HttpResponse<String> hold = postTo(nginxUrl("/api/holds"), "carol", Map.of(),
                "{\"items\":[{\"saleItemId\":" + saleItemId + ",\"quantity\":1}]}");
        assertThat(hold.statusCode()).isEqualTo(201);
    }

    // ---- helpers ----

    /**
     * drains at 20 a second: after 2.5 s it is empty again, whatever the previous test did.
     */
    private static void waitForTheIpBucketToEmpty() throws InterruptedException {
        Thread.sleep(2500);
    }

    private static ConfigurableApplicationContext startApp(String name) {
        return new SpringApplicationBuilder(FlashSaleEngineApplication.class).run(
                "--server.port=0",
                "--spring.application.name=" + name,
                "--spring.main.banner-mode=off",
                "--spring.datasource.url=" + POSTGRES.getJdbcUrl(),
                "--spring.datasource.username=" + POSTGRES.getUsername(),
                "--spring.datasource.password=" + POSTGRES.getPassword(),
                "--spring.data.redis.host=" + REDIS.getHost(),
                "--spring.data.redis.port=" + REDIS.getMappedPort(6379),
                "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "--flashsale.payments.fake-latency=0ms");
    }

    private static int port(ConfigurableApplicationContext app) {
        return Integer.parseInt(app.getEnvironment().getProperty("local.server.port"));
    }

    private static String nginxUrl(String path) {
        return "http://" + nginx.getHost() + ":" + nginx.getMappedPort(80) + path;
    }

    /** Creates a live sale with one item through instance 1's admin API; returns the sale item id. */
    private static long createSale(String name, int stock, Integer perUserLimit) {
        Instant now = Instant.now();
        String body = "{\"name\":\"" + name + "\",\"startsAt\":\"" + now.minusSeconds(60) + "\",\"endsAt\":\""
                + now.plusSeconds(3600) + "\",\"items\":[{\"productName\":\"iPhone\",\"regularPrice\":100,"
                + "\"salePrice\":90,\"stock\":" + stock + "}]"
                + (perUserLimit == null ? "" : ",\"perUserLimit\":" + perUserLimit) + "}";
        HttpResponse<String> sale = send(HttpRequest.newBuilder(URI.create(base(app1) + "/api/admin/sales"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build());
        assertThat(sale.statusCode()).isEqualTo(201);
        return json(sale).get("items").get(0).get("saleItemId").asLong();
    }

    private static HttpResponse<String> post(ConfigurableApplicationContext app, String path, String userId,
                                             Map<String, String> headers, String body) {
        return postTo(base(app) + path, userId, headers, body);
    }

    private static HttpResponse<String> postTo(String url, String userId, Map<String, String> headers, String body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .header("Content-Type", "application/json")
                .header(ApiHeaders.USER_ID, userId)
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .POST(HttpRequest.BodyPublishers.ofString(body));
        headers.forEach(request::header);
        return send(request.build());
    }

    private static HttpResponse<String> get(String url) {
        return send(HttpRequest.newBuilder(URI.create(url)).GET().build());
    }

    private static HttpResponse<String> send(HttpRequest request) {
        try {
            return HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IllegalStateException(request.uri() + ": " + e.getMessage(), e);
        }
    }

    private static String base(ConfigurableApplicationContext app) {
        return "http://localhost:" + port(app);
    }

    private static JsonNode json(HttpResponse<String> response) {
        return JSON.readTree(response.body());
    }
}
