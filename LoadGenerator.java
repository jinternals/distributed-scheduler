
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadGenerator {

    private static final int TARGET_PARTITION = 1;
    private static final int NUM_PARTITIONS = 12;
    private static final int TOTAL_EVENTS = 10000;
    private static final String API_URL = "http://localhost:8080/tasks";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Load Generator...");
        System.out.println("Target: " + TOTAL_EVENTS + " events to Partition " + TARGET_PARTITION);

        HttpClient client = HttpClient.newBuilder()
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();

        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        AtomicInteger sentCount = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < TOTAL_EVENTS; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    String id = generateIdForPartition(TARGET_PARTITION);
                    String payload = """
                            {
                                "id": "%s",
                                "name": "load-test-%d",
                                "scheduledTime": "%s",
                                "payload": "load-data"
                            }
                        """.formatted(id, index, LocalDateTime.now().minusHours(24).withNano(0)); 

                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(API_URL))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .build();

                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                
                if (index == 0) {
                    System.out.println("First Response: " + response.statusCode() + " " + response.body());
                }

                if (response.statusCode() != 200) {
                    System.err.println("Error: " + response.statusCode() + " " + response.body());
                }

                int current = sentCount.incrementAndGet();
                if (current % 1000 == 0) {
                    System.out.println("Sent " + current + " events...");
                }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        executor.close(); // Waits for all tasks
        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Finished sending " + TOTAL_EVENTS + " events in " + duration + "ms");
    }

    private static String generateIdForPartition(int targetPartition) {
        while (true) {
            String id = UUID.randomUUID().toString();
            if (Math.abs(id.hashCode() % NUM_PARTITIONS) == targetPartition) {
                return id;
            }
        }
    }
}
