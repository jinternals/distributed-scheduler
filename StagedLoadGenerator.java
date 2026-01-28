import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class StagedLoadGenerator {
    private static final String API_URL = "http://localhost:8080/tasks";
    private static final int BATCH_1_SIZE = 5000;
    private static final int BATCH_2_SIZE = 5000;
    private static final int TARGET_PARTITION = 1;

    public static void main(String[] args) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        AtomicInteger sentCount = new AtomicInteger(0);

        System.out.println("=== Staged Load Test ===");
        System.out.println("Batch 1: " + BATCH_1_SIZE + " events");
        System.out.println("Batch 2: " + BATCH_2_SIZE + " events (after 60s delay)");
        System.out.println();

        // Batch 1
        System.out.println("Starting Batch 1 at: " + LocalDateTime.now());
        long batch1Start = System.currentTimeMillis();
        sendBatch(client, sentCount, 0, BATCH_1_SIZE);
        long batch1End = System.currentTimeMillis();
        System.out.println("Batch 1 completed in: " + (batch1End - batch1Start) + "ms");
        System.out.println();

        // Wait 60 seconds
        System.out.println("Waiting 60 seconds before Batch 2...");
        Thread.sleep(60000);

        // Batch 2
        System.out.println("Starting Batch 2 at: " + LocalDateTime.now());
        long batch2Start = System.currentTimeMillis();
        sendBatch(client, sentCount, BATCH_1_SIZE, BATCH_1_SIZE + BATCH_2_SIZE);
        long batch2End = System.currentTimeMillis();
        System.out.println("Batch 2 completed in: " + (batch2End - batch2Start) + "ms");
        System.out.println();

        System.out.println("=== Test Complete ===");
        System.out.println("Total events sent: " + sentCount.get());
        System.out.println("Batch 1 time: " + (batch1End - batch1Start) + "ms");
        System.out.println("Batch 2 time: " + (batch2End - batch2Start) + "ms");
    }

    private static void sendBatch(HttpClient client, AtomicInteger sentCount, int startIndex, int endIndex) throws Exception {
        for (int i = startIndex; i < endIndex; i++) {
            String id = UUID.randomUUID().toString();
            String payload = """
                    {
                        "id": "%s",
                        "name": "staged-test-%d",
                        "scheduledTime": "%s",
                        "payload": "staged-load-data"
                    }
                    """.formatted(id, i, LocalDateTime.now().minusHours(24).withNano(0));

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(API_URL))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            try {
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    System.err.println("Error: " + response.statusCode());
                }
                int current = sentCount.incrementAndGet();
                if (current % 500 == 0) {
                    System.out.println("Sent " + current + " events...");
                }
            } catch (Exception e) {
                // Continue on error
            }
        }
    }
}
