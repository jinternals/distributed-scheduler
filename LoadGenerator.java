import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


public class LoadGenerator {

    private static final String API_URL = "http://localhost:8080/tasks";
    private static final int TOTAL_REQUESTS = 100000;
    private static final int CONCURRENCY = 50;

    public static void main(String[] args) {
        System.out.println("Starting Load Generator...");
        System.out.println("Target: " + API_URL);
        System.out.println("Requests: " + TOTAL_REQUESTS);
        System.out.println("Concurrency: " + CONCURRENCY);

        HttpClient client = HttpClient.newHttpClient();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        
        long startTime = System.currentTimeMillis();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < TOTAL_REQUESTS; i++) {
            final int id = i;
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    String json = String.format(
                        "{\"name\":\"task-%d\", \"scheduledTime\":null, \"payload\":\"load-test-payload-%d\"}", 
                        id, id
                    );

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(API_URL))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(json))
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    
                    if (response.statusCode() == 200 || response.statusCode() == 201) {
                        successCount.incrementAndGet();
                    } else {
                        System.err.println("Failed: " + response.statusCode() + " " + response.body());
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    failureCount.incrementAndGet();
                }
            }, executor);
            futures.add(future);
        }

        // Wait for all
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long endTime = System.currentTimeMillis();

        executor.shutdown();
        
        long duration = endTime - startTime;
        System.out.println("========================================");
        System.out.println("Load Test Finished");
        System.out.println("Duration: " + duration + " ms");
        System.out.println("Throughput: " + (TOTAL_REQUESTS / (duration / 1000.0)) + " req/sec");
        System.out.println("Success: " + successCount.get());
        System.out.println("Failed: " + failureCount.get());
        System.out.println("========================================");
    }
}
