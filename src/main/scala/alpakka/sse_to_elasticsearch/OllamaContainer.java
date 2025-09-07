package alpakka.sse_to_elasticsearch;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;

public class OllamaContainer extends GenericContainer<OllamaContainer> {
    private static final int DEFAULT_PORT = 11434;
    private static final String DEFAULT_IMAGE = "sysnet4admin/ollama-llama3.2:1b";
    private static final String DEFAULT_TAG = "latest";

    public static void main(String[] args) throws InterruptedException {
        OllamaContainer ollamaContainer = new OllamaContainer();
        ollamaContainer.start();
        Thread.sleep(100000000);
    }

    public OllamaContainer() {
        this(DEFAULT_IMAGE + ":" + DEFAULT_TAG);
    }

    public OllamaContainer(String image) {
        super(image == null ? DEFAULT_IMAGE + ":" + DEFAULT_TAG : image);
        addExposedPort(DEFAULT_PORT);
        waitingFor(Wait.forHttp("/api/tags")
                .forPort(DEFAULT_PORT)
                .withStartupTimeout(Duration.ofMinutes(5)));
    }

    public String getHostAddress() {
        return getHost() + ":" + getMappedPort(DEFAULT_PORT);
    }

    public String getBaseUrl() {
        return "http://" + getHostAddress();
    }
}