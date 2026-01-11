package rag.core;

import ai.docling.serve.api.DoclingServeApi;

import java.net.URI;
import java.time.Duration;

/**
 * Factory to create DoclingServeApi instances.
 * This Java class works around Scala-Java interop issues with static generic methods.
 */
public class DoclingClientFactory {

    private DoclingClientFactory() {
        // Utility class
    }

    /**
     * Creates a DoclingServeApi client with the given base URL.
     *
     * @param baseUrl the Docling server URL (e.g., "http://localhost:5001")
     * @return configured DoclingServeApi instance
     */
    public static DoclingServeApi create(String baseUrl) {
        return DoclingServeApi.builder()
                .baseUrl(URI.create(baseUrl))
                .build();
    }

    /**
     * Creates a DoclingServeApi client with custom timeout settings.
     *
     * @param baseUrl           the Docling server URL
     * @param readTimeout       timeout for receiving HTTP responses (sync operations)
     * @param asyncTimeout      timeout for async task polling operations
     * @param asyncPollInterval polling interval for async operations
     * @return configured DoclingServeApi instance
     */
    public static DoclingServeApi create(String baseUrl, Duration readTimeout, Duration asyncTimeout, Duration asyncPollInterval) {
        return DoclingServeApi.builder()
                .baseUrl(URI.create(baseUrl))
                .readTimeout(readTimeout)
                .asyncTimeout(asyncTimeout)
                .asyncPollInterval(asyncPollInterval)
                .build();
    }
}
