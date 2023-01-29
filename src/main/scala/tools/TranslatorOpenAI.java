package tools;

import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * PoC with Apache HTTP client
 * <br>
 * Doc:
 * https://beta.openai.com/docs/api-reference/completions/create
 */
public class TranslatorOpenAI {
    private static final Logger LOGGER = LoggerFactory.getLogger(TranslatorOpenAI.class);

    // API key: https://beta.openai.com/account/api-keys
    public static final String API_KEY = "***";

    public static final int DELAY_TO_RETRY_SECONDS = 10;

    public static void main(String[] args) throws IOException {
        String toTranslate = String.format("Translate the following subtitle text from English to German: %s", "This is fun.");
        String resultRaw = new TranslatorOpenAI().run(toTranslate);
        LOGGER.info("DE: {}", resultRaw);
    }

    public String run(String prompt) throws IOException {
        JSONObject requestParams = new JSONObject();
        requestParams.put("model", "text-davinci-003");
        requestParams.put("prompt", prompt);

        // Max tokens in the response. For testing use lower number
        requestParams.put("max_tokens", 200);
        // Sampling temperature: Higher values means the model will take more risks (0-1)
        // In the context of translations: control the degree of deviation from the source text
        // High temperature value: the model generates a more creative or expressive translation
        //  Open phrases may be translated in a creative fashion, eg.
        //  "By the way," ->> "Bye the way, I like your shoes"
        // Low temperature value:  the model generates a more literal or faithful translation
        requestParams.put("temperature", 0.2);

        HttpPost request = new HttpPost("https://api.openai.com/v1/completions");
        request.setHeader("Authorization", "Bearer " + API_KEY);
        StringEntity requestEntity = new StringEntity(
                requestParams.toString(),
                ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);

        RequestConfig timeoutsConfig = RequestConfig.custom()
                .setConnectTimeout(Timeout.of(DELAY_TO_RETRY_SECONDS, TimeUnit.SECONDS)).build();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(timeoutsConfig)
                .setRetryStrategy(new DefaultHttpRequestRetryStrategy(3, TimeValue.ofMinutes(1L)))
                .build()) {
            return IOUtils.toString(httpClient.execute(request).getEntity().getContent(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOGGER.warn("Unable to get result from openai completions endpoint. Cause: ", e);
        }
        return "N/A";
    }
}