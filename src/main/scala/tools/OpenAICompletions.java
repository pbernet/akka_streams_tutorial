package tools;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * We use Apache HTTP client for graceful retry behaviour
 * <br>
 * Doc:
 * https://platform.openai.com/docs/guides/chat/chat-vs-completions
 * https://beta.openai.com/docs/api-reference/completions/create
 */
public class OpenAICompletions {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenAICompletions.class);

    // API key: https://beta.openai.com/account/api-keys
    public static final String API_KEY = "***";

    public static final int RESPONSE_TIMEOUT_SECONDS = 180;

    public static void main(String[] args) throws IOException {
        String toTranslate = String.format("Translate the following subtitle text from English to German: %s", "This is fun.");
        ImmutablePair<String, Integer> resultRaw = new OpenAICompletions().runChatCompletions(toTranslate);
        //ImmutablePair<String, Integer> resultRaw = new TranslatorOpenAI().runCompletions(toTranslate);
        LOGGER.info("Translation: {}", resultRaw.getLeft());
        LOGGER.info("Total tokens: {}", resultRaw.getRight());
    }

    public ImmutablePair<String, Integer> runCompletions(String prompt) {
        JSONObject requestParams = new JSONObject();
        requestParams.put("model", "text-davinci-003");
        requestParams.put("prompt", prompt);

        // The token limit for "text-davinci-003" is 4000 tokens (=  sum of prompt and completion tokens)
        requestParams.put("max_tokens", 1000);

        // Sampling temperature: Higher values means the model will take more risks (0-1)
        // In the context of translations: control the degree of deviation from the source text
        // High temperature value: the model generates a more creative or expressive translation
        // Low temperature value:  the model generates a more literal or faithful translation
        requestParams.put("temperature", 0.2);
        return extractPayloadCompletions(postRequest(requestParams, "completions"));
    }

    public ImmutablePair<String, Integer> runChatCompletions(String prompt) {

        JSONArray messages = new JSONArray();
        JSONObject jo = new JSONObject();
        jo.put("role", "user");
        jo.put("content", prompt);
        messages.put(jo);

        JSONObject requestParams = new JSONObject();
        requestParams.put("model", "gpt-3.5-turbo");
        requestParams.put("messages", messages);

        // The token limit for "gpt-35-turbo" is 4096 tokens (=  sum of prompt and completion tokens)
        requestParams.put("max_tokens", 1000);

        // Sampling temperature: Higher values means the model will take more risks (0-1)
        // In the context of translations: control the degree of deviation from the source text
        // High temperature value: the model generates a more creative or expressive translation
        // Low temperature value:  the model generates a more literal or faithful translation
        requestParams.put("temperature", 0.2);
        return extractPayloadChatCompletions(postRequest(requestParams, "chat/completions"));
    }

    private String postRequest(JSONObject requestParams, String endpoint) {
        HttpPost request = new HttpPost("https://api.openai.com/v1/" + endpoint);
        request.setHeader("Authorization", "Bearer " + API_KEY);
        StringEntity requestEntity = new StringEntity(
                requestParams.toString(),
                ContentType.APPLICATION_JSON);
        request.setEntity(requestEntity);

        RequestConfig timeoutsConfig = RequestConfig.custom()
                .setResponseTimeout(Timeout.of(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .build();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create()
                .setDefaultRequestConfig(timeoutsConfig)
                .setRetryStrategy(new DefaultHttpRequestRetryStrategy(5, TimeValue.ofMinutes(5L)))
                .build()) {
            return IOUtils.toString(httpClient.execute(request).getEntity().getContent(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOGGER.warn("Unable to get result from openai completions endpoint. Cause: ", e);
        }
        return "N/A";
    }

    private static ImmutablePair<String, Integer> extractPayloadChatCompletions(String jsonResponseChatCompletions) {
        LOGGER.debug("RAW Response JSON: " + jsonResponseChatCompletions);
        JSONObject obj = new JSONObject(jsonResponseChatCompletions);

        JSONArray arr = obj.getJSONArray("choices");
        JSONObject msg = arr.getJSONObject(0);
        checkLength(msg);

        String content = msg.getJSONObject("message").getString("content");
        int totalTokens = obj.getJSONObject("usage").getInt("total_tokens");
        return new ImmutablePair<>(content, totalTokens);
    }

    private ImmutablePair<String, Integer> extractPayloadCompletions(String jsonResponseCompletions) {
        LOGGER.debug("RAW Response JSON: " + jsonResponseCompletions);
        JSONObject obj = new JSONObject(jsonResponseCompletions);

        JSONArray arr = obj.getJSONArray("choices");
        JSONObject msg = arr.getJSONObject(0);
        checkLength(msg);

        String content = msg.getString("text");
        int totalTokens = obj.getJSONObject("usage").getInt("total_tokens");
        return new ImmutablePair<>(content, totalTokens);
    }

    private static void checkLength(JSONObject obj) {
        String finish_reason = obj.getString("finish_reason");
        if (finish_reason.equals("length")) {
            LOGGER.warn("finish_reason has value 'length'. Increase max_tokens to get full response.");
        }
    }
}