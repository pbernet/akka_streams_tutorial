package tools;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.response.ChatResponse;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Access OpenAI models with LangChain4j
 * <br>
 * Doc:
 * https://platform.openai.com/docs/api-reference
 * https://github.com/langchain4j/langchain4j
 */
public class OpenAICompletions {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenAICompletions.class);

    public static final String MODEL_NAME = "gpt-6-luna";
    public static final String REASONING_EFFORT = "none";

    private final ChatModel model;
    private final String systemMessage;

    public OpenAICompletions() {
        this(CompletionsUtil.DEFAULT_TRANSLATOR_SYSTEM_MESSAGE);
    }

    private OpenAICompletions(String systemMessage) {
        this.model = OpenAiChatModel.builder()
                .apiKey(apiKey())
                .modelName(MODEL_NAME)
                .reasoningEffort(REASONING_EFFORT)
                .temperature(0.2)
                .maxCompletionTokens(4096)
                .maxRetries(10)
                .timeout(Duration.ofMinutes(5))
                .build();
        this.systemMessage = systemMessage;
    }

    /**
     * Generates movie context information to improve translation quality.
     *
     * @param movieTitle       The title of the movie
     * @param movieReleaseYear The release year of the movie
     * @return The generated movie context
     */
    public static String generateMovieContext(String movieTitle, int movieReleaseYear) {
        ChatModel contextModel = OpenAiChatModel.builder()
                .apiKey(apiKey())
                .modelName(MODEL_NAME)
                .reasoningEffort(REASONING_EFFORT)
                .temperature(0.2)
                .maxCompletionTokens(1024)
                .build();

        String contextPrompt = CompletionsUtil.createMovieContextPrompt(movieTitle, movieReleaseYear);
        ChatResponse response = contextModel.chat(
                UserMessage.from(contextPrompt)
        );

        String movieContext = response.aiMessage().text();
        LOGGER.info("Generated movie context with: {} for: {} ({}):\n{}", MODEL_NAME, movieTitle, movieReleaseYear, movieContext);

        return movieContext;
    }

    /**
     * Creates a new OpenAICompletions instance with movie context information.
     *
     * @param movieContext The movie context, e.g. from {@link #generateMovieContext(String, int)}
     * @return A new OpenAICompletions instance with the movie context in its system message
     */
    public static OpenAICompletions withContext(String movieContext) {
        return new OpenAICompletions(CompletionsUtil.createTranslatorSystemMessage(movieContext));
    }

    private static String apiKey() {
        String apiKey = System.getenv("OPENAI_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("OPENAI_API_KEY environment variable not set!");
        }
        return apiKey;
    }

    public ImmutablePair<String, Integer> runCompletions(String prompt) {
        ChatResponse response = model.chat(
                SystemMessage.systemMessage(systemMessage),
                UserMessage.from(prompt)
        );
        return new ImmutablePair<>(response.aiMessage().text(), response.tokenUsage().totalTokenCount());
    }

    public static void main(String[] args) {
        String prompt = CompletionsUtil.createTranslationPrompt("This is fun.", "English", "German");

        ImmutablePair<String, Integer> result = new OpenAICompletions().runCompletions(prompt);
        CompletionsUtil.logCompletionResult(result.getLeft(), result.getRight(), "Translation");

        ImmutablePair<String, Integer> resultWithContext = OpenAICompletions.withContext(OpenAICompletions.generateMovieContext("Bob Marley - One Love", 2024)).runCompletions(prompt);
        CompletionsUtil.logCompletionResult(resultWithContext.getLeft(), resultWithContext.getRight(), "Translation with context");
    }

    public String toString() {
        return "OpenAI: " + MODEL_NAME;
    }
}
