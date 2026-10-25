package tools;

import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.anthropic.AnthropicChatModel;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.response.ChatResponse;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Access Anthropic Claude models with LangChain4j
 * <br>
 * Doc:
 * https://docs.anthropic.com/claude/reference/getting-started-with-the-api
 * https://github.com/langchain4j/langchain4j
 */
public class AnthropicCompletions {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnthropicCompletions.class);

    // https://docs.anthropic.com/en/docs/about-claude/models/overview#model-comparison-table
    public static final String MODEL_NAME = "claude-sonnet-5-5";

    private final ChatModel model;
    private final String systemMessage;

    public AnthropicCompletions() {
        this(CompletionsUtil.DEFAULT_TRANSLATOR_SYSTEM_MESSAGE);
    }

    private AnthropicCompletions(String systemMessage) {
        this.model = AnthropicChatModel.builder()
                .apiKey(apiKey())
                .modelName(MODEL_NAME)
                .maxTokens(8192)
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
        ChatModel contextModel = AnthropicChatModel.builder()
                .apiKey(apiKey())
                .modelName(MODEL_NAME)
                .maxTokens(1024)
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
     * Creates a new AnthropicCompletions instance with movie context information.
     *
     * @param movieContext The movie context, e.g. from {@link #generateMovieContext(String, int)}
     * @return A new AnthropicCompletions instance with the movie context in its system message
     */
    public static AnthropicCompletions withContext(String movieContext) {
        return new AnthropicCompletions(CompletionsUtil.createTranslatorSystemMessage(movieContext));
    }

    private static String apiKey() {
        String apiKey = System.getenv("ANTHROPIC_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("ANTHROPIC_API_KEY environment variable not set!");
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

        ImmutablePair<String, Integer> result = new AnthropicCompletions().runCompletions(prompt);
        CompletionsUtil.logCompletionResult(result.getLeft(), result.getRight(), "Translation");

        ImmutablePair<String, Integer> resultWithContext = AnthropicCompletions.withContext(AnthropicCompletions.generateMovieContext("The Hangover", 2009)).runCompletions(prompt);
        CompletionsUtil.logCompletionResult(resultWithContext.getLeft(), resultWithContext.getRight(), "Translation with context");
    }

    public String toString() {
        return "Anthropic: " + MODEL_NAME;
    }
}
