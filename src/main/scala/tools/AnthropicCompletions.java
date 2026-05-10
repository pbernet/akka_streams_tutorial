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
    // Add your API key
    public static final String API_KEY = "***";

    // https://docs.anthropic.com/en/docs/about-claude/models/overview#model-comparison-table
    public static final String MODEL_NAME = "claude-sonnet-4-6";

    private final ChatModel model;
    private final String moviePlotContext;

    public AnthropicCompletions() {
        this(CompletionsUtil.DEFAULT_TRANSLATOR_SYSTEM_MESSAGE);
    }

    private AnthropicCompletions(String moviePlotContext) {
        this.model = AnthropicChatModel.builder()
                .apiKey(API_KEY)
                .modelName(MODEL_NAME)
                .temperature(0.2)
                .cacheSystemMessages(true)
                .cacheTools(true)
                .maxTokens(8192)
                .maxRetries(10)
                .timeout(Duration.ofMinutes(5))
                .build();
        this.moviePlotContext = moviePlotContext;
    }

    /**
     * Creates a new AnthropicCompletions instance with movie context information.
     *
     * @param movieTitle       The title of the movie
     * @param movieReleaseYear The release year of the movie
     * @return A new AnthropicCompletions instance with the resolved movie plot context
     */
    public static AnthropicCompletions withContext(String movieTitle, int movieReleaseYear) {
        ChatModel contextModel = AnthropicChatModel.builder()
                .apiKey(API_KEY)
                .modelName(MODEL_NAME)
                .temperature(0.1)
                .maxTokens(1024)
                .build();

        String contextPrompt = CompletionsUtil.createMovieContextPrompt(movieTitle, movieReleaseYear);
        ChatResponse response = contextModel.chat(
                UserMessage.from(contextPrompt)
        );

        String movieContext = response.aiMessage().text();
        LOGGER.info("Generated movie context with: {} for: {} ({}):\n{}", MODEL_NAME, movieTitle, movieReleaseYear, movieContext);

        return new AnthropicCompletions(movieContext);
    }

    public ImmutablePair<String, Integer> runCompletions(String prompt) {
        ChatResponse response = model.chat(
                SystemMessage.systemMessage(moviePlotContext),
                UserMessage.from(prompt)
        );
        return new ImmutablePair<>(response.aiMessage().text(), response.tokenUsage().totalTokenCount());
    }

    public static void main(String[] args) {
        String prompt = CompletionsUtil.createTranslationPrompt("This is fun.", "English", "German");

        ImmutablePair<String, Integer> result = new AnthropicCompletions().runCompletions(prompt);
        CompletionsUtil.logCompletionResult(result.getLeft(), result.getRight(), "Translation");

        ImmutablePair<String, Integer> resultWithContext = AnthropicCompletions.withContext("The Hangover", 2009).runCompletions(prompt);
        CompletionsUtil.logCompletionResult(resultWithContext.getLeft(), resultWithContext.getRight(), "Translation with context");
    }

    public String toString() {
        return "Anthropic: " + MODEL_NAME;
    }
}
