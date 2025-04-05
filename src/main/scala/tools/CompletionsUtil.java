package tools;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for common functionality used by different LLM completion implementations
 */
public class CompletionsUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(CompletionsUtil.class);

    /**
     * Default system message for translation tasks
     */
    public static final String DEFAULT_TRANSLATOR_SYSTEM_MESSAGE = "You are a translator";

    /**
     * Creates a translation prompt
     *
     * @param text           Text to translate
     * @param sourceLanguage Source language
     * @param targetLanguage Target language
     * @return Formatted translation prompt
     */
    public static String createTranslationPrompt(String text, String sourceLanguage, String targetLanguage) {
        return String.format("Translate the following subtitle text from %s to %s: %s",
                sourceLanguage, targetLanguage, text);
    }

    /**
     * Creates a movie context prompt
     *
     * @param movieTitle       The title of the movie
     * @param movieReleaseYear The release year of the movie
     * @return Formatted movie context prompt
     */
    public static String createMovieContextPrompt(String movieTitle, int movieReleaseYear) {
        return String.format(
                "Find movie metadata about the movie '%s' released in %d\n" +
                        "If you don't know this movie or if the title or release year is ambiguous: Respond with \"N/A\", do not apologize." +
                        "Structure of the response:\n" +
                        "Plot summary: [Max 100 words]" +
                        "Locations: [Max 3 key locations]" +
                        "Key characters: [Max 5 main character names, no actor names]" +
                        "Themes: [Max 3 main themes]" +
                        "Notable scenes: [Max 3 brief descriptions of memorable scenes]",
                movieTitle, movieReleaseYear);
    }

    /**
     * Logs completion results
     *
     * @param result     The completion result
     * @param tokenCount The token count
     * @param context    Additional context information (optional)
     */
    public static void logCompletionResult(String result, int tokenCount, String context) {
        if (context != null && !context.isEmpty()) {
            LOGGER.info("{}: {}", context, result);
        } else {
            LOGGER.info("Result: {}", result);
        }
        LOGGER.info("Total tokens: {}", tokenCount);
    }
}