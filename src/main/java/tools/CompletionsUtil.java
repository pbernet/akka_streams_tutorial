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
     * Creates a movie context prompt optimized for subtitle translation.
     * Focuses on elements most relevant to understanding dialogue and cultural context.
     *
     * @param movieTitle       The title of the movie
     * @param movieReleaseYear The release year of the movie
     * @return Formatted movie context prompt optimized for subtitle translation
     */
    public static String createMovieContextPrompt(String movieTitle, int movieReleaseYear) {
        return String.format(
                "Provide context for subtitle translation of '%s' (%d).\n" +
                        "If unknown/ambiguous: respond \"N/A\".\n" +
                        "Genre: [1-2 genres]\n" +
                        "Setting: [Time period, main location]\n" +
                        "Key characters: [Max 4 main character names with brief role]\n" +
                        "Cultural context: [Important cultural/historical references for translation, Max 100 words]\n" +
                        "Language style: [Formal/informal, period-specific terms, slang]",
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