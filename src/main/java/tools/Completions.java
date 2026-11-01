package tools;

import org.apache.commons.lang3.tuple.ImmutablePair;

/**
 * An LLM, which answers a prompt with the response text and the number of tokens used
 */
public interface Completions {
    ImmutablePair<String, Integer> runCompletions(String prompt);
}
