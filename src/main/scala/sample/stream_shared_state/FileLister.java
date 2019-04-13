package sample.stream_shared_state;

import java.io.File;
import java.util.Arrays;
import java.util.List;

/**
 * Read ordered by lastModified
 * https://stackoverflow.com/questions/203030/best-way-to-list-files-in-java-sorted-by-date-modified
 */
public class FileLister {


    public List<File> run(File directory) {
        // Obtain the array of (file, timestamp) pairs.
        File[] files = directory.listFiles();
        Pair[] pairs = new Pair[files.length];
        for (int i = 0; i < files.length; i++)
            pairs[i] = new Pair(files[i]);

        // Sort them by timestamp.
        Arrays.sort(pairs);

        // Take the sorted pairs and extract only the file part, discarding the timestamp.
        for (int i = 0; i < files.length; i++)
            files[i] = pairs[i].f;

        return Arrays.asList(files);
    }
}

class Pair implements Comparable {
    public long t;
    public File f;

    public Pair(File file) {
        f = file;
        t = file.lastModified();
    }

    public int compareTo(Object o) {
        long u = ((Pair) o).t;
        return Long.compare(t, u); //Oldest file is first
    }
}
