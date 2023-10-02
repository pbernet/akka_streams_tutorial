package alpakka.file;

import org.apache.pekko.util.ByteString;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Because we don't have support for un-archive in Alpakka files module yet
 * <p>
 * Inspired by:
 * [[docs.javadsl.ArchiveHelper]]
 */
public class ArchiveHelper {
    private final static int CHUNK_SIZE = 1024;

    public Map<String, ByteString> unzip(ByteString zipArchive) throws Exception {

        Map<String, ByteString> result = new HashMap<>();
        ZipEntry entry;

        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipArchive.toArray()))) {
            while ((entry = zis.getNextEntry()) != null) {
                int count;
                byte[] data = new byte[CHUNK_SIZE];

                ByteArrayOutputStream dest = new ByteArrayOutputStream();
                while ((count = zis.read(data, 0, CHUNK_SIZE)) != -1) {
                    dest.write(data, 0, count);
                }
                dest.flush();
                dest.close();
                zis.closeEntry();
                result.putIfAbsent(entry.getName(), ByteString.fromArray(dest.toByteArray()));
            }
        }
        return result;
    }
}

