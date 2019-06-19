package sample.stream_shared_state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Read ordered by lastModified
 * New files are loaded at the end
 * We can not use the alpakka-file connector here, because an unbounded stream does not support ordering
 */
public class FileLister {
	private static final Logger LOGGER = LoggerFactory.getLogger(FileLister.class);

	public List<Path> run(File directory) {
		AtomicInteger filesCounter = new AtomicInteger(0);
		List<Path> resultList = null;

		try {
			resultList = Files.walk(directory.toPath(), 2)

					.filter(Files::isRegularFile)
					.filter(path -> path.toString().endsWith(".zip"))
					.sorted(Comparator.comparing(xdmFile -> {
						try {
							return Files.getLastModifiedTime(xdmFile);

						} catch (IOException e) {
							LOGGER.warn("Cannot access last modified time of '{}'. It will be assigned as the " +
									"most recent file to minimize the risk that it will be evicted earlier than should.", xdmFile, e);
							return FileTime.fromMillis(System.currentTimeMillis());
						}
					}))
					.map(each -> {
						filesCounter.incrementAndGet();
						return each;
					})
					.collect(Collectors.toList());

			LOGGER.info("Loaded {} file paths from directory {}", filesCounter.get(), directory);
		} catch (IOException e) {
			LOGGER.error("Error while loading file paths from directory {}. Please check file system permissions", directory, e);
		}
		return resultList;
	}
}
