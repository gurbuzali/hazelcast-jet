package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Sources;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

import static java.util.Objects.requireNonNull;

/**
 * Builder for file sources
 * <p>
 * The builder works with local filesystem and with hadoop supported filesystems.
 * <p>
 * The builder requires 'path' and 'format' parameters and creates a {@link BatchSource}. The path specifies the
 * location of the file(s) and possibly the data source - s3a://, hdfs://, etc..
 *
 * The format determines how the contents of the file is parsed and also determines the type of the source items.
 * E.g. the {@link LineTextFileFormat} returns each line as a String, {@link JsonFileFormat} returns
 * each line of a JSON Lines file deserialized into an instance of a specified class.
 * <p>
 * You may also use Hadoop to read local files by specifying the {@link #useHadoopForLocalFiles()} flag. This might
 * <p>
 * Usage:
 * <pre>{@code
 * BatchSource<User> source = new FileSourceBuilder("data/users.jsonl")
 *   .withFormat(new JsonFileFormat<>(User.class))
 *   .build();
 * }</pre>
 *
 * @param <T>
 */
public class FileSourceBuilder<T> {

    private final Map<String, String> options = new HashMap<>();

    private String path;
    private FileFormat<?, ?, T> format;
    private boolean useHadoop;

    // TODO We should have only single constructor and withFormat(..)
    // Our current filesystem takes path as constructor parameter
    // It is also what makes sense to me (Frantisek) - first define location then other options - especially format
    // But Spark defines format first and then calls load from a location
    // Hadoop also has path as a parameter of FileInputFormat

    /**
     * Create a new builder with given path.
     *
     * Path can point to a file, a directory or contain a glob (e.g. 'file*' capturing file1, file2, ...).
     *
     * @param path path
     */
    public FileSourceBuilder(String path) {
        this.path = requireNonNull(path, "path must not be null");
    }

    /**
     * Create a new builder with given file format.
     *
     * TODO likely to remove (see discussion above)
     */
    public FileSourceBuilder(FileFormat<?, ?, T> format) {
        this.format = requireNonNull(format, "format must not be null");
    }

    /**
     * TODO likely to remove (see discussion above)
     */
    public FileSourceBuilder<T> withPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * Set the file format for the source
     *
     * Currently supported file formats are:
     * <li> {@link AvroFileFormat}
     * <li> {@link CsvFileFormat}
     * <li> {@link JsonFileFormat}
     * <li> {@link LineTextFileFormat}
     * <li> {@link ParquetFileFormat}
     * <li> {@link RawBytesFileFormat}
     * <li> {@link TextFileFormat}
     *
     * You may provide a custom format by implementing the {@link FileFormat} interface. See its javadoc for details.
     */
    public <U> FileSourceBuilder<U> withFormat(FileFormat<?, ?, U> fileFormat) {
        format = (FileFormat<?, ?, T>) fileFormat;
        return (FileSourceBuilder<U>) this;
    }

    /**
     * Use hadoop for files from local filesystem
     * <p>
     * Using Hadoop may be advantageous when working with small number of large files, because of better parallelization.
     */
    public FileSourceBuilder<T> useHadoopForLocalFiles() {
        useHadoop = true;
        return this;
    }

    /**
     * Specify an option for the underlying source
     *
     * NOTE: Format related options are set on the FileFormat directly.
     */
    public FileSourceBuilder<T> withOption(String key, String value) {
        requireNonNull(key, "key must not be null");
        requireNonNull(value, "value must not be null");
        options.put(key, value);
        return this;
    }

    /**
     * The configured source options
     */
    public Map<String, String> options() {
        return options;
    }

    /**
     * The source path
     */
    public String path() {
        return path;
    }

    /**
     * The source format
     */
    public FileFormat<?, ?, T> format() {
        return format;
    }

    /**
     * Build a batch source based on the configuration
     */
    public BatchSource<T> build() {
        if (path == null) {
            throw new IllegalStateException("Parameter 'path' is required");
        }
        if (format == null) {
            throw new IllegalStateException("Parameter 'format' is required");
        }

        if (useHadoop || path.startsWith("s3a://") ||
                path.startsWith("hdfs://")) {
            // TODO add others which we can get working reliably

            ServiceLoader<FileSourceFactory> loader = ServiceLoader.load(FileSourceFactory.class);
            for (FileSourceFactory<T> fileSourceFactory : loader) {
                return fileSourceFactory.create(this);
            }

            throw new JetException("No suitable FileSourceFactory found. " +
                    "Do you have Jet's Hadoop module on classpath?");
        } else {
            Path p = Paths.get(path);

            String directory;
            String glob = "*";
            if (p.toFile().isDirectory()) {
                directory = p.toString();
            } else {
                directory = p.getParent().toString();
                glob = p.getFileName().toString();
            }

            return Sources.filesBuilder(directory)
                          .glob(glob)
                          .build(format.mapFn());
        }
    }
}
