package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.pipeline.BatchSource;

import java.util.HashMap;
import java.util.Map;

public class FileSourceBuilder<T> {

    private final Map<String, String> options = new HashMap<>();

    private String path;
    private FileFormat<?, ?, T> format;
    private boolean useHadoop;

    public FileSourceBuilder(FileFormat<?, ?, T> format) {
        this.format = format;
    }

    public FileSourceBuilder(String path) {
        this.path = path;
    }

    public FileSourceBuilder<T> withOption(String key, String value) {
        options.put(key, value);
        return this;
    }

    public FileSourceBuilder<T> withPath(String path) {
        this.path = path;
        return this;
    }

    public FileSourceBuilder<T> useHadoopForLocalFiles() {
        useHadoop = true;
        return this;
    }

    public <U> FileSourceBuilder<U> withFormat(FileFormat<?, ?, U> fileFormat) {
        format = (FileFormat<?, ?, T>) fileFormat;
        return (FileSourceBuilder<U>) this;
    }

    public Map<String, String> options() {
        return options;
    }

    public String path() {
        return path;
    }

    public FileFormat<?, ?, T> format() {
        return format;
    }

    public BatchSource<T> build() {
        if (useHadoop || path.startsWith("s3a://") ||
                path.startsWith("hdfs://")) {

            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            try {
                Class<FileSourceFactory<T>> sourceFactoryClass = (Class<FileSourceFactory<T>>)
                        classLoader.loadClass("com.hazelcast.jet.hadoop.HadoopSourceFactory");

                FileSourceFactory<T> sourceFactory = sourceFactoryClass.newInstance();
                return sourceFactory.create(this);
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return null;
        }
    }
}
