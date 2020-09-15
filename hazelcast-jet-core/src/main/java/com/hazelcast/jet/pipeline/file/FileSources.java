package com.hazelcast.jet.pipeline.file;

public class FileSources {

    public static FileSourceBuilder<byte[]> files(String path) {
        return new FileSourceBuilder<>(path)
                .withFormat(new RawBytesFileFormat());
    }

    public static FileSourceBuilder<byte[]> s3(String path) {
        return new FileSourceBuilder<>(path);
    }

    public static FileSourceBuilder<byte[]> hdfs(String path) {
        return new FileSourceBuilder<>(path);
    }

    public static <T> FileSourceBuilder<T> format(FileFormat<Object, Object, T> format) {
        return new FileSourceBuilder<>(format);
    }

}
