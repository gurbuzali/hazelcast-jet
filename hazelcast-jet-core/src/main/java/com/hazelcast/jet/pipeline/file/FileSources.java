package com.hazelcast.jet.pipeline.file;

public class FileSources {

    public static FileSourceBuilder<byte[]> files(String path) {
        return new FileSourceBuilder();
    }

    public static FileSourceBuilder<byte[]> s3(String path) {
        return new FileSourceBuilder();
    }

    public static FileSourceBuilder<byte[]> hdfs(String path) {
        return new FileSourceBuilder();
    }

    /*public static FileSourceBuilder<byte[]> hdfs(String path) {
        return new FileSourceBuilder();
    }
*/


}
