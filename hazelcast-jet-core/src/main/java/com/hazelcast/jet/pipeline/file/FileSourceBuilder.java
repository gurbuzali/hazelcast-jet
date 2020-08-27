package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.pipeline.BatchSource;

public class FileSourceBuilder<T> {

    <U> FileSourceBuilder<U> withFormat(FileFormat<U> fileFormat) {
        return (FileSourceBuilder<U>) this;
    }

    public BatchSource<T> build() {
        return null;
    }
}
