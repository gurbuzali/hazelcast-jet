package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.pipeline.BatchSource;

public interface FileSourceFactory<T> {

    BatchSource<T> create(FileSourceBuilder<T> builder);
}
