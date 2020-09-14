package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public abstract class AbstractFileFormat<K, V, T> implements FileFormat<K, V, T> {

    @Override
    public FunctionEx<Path, Stream<T>> mapFn() {
        FunctionEx<InputStream, Stream<T>> mapInputStreamFn = mapInputStreamFn();
        return path -> {
            FileInputStream stream = new FileInputStream(path.toFile());

            return mapInputStreamFn.apply(stream).onClose(() -> uncheckRun(stream::close));
        };
    }

    public FunctionEx<InputStream, Stream<T>> mapInputStreamFn() {
        throw new UnsupportedOperationException("FileFormat should override either mapInputStreamFn or mapFn");
    }

    @Override public void apply(Object object) {

    }

    @Override public BiFunctionEx<K, V, T> projectionFn() {
        return null;
    }
}
