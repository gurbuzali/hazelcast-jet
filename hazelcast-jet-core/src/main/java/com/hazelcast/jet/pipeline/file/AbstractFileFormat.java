package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.OffsetTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public abstract class AbstractFileFormat<K, V, T> implements FileFormat<K, V, T> {

    public static final String INPUT_FORMAT_CLASS = "job.inputformat.class";
    private final Map<String, String> options = new HashMap<>();

    @Override
    public FunctionEx<Path, Stream<T>> localMapFn() {
        FunctionEx<InputStream, Stream<T>> mapInputStreamFn = mapInputStreamFn();
        return path -> {
            FileInputStream stream = new FileInputStream(path.toFile());

            return mapInputStreamFn.apply(stream).onClose(() -> uncheckRun(stream::close));
        };
    }

    public FunctionEx<InputStream, Stream<T>> mapInputStreamFn() {
        throw new UnsupportedOperationException("FileFormat should override either mapInputStreamFn or mapFn");
    }

    @Override public BiFunctionEx<K, V, T> projectionFn() {
        return null;
    }

    @Override
    public Map<String, String> options() {
        return options;
    }

    protected AbstractFileFormat<K, V, T> withOption(String key, String value) {
        options.put(key, value);
        return this;
    }
}
