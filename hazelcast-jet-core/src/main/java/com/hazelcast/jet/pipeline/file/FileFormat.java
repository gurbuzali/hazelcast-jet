package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.FunctionEx;

import java.io.InputStream;
import java.util.stream.Stream;

public interface FileFormat<T> {

    FunctionEx<? super InputStream, Stream<T>> mapFn();
}
