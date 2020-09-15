/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.FunctionEx;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * Base class for implementations of {@link FileFormat}
 *
 * @param <K> type of InputFormat's key
 * @param <V> type of InputFormat's value
 * @param <T> type of item emitted from the source
 */
public abstract class AbstractFileFormat<K, V, T> implements FileFormat<K, V, T> {

    /**
     * InputFormat class option
     *
     * Subclasses must set the this option to the fully qualified name of the InputFormat class
     */
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

    /**
     * Convenience method to be overridden by subclasses
     * @return
     */
    public FunctionEx<InputStream, Stream<T>> mapInputStreamFn() {
        throw new UnsupportedOperationException("FileFormat should override either mapInputStreamFn or localMapFn");
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
