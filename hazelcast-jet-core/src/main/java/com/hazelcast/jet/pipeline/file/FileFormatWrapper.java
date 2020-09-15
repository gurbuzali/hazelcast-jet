/*
 * Copyright 2020 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.pipeline.file;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.FunctionEx;

import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * FileFormat wrapper that allows to map item into Object[] (or could be any generic type)
 *
 * Could be used with RowMapper in SQL
 *
 * @param <K>
 * @param <V>
 * @param <T>
 */
public class FileFormatWrapper<K, V, T> implements FileFormat<K, V, Object[]> {

    private final FileFormat<K, V, T> format;
    private final FunctionEx<T, Object[]> toObjectFn;

    public FileFormatWrapper(FileFormat<K, V, T> format, FunctionEx<T, Object[]> toObjectFn) {
        this.format = format;
        this.toObjectFn = toObjectFn;
    }

    @Override
    public FunctionEx<Path, Stream<Object[]>> mapFn() {
        return format.mapFn().andThen(stream -> stream.map(toObjectFn));
    }

    @Override
    public void apply(Object object) {

    }

    @Override
    public BiFunctionEx<K, V, Object[]> projectionFn() {
        return format.projectionFn().andThen(toObjectFn);
    }
}
