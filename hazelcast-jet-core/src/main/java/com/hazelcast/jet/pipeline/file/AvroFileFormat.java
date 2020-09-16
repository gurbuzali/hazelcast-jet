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
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.hazelcast.jet.impl.util.Util.uncheckRun;

/**
 * FileFormat for avro files
 *
 * @param <T>
 */
public class AvroFileFormat<T> extends AbstractFileFormat<T> {

    // Taken from org.apache.avro.mapreduce.AvroJob.CONF_INPUT_KEY_SCHEMA, which is private
    private static final String CONF_INPUT_KEY_SCHEMA = "avro.schema.input.key";

    private Class<T> reflectClass;

    public AvroFileFormat() {
        withOption(INPUT_FORMAT_CLASS, "org.apache.avro.mapreduce.AvroKeyInputFormat");
        withOption(PROJECTION_CLASS, "com.hazelcast.jet.hadoop.impl.AvroKeyProjection");
    }

    @Override
    public FunctionEx<Path, Stream<T>> localMapFn() {
        Class<T> thisReflectClass = reflectClass;
        return (path) -> {
            DatumReader<T> datumReader = datumReader(thisReflectClass);
            DataFileReader<T> reader = new DataFileReader<>(path.toFile(), datumReader);
            return StreamSupport.stream(reader.spliterator(), false)
                    .onClose(() -> uncheckRun(reader::close));
        };
    }

    public AvroFileFormat<T> withReflect(Class<T> reflectClass) {
        this.reflectClass = reflectClass;
        Schema schema = ReflectData.get().getSchema(reflectClass);
        withOption(CONF_INPUT_KEY_SCHEMA, schema.toString());
        return this;
    }

    private static <T> DatumReader<T> datumReader(Class<T> reflectClass) {
        return reflectClass == null ? new SpecificDatumReader<>() : new ReflectDatumReader<>(reflectClass);
    }
}
