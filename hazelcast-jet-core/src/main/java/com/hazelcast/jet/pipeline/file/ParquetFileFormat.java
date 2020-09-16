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
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.nio.file.Path;
import java.util.stream.Stream;

public class ParquetFileFormat<T> extends AbstractFileFormat<T> {

    public ParquetFileFormat() {
        withOption(INPUT_FORMAT_CLASS, AvroParquetInputFormat.class.getCanonicalName());
    }

    @Override
    public FunctionEx<Path, Stream<T>> localMapFn() {
        throw new UnsupportedOperationException("Reading Parquet files is not supported in local filesystem mode. " +
                "Use Jet Hadoop module with FileSourceBuilder.useHadoopForLocalFiles option instead.");
    }
}
