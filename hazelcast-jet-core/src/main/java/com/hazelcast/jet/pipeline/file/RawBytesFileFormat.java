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
import com.hazelcast.jet.impl.util.IOUtil;

import java.io.InputStream;
import java.util.stream.Stream;

public class RawBytesFileFormat extends AbstractFileFormat<byte[]> {

    public RawBytesFileFormat() {
        withOption(INPUT_FORMAT_CLASS, "com.hazelcast.jet.hadoop.impl.WholeFileInputFormat");
        withOption(PROJECTION_CLASS, "com.hazelcast.jet.hadoop.impl.BytesWritableProjection");
    }

    @Override
    public FunctionEx<InputStream, Stream<byte[]>> mapInputStreamFn() {
        return is -> Stream.of(IOUtil.readFully(is));
    }
}
