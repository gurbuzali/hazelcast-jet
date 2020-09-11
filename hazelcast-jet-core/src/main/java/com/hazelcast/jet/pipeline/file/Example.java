package com.hazelcast.jet.pipeline.file;

import com.hazelcast.jet.pipeline.BatchSource;

import java.nio.charset.StandardCharsets;

public class Example {

    public static void main(String[] args) {


        BatchSource<byte[]> sourceBytes = FileSources.files("my/path/on/filesystem/*")
                                                     .build();

        BatchSource<byte[]> sourceBytesS3 = FileSources.s3("s3a://my-bucket/my/path/*")
                                                     .build();

        BatchSource<String> sourceText = FileSources.s3("s3a://my-bucket/my/path/*")
                                                    .withFormat(new TextFileFormat())
                                                    .build();

        BatchSource<String> sourceLines = FileSources.s3("s3a://my-bucket/my/path/*")
                                                     .withFormat(new LinesTextFileFormat("UTF-8"))
                                                     .build();

        /*BatchSource<String> avro = FileSources.files("s3a://my-bucket/my/path/*")
                                                     .withFormat(new AvroFileFormat<?>())
                                                     .build();
*/
    }
}
