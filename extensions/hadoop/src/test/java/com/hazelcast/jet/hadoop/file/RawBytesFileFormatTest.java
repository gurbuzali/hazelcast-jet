package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RawBytesFileFormatTest extends BaseFileFormatTest {

    @Test
    public void testRawFile() {
        FileSourceBuilder<byte[]> source = FileSources.files("src/test/resources/raw.bin");

        byte[] expectedBytes = "Raw contents of the file.".getBytes(UTF_8);
        assertItemsInSource(source, expectedBytes);
    }
}
