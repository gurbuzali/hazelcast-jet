package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.file.FileSourceBuilder;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.LinesTextFileFormat;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import org.junit.Test;

public class TextFileTest extends BaseFileTest {

    @Test
    public void readTextFileAsSingleItem() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/file.txt")
                                                      .withFormat(new TextFileFormat());

        assertItemsInSource(source, "Text contents of\nthe file.\n");
    }

    @Test
    public void readTextFileAsLines() {
        FileSourceBuilder<String> source = FileSources.files("src/test/resources/file.txt")
                                                      .withFormat(new LinesTextFileFormat());

        assertItemsInSource(source, "Text contents of", "the file.");
    }
}
