package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.file.TextFileFormat;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TextFileTest extends JetTestSupport {

    @Test
    public void readTextFile() {

        BatchSource<String> source = FileSources.files("src/test/resources/file.txt")
                                                .useHadoopForLocalFiles()
                                                .withFormat(new TextFileFormat())
                                                .build();

        Pipeline p = Pipeline.create();

        p.readFrom(source)
         .apply(Assertions.assertCollected(collected -> {
             assertThat(collected).containsOnly("Text contents of\nthe file.\n");
         }));


        JetInstance jet = createJetMember();
        jet.newJob(p).join();
    }
}
