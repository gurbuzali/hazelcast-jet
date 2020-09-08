package com.hazelcast.jet.hadoop.file;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.JetTestSupport;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.file.FileSources;
import com.hazelcast.jet.pipeline.test.Assertions;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class RawFileTest extends JetTestSupport {

    @Test
    public void testRawFile() {
        BatchSource<byte[]> source = FileSources.files("src/test/resources/raw.bin")
                                                .useHadoopForLocalFiles()
                                                .build();

        Pipeline p = Pipeline.create();

        p.readFrom(source)
         .apply(Assertions.assertCollected(collected -> {
             assertThat(collected).containsOnly("Raw contents of the file.".getBytes(UTF_8));
         }));


        JetInstance jet = createJetMember();
        jet.newJob(p).join();
    }
}
