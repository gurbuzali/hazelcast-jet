package com.hazelcast.jet.hadoop.impl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CsvInputFormat extends FileInputFormat<NullWritable, Object> {

    @Override
    public RecordReader<NullWritable, Object> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        return new CsvRecordReader();
    }
}
