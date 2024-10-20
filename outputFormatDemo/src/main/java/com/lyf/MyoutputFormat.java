package com.lyf;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

public class MyoutputFormat extends FileOutputFormat<Text,NullWritable>{

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
        throws IOException, InterruptedException
    {
            LogRecordWriter lrw = new LogRecordWriter(job);
            return lrw;
    }
}
