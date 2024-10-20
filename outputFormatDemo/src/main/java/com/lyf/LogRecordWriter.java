package com.lyf;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class LogRecordWriter extends RecordWriter<Text,NullWritable>{
    private FSDataOutputStream aim,other;
    public LogRecordWriter(TaskAttemptContext job) throws IOException{
        FileSystem fs = FileSystem.get(job.getConfiguration());
        aim = fs.create(new Path("/home/hadoop/eclipse-workspace/outputFormatDemo/output/aim.log"));
        other = fs.create(new Path("/home/hadoop/eclipse-workspace/outputFormatDemo/output/other.log"));
    }
    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if(log.contains("fei")){
            aim.writeBytes(log+"\n");
        }else{
            other.writeBytes(log+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(aim);
        IOUtils.closeStream(other);
    }
    
}
