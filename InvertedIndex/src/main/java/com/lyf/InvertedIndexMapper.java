package com.lyf;


import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    //private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text file = new Text();
    //private IntWritable num = new IntWritable();
    public InvertedIndexMapper() {}
    private String name=null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取文件名称
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        name = inputSplit.getPath().getName();
    }
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, 
    InterruptedException 
    {
        String line = value.toString();
        String[] strArray = line.split("[^a-zA-Z]+");
        for(String str:strArray)
        {
            word.set(str);
            file.set(name);
            System.out.println("Mapper "+word.toString()+" "+name.toString());
            context.write(word,file);
        }
    }
}
