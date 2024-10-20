package main.java;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class CountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public CountMapper() {}
    public void Mapper(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, 
    InterruptedException 
    {
        // StringTokenizer itr = new StringTokenizer(value.toString()); 
        // while(itr.hasMoreTokens()) {
        //     this.word.set(itr.nextToken());
        //     context.write(this.word, one);
        // }
        String line = value.toString();
        String[] strArray = line.split(" ");
        for(String str:strArray)
        {
            word.set(str);
            /*
            context.write(k,v)的k,v就是Mapper的输出,Reducer的输入
            */
            context.write(word,one);
        }
    }
}