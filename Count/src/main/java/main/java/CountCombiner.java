package main.java;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountCombiner extends Reducer<Text,IntWritable,Text,IntWritable>{
    private IntWritable outV = new IntWritable(0);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException 
    {
        int vsum=0;
        for(IntWritable v:values)
        {
            vsum+=v.get();
        }
        outV.set(vsum);
        context.write(key, outV);
    }
}
