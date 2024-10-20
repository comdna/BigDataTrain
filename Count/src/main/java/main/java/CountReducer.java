package main.java;
//package,src下的目录

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();
    public CountReducer() {}
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws 
    IOException, InterruptedException {
        // int sum = 0;
        // IntWritable val;
        // for(Iterator i$ = values.iterator(); i$.hasNext(); sum += val.get()) {
        //     val = (IntWritable)i$.next();
        // }
        // this.result.set(sum);
        // context.write(key, this.result);
        int sum=0;
        for(IntWritable val:values)
        {
            sum+=val.get();
        }
        result.set(sum);
        context.write(key,result);
    }
}
