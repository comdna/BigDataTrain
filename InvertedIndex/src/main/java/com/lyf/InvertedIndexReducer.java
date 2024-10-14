package com.lyf;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.HashMap;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    public InvertedIndexReducer() {}
    private Text result = new Text("");
    private static final IntWritable one = new IntWritable(1);
    private HashMap<String,IntWritable> hashMap = new HashMap<>();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        hashMap.clear();
        result.set("");
    }
    @Override
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws 
    IOException, InterruptedException 
    {
        /*
            key:word
            values:file list 
        */
        //TreeMap<String, IntWritable> hashMap = new TreeMap<>();
        
        hashMap.clear();
        for (Text file : values) 
        {
            String file_=file.toString();
            IntWritable v = hashMap.get(file_);
            if (v == null) {
                v = new IntWritable(1);
                //v.set(1);
                hashMap.put(file_,v);
            } else {
                v.set(v.get() + 1);
                hashMap.put(file_, v);
            }
            System.out.println("Reducer: "+key.toString()+" "+file_+" "+hashMap.get(file_).toString());
        }

        String str=new String("");
        for(String file:hashMap.keySet())
        {
            //System.out.println(key.toString()+" "+file.toString()+"\n");
            IntWritable v = hashMap.get(file);
            //System.out.println(key.toString()+" "+file.toString()+v.toString()+"\n");
            String add = file+"->"+v.toString()+";";
            str+=add;
        }
        result.set(str);
        context.write(key,result);
    }
}
