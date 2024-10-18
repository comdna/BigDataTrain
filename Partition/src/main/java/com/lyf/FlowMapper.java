package com.lyf;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean>{
    
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        String[] split = line.split(",");
        
        String phone = split[1];
        int length = split.length;
        String upFlow = split[length-3];
        String downFlow = split[length-2];
        
        System.out.println(phone+" "+upFlow+" "+downFlow);
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(upFlow));
        outV.setDownFlow(Long.parseLong(downFlow));
        outV.setSumFlow();
        context.write(outK, outV);
    }
}
