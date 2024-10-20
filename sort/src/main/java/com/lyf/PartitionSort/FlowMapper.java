package com.lyf.PartitionSort;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowMapper extends Mapper<LongWritable,Text,FlowBean,Text>{
    
    private Text outV = new Text();
    private FlowBean outK = new FlowBean();
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        String[] split = line.split(",");
        
        String phone = split[0];
        int length = split.length;
        String upFlow = split[length-3];
        String downFlow = split[length-2];
        outK.setUpFlow(Long.parseLong(upFlow));
        outK.setDownFlow(Long.parseLong(downFlow));
        outK.setSumFlow();

        outV.set(phone);
        context.write(outK,outV);
    }
}
