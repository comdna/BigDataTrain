/*MR1Mapper.java */
package com.myclass.lab1.mr1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MR1Mapper extends Mapper<LongWritable,Text,Text,MoneyBean>{
    private Text outK = new Text();
    private MoneyBean outV = new MoneyBean();
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        String[] split = line.split(",");
        
        String id = split[0];
        String income = split[1];
        String outcome = split[2];
        outV.setIncome(Double.parseDouble(income));
        outV.setOutcome(Double.parseDouble(outcome));

        outK.set(id);
        context.write(outK,outV);
    }
}
