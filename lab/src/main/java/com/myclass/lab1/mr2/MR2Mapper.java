/*MR2Mapper.java */
package com.myclass.lab1.mr2;

import com.myclass.lab1.mr1.MoneyBean;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MR2Mapper extends Mapper<LongWritable,Text,MoneyBean,Text>{
    private Text outV = new Text();
    private MoneyBean outK = new MoneyBean();
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
    {
        String line = value.toString();
        String[] split = line.split("\t");
        
        String id = split[0];
        String income = split[1];
        String outcome = split[2];
        outK.setIncome(Double.parseDouble(income));
        outK.setOutcome(Double.parseDouble(outcome));
        outK.setRestcome();

        outV.set(id);
        context.write(outK,outV);
    }
}
