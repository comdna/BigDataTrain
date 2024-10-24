/*MR2Reducer.java */
package com.myclass.lab1.mr2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import com.myclass.lab1.mr1.MoneyBean;

public class MR2Reducer extends Reducer<MoneyBean,Text, Text, MoneyBean>{
    public MR2Reducer(){}
    @Override
    public void reduce(MoneyBean key, Iterable<Text> values, Reducer<MoneyBean,Text, Text, MoneyBean>.Context context) throws 
    IOException, InterruptedException 
    {
        /*
        Text id
        key MoneyBean 
        */
        for(Text text:values)
        {
            context.write(text, key);
        }
    }
}
