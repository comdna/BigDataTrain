/*MR1Reducer.java */
package com.myclass.lab1.mr1;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MR1Reducer extends Reducer<Text,MoneyBean, Text, MoneyBean>{
    public MR1Reducer(){}
    private MoneyBean outV=new MoneyBean();
    @Override
    public void reduce(Text key, Iterable<MoneyBean> values, Reducer<Text,MoneyBean, Text, MoneyBean>.Context context) throws 
    IOException, InterruptedException 
    {
        double incomeSum=0.0,outcomeSum=0.0;
        for(MoneyBean moneyBean:values)
        {
            incomeSum+=moneyBean.getIncome();
            outcomeSum+=moneyBean.getOutcome();
        }
        outV.setIncome(incomeSum);
        outV.setOutcome(outcomeSum);
        context.write(key, outV);
    }
}
