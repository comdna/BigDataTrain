/*MoneyBean.java */
package com.myclass.lab1.mr1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class MoneyBean implements WritableComparable<MoneyBean>{
    private double income;
    private double outcome;
    private double restcome;
    public MoneyBean(){}
    public double getIncome() {
        return income;
    }
    public void setIncome(double income) {
        this.income = income;
    }
    public double getOutcome() {
        return outcome;
    }
    public void setOutcome(double outcome) {
        this.outcome = outcome;
    }
    public double getRestcome() {
        return restcome;
    }
    public void setRestcome() {
        this.restcome = this.income-this.outcome;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        //注意数据类型
        out.writeDouble(income);
        out.writeDouble(outcome);
        out.writeDouble(restcome);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        //注意和write的顺序保持对应一致
        this.income=in.readDouble();
        this.outcome=in.readDouble();
        this.restcome=in.readDouble();
    }

    @Override
    public String toString() {
        //return "消费:"+outcome+"\t"+"收入:"+income+"\t"+"结余:"+restcome;
        return income+"\t"+outcome+"\t"+restcome;
    }
    @Override
    public int compareTo(MoneyBean o) {
        if(this.income<o.income) return -1;
        else if(this.income>o.income) return 1;
        else if(this.outcome>o.outcome) return -1;
        else if(this.outcome<o.outcome) return 1;
        return 0;
    }
}
