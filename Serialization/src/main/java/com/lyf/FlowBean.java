package com.lyf;


/*
    实现writable接口
    重写序列化和反序列化方法
    重写无参ctor
    重写toString() 
*/
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean()
    {

    }

    //getter and setter
    public long getUpFlow() {
        return upFlow;
    }
    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }
    public long getDownFlow() {
        return downFlow;
    }
    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }
    public long getSumFlow() {
        return sumFlow;
    }
    public void setSumFlow() {
        this.sumFlow = this.downFlow + this.upFlow;
    }


    @Override
    public void write(DataOutput out) throws IOException
    {
        //注意数据类型
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        //注意和write的顺序保持对应一致
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }
}
