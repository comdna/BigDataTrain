package com.lyf;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    public FlowReducer() {}
    private FlowBean flow = new FlowBean();
    @Override
    public void reduce(Text key, Iterable<FlowBean> values, Reducer<Text, FlowBean, Text, FlowBean>.Context context) throws 
    IOException, InterruptedException 
    {
        long upFlow = 0;
        long downFlow = 0;
        for(FlowBean flowBean:values)
        {
            upFlow+=flowBean.getUpFlow();
            downFlow+=flowBean.getDownFlow();
        }
        flow.setUpFlow(upFlow);
        flow.setDownFlow(downFlow);
        flow.setSumFlow();
        context.write(key, flow);
    }
}
