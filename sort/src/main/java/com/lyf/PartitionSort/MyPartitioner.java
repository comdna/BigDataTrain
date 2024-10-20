package com.lyf.PartitionSort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


//extends Partitioner<k2,v2>
//Override public int getPartition(k2,v2,int numPartitions);
public class MyPartitioner extends Partitioner<FlowBean,Text>{
    @Override
    public int getPartition(FlowBean flowBean,Text text,int numPartitions)
    {
        String phone = text.toString();
        String head = phone.substring(0,3);
        if("181".equals(head)) return 0;
        else if("182".equals(head)) return 1;
        else if("183".equals(head)) return 2;
        return 3;
    }
}
