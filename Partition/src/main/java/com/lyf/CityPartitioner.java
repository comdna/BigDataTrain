package com.lyf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


//extends Partitioner<k2,v2>
//Override public int getPartition(k2,v2,int numPartitions);
public class CityPartitioner extends Partitioner<Text,FlowBean>{
    @Override
    public int getPartition(Text text,FlowBean flowBean,int numPartitions)
    {
        String phone = text.toString();
        String head = phone.substring(0,3);
        // if(head=="181") return 0;
        // else if(head=="182") return 1;
        // else if(head=="183") return 2;
        if("181".equals(head)) return 0;
        else if("182".equals(head)) return 1;
        else if("183".equals(head)) return 2;
        return 3;
    }
}
