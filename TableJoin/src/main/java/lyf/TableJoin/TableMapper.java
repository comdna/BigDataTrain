package lyf.TableJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean>{

    private Text outK = new Text("");
    private TableBean outV = new TableBean();
    private String filename;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, TableBean>.Context context)throws IOException, InterruptedException 
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        filename = fileSplit.getPath().getName();
    }
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, TableBean>.Context context)throws IOException, InterruptedException
    {
        String line = value.toString();
        if(filename.contains("order"))
        {
            String[] split = line.split(",");
            outV.setId(split[0]);
            outV.setPid(split[1]);
            outV.setAmount(Integer.parseInt(split[2]));
            outV.setPname("");
            outV.setFlag("order");
            outK.set(split[1]);
        }
        else if(filename.contains("pd")){
            String[] split = line.split(",");
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);
            outV.setFlag("pd");
            outK.set(split[0]);
        }
        context.write(outK, outV);
    }
}
