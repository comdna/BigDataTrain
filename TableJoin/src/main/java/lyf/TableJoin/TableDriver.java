package lyf.TableJoin;

import java.sql.Driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {
    public TableDriver(){}
    public static void main(String[] args) throws Exception {
        //获取Job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置jar包
        job.setJarByClass(TableDriver.class);
        //关联mapper reducer
        job.setMapperClass(TableMapper.class);
        job.setReducerClass(TableReducer.class);
        //设置mapper输出kv
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TableBean.class);
        //设置最终输出kv
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TableBean.class);
        //设置输入输出路径
        //本地操作
		FileInputFormat.addInputPath(job, new Path("/home/hadoop/eclipse-workspace/TableJoin/input"));
		FileOutputFormat.setOutputPath(job, new Path("/home/hadoop/eclipse-workspace/TableJoin/output"));
		//7.提交job
		System.exit(job.waitForCompletion(true)?0:1);
    }
}
