package rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class memory_partition {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        //构建spark运行环境
        final JavaSparkContext jsc = new JavaSparkContext(conf);


        //设置分区数量
        final List<String> names = Arrays.asList("zhangsan","lisi","wangwu");
        JavaRDD<String> rdd = jsc.parallelize(names, 3);
        rdd.saveAsTextFile("/home/hadoop/eclipse-workspace/spark-exp/output");
        jsc.close();
    }
}
