package rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class disk_rdd {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        //构建spark运行环境
        final JavaSparkContext jsc = new JavaSparkContext(conf);
        /*
            对接磁盘数据(文件)，构建RDD对象
        */
        //相互转换
        JavaRDD<String> rdd = jsc.textFile("/home/hadoop/eclipse-workspace/spark-exp/data");
        List<String> collect = rdd.collect();
        collect.forEach(System.out::println);
        jsc.close();
    }
}
