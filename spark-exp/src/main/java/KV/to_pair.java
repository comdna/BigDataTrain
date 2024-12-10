package KV;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class to_pair {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);


        List<Integer> nums = Arrays.asList(1,2,3,4);
        JavaRDD<Integer> rdd = jsc.parallelize(nums);

        rdd.mapToPair(
            num -> new Tuple2<Integer,Integer>(num, num+1)
        ).collect().forEach(System.out::println);

        jsc.close();
    }
}
