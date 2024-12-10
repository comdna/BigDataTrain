package KV;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class kv1 {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        //构建spark运行环境
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> tuple2s = Arrays.asList(
            new Tuple2<String,Integer>("a",1),
            new Tuple2<String,Integer>("b",2),
            new Tuple2<String,Integer>("c",3)
        );

        JavaPairRDD<String,Integer> rdd = jsc.parallelizePairs(tuple2s);
        //只对V进行map
        rdd.mapValues(
            num->{
                return num*2;
            }
        )
         .collect()
         .forEach(System.out::println);

        jsc.close();
    }
}
