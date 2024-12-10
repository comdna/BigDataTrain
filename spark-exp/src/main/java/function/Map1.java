package function;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class Map1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = jsc.parallelize(
            Arrays.asList(1,2,3,4),2
        );

        JavaRDD<Object> rdd2 = rdd.map(new Function<Integer,Object>(){
            @Override
            public Object call(Integer in){
                return in*2;
            }
        });
        List<Object> collect = rdd2.collect();
        collect.forEach(System.out::println);
        jsc.close();
    }
}
