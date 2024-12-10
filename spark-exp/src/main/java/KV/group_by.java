package KV;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class group_by {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);


        List<Integer> nums = Arrays.asList(1,2,3,4);
        JavaRDD<Integer> rdd = jsc.parallelize(nums);

        JavaPairRDD<Integer, Iterable<Integer>>  rdd2=  rdd.groupBy(
            num ->num%2
        );
        rdd2.collect().forEach(System.out::println);

        rdd2.mapValues(
            iter -> {
                int sum=0;
                Iterator<Integer> iterator= iter.iterator();
                while(iterator.hasNext()){
                    Integer num = iterator.next();
                    sum+=num;
                }
                return sum;
            }
        ).collect().forEach(System.out::println);;
        jsc.close();
    }
}
