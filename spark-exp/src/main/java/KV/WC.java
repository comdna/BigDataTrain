package KV;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WC {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        //读取文件
        JavaRDD<String> lineRdd = jsc.textFile("/home/hadoop/eclipse-workspace/spark-exp/data/words.txt");
        //切分
        JavaRDD<String> wordRdd = lineRdd.flatMap(line->Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<String, Iterable<String>> WCRdd = wordRdd.groupBy(word->word);
        JavaPairRDD<String, Integer> WCRdd2=WCRdd.mapValues(
            iter ->{
                int sum=0;
                for(String s:iter){
                    sum++;
                }
                return sum;
            }
        );
        WCRdd2.collect().forEach(System.out::println);
        jsc.close();
    }
}
