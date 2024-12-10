package homework;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class AvgGrade {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        //读取文件
        JavaRDD<String> lineRdd = jsc.textFile("/home/hadoop/eclipse-workspace/spark-exp/homeworkInput3");
        JavaPairRDD<String,Double> scoreRdd = lineRdd.mapToPair(
            line ->{
                String[] splits = line.split(" ");
                String name = splits[0];
                double score = Double.parseDouble(splits[1]);
                return new Tuple2<String,Double>(name,score);
            }
        );
        JavaPairRDD<String,Iterable<Double>> scoreListRdd = scoreRdd.groupByKey();;
        scoreListRdd.collect().forEach(System.out::println);
        
        JavaPairRDD<String,Double> avgScoreRdd = scoreListRdd.mapValues(
            iter ->{
                double totalScore = 0;
                int cnt=0;
                Iterator<Double> itor = iter.iterator();
                while(itor.hasNext()){
                    double score = itor.next();
                    totalScore+=score;
                    cnt++;
                }
                return (Double)totalScore/cnt;
            }
        );
        avgScoreRdd.collect().forEach(System.out::println);
        //设置为分区为1
        JavaPairRDD<String,Double> singlePartitionRdd = avgScoreRdd.coalesce(1);
        singlePartitionRdd.saveAsTextFile("/home/hadoop/eclipse-workspace/spark-exp/HKOut3");
        jsc.close();
    }
}
