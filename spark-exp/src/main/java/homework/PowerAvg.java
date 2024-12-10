package homework;

import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PowerAvg {
        public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        //读取文件
        JavaRDD<String> lineRdd = jsc.textFile("/home/hadoop/eclipse-workspace/spark-exp/homeworkInput2");
        JavaPairRDD<String,Tuple2<Double,Double>> scoreRdd = lineRdd.mapToPair(
            line ->{
                String[] splits = line.split(" ");
                String name = splits[0];
                double score = Double.parseDouble(splits[1]);
                double credit = Double.parseDouble(splits[2]);
                return new Tuple2<String,Tuple2<Double,Double>>(name,new Tuple2<>(score,credit));
            }
        );
        /*
        [name, (sc1,cre1),(sc2,cre2),……] 
        */
        JavaPairRDD<String,Iterable<Tuple2<Double,Double>>> scoreListRdd = scoreRdd.groupByKey();
        scoreListRdd.collect().forEach(System.out::println);
        
        JavaPairRDD<String,Double> avgScoreRdd = scoreListRdd.mapValues(
            iter ->{
                double totalScore = 0.0;
                double totalCredit = 0.0;
                Iterator<Tuple2<Double,Double>> itor = iter.iterator();
                while(itor.hasNext()){
                    Tuple2<Double,Double> next = itor.next();
                    totalScore+=next._1()*next._2();
                    totalCredit+=next._2();
                }
                return (Double)totalScore/totalCredit;
            }
        );
        avgScoreRdd.collect().forEach(System.out::println);
        //设置为分区为1
        JavaPairRDD<String,Double> singlePartitionRdd = avgScoreRdd.coalesce(1);
        singlePartitionRdd.saveAsTextFile("/home/hadoop/eclipse-workspace/spark-exp/HKOut2");
        jsc.close();
    }
}
