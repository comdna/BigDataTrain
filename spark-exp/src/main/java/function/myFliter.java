package function;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class myFliter {
    public static void main(String[] args) {
        //创建spark配置对象
        final SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("spark");
        final JavaSparkContext jsc = new JavaSparkContext(conf);

        //
        List<String> names = Arrays.asList("Hadoop","Hive","Flink","Spark");
        JavaRDD<String> rdd = jsc.parallelize(names);
        Search search = new Search("H");
        search.match(rdd);

        jsc.close();
    }
}

class Search implements Serializable{
    private String query;
    public Search(String query){
        this.query=query;
    }
    public void match(JavaRDD<String> rdd){
        rdd.filter(
            s->s.startsWith(query)
        ).collect().forEach(System.out::println);
    }
};
