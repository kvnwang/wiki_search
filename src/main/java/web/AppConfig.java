package web;


import org.apache.spark.SparkConf; 
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
@Configuration
public class AppConfig {

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("searcher")
                .setMaster("local")
                .set("spark.executor.instances", "8");

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

}