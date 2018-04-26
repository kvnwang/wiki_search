//package configuration;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//
//@Configuration
//public class Config {
//    @Bean
//    public SparkConf sparkConf() {
//        return new SparkConf()
//                .setAppName("cs132g4searcher")
//                .setMaster(System.getenv("MASTER"))
//                .set("spark.executor.instances", "8")
//                .set("spark.submit.deployMode", "cluster");
//    }
//
//    @Bean
//    public JavaSparkContext javaSparkContext() {
//        return new JavaSparkContext(sparkConf());
//    }
//}