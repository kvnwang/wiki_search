//package configuration;
//
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.context.annotation.Configuration;
//
//
//@Configuration
//@ComponentScan("com.baeldung.autowire.sample")
//public class Config {
//    @Bean
//    public SparkConf sparkConf() {
//        return new SparkConf()
//                .setAppName("Wiki Config")
//                .setMaster("local[*]");
////                .set("spark.executor.instances", "8")
////                .set("spark.submit.deployMode", "cluster");
//    }
//
//    @Bean
//    public JavaSparkContext javaSparkContext() {
//        return new JavaSparkContext(sparkConf());
//    }
//}