package query;


import java.beans.Encoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.eclipse.jetty.websocket.common.frames.DataFrame;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import scala.Tuple2;




public class Searcher {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Scanner input=new Scanner(System.in);		
		System.out.println("Enter a term to search on");
		String word=input.nextLine().trim();
		System.out.println("Enter a term to also search on");
		String filter=input.nextLine().trim();
		System.out.println("Enter a term to not search and filter on");

		String word2=input.nextLine().trim();
		analyze(word, filter, word2);
           
	}
	private static SparkSession setUpSpark() {
		SparkConf conf = new SparkConf()
				.setAppName("Simple Application")
				.setMaster("local[*]");
		org.apache.spark.sql.Encoder<Article> encoder = Encoders.bean(Article.class);

		SparkSession spark = SparkSession
			      .builder()
			      .appName("Java Spark SQL basic example")
			      .config(conf)
			      .getOrCreate();
		return spark;
	}
	
	private static String getFileNumber(String s) {
		  String value=""+s.charAt(0)+s.charAt(1);
		  String name="output/part-r-00";
		  int hash= value.hashCode() % 676;
		  if(hash<10) {
			  name+=("00"+hash);
		  } else if(hash<100) {
			  name+=("0"+hash);
		  } else {
			  name+=(""+hash);
		  }
		  return name;
	}
	

	private static void analyze(String word, String include, String exclude) {
		SparkSession spark=setUpSpark();

		// reads the data
		 Dataset<Row> ds = spark.read().json("output/part-r-00022").cache();
		 SQLContext sqlContext = new org.apache.spark.sql.SQLContext(spark);
		 Dataset<Row> expanded = sqlContext.read().json(getFileNumber(word), getFileNumber(include), getFileNumber(exclude) );
		 
		 
		 
//		 data logic
		 Dataset<Row> filtered= expanded.select("id", "pos", "url", "word")
				 .filter(expanded.col("word").isin(word, include))
				 .filter(expanded.col("word").notEqual(exclude));
//	 	 
		 filtered.show();
		 JavaRDD<Row> s=filtered.toJavaRDD();
		 
		 JavaRDD<Article> articles=s.map((line) -> {
			 String id= line.getString(line.fieldIndex("id"));
			 String pos= line.getString(line.fieldIndex("pos"));
			 String url= line.getString(line.fieldIndex("url"));
			 String words= line.getString(line.fieldIndex("word"));
			 return  new Article(id, pos, url, words);
		 });
		 
		 List<Article> result = articles.collect();

		 System.out.println();
		 
//		 Dataset<Row> expanded = d.withColumn("id", org.apache.spark.sql.functions.explode(d.col("ids")))
//				 				.withColumn("pos", org.apache.spark.sql.functions.explode(d.col("positions")))
//				 				.withColumn("url", org.apache.spark.sql.functions.explode(d.col("urls")))
//				 				.withColumn("word", org.apache.spark.sql.functions.explode(d.col("words"))).cache();
//		 				
//		 

	}
	
	
//	 Dataset<Row> expanded = d.withColumn("id", org.apache.spark.sql.functions.explode(d.col("ids")))
//		.withColumn("pos", org.apache.spark.sql.functions.explode(d.col("positions")))
//		.withColumn("url", org.apache.spark.sql.functions.explode(d.col("urls")))
//		.withColumn("word", org.apache.spark.sql.functions.explode(d.col("words"))).cache();
//
//
//Dataset<Row> filtered= expanded.select("id", "pos", "url", "word").filter(expanded.col("word").equalTo("jtag"));
//
//List<String> list = Arrays.asList("id", "pos", "url", "word");
//JavaRDD<Row> s=filtered.toJavaRDD();
//
//JavaRDD<Article> articles=s.map((line) -> {
//String id= line.getString(line.fieldIndex("id"));
//String pos= line.getString(line.fieldIndex("pos"));
//String url= line.getString(line.fieldIndex("url"));
//String words= line.getString(line.fieldIndex("word"));
//return  new Article(id, pos, url, words);
//});
//
//List<Article> result = articles.collect();
//System.out.println(result);

}