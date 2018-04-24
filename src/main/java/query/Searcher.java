package query;


import java.beans.Encoder;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

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
		System.out.println("Enter a term to also search on - OR");
		String or=input.nextLine().trim();
		System.out.println("Enter a term to not search and filter on - NOT");
		String not=input.nextLine().trim();
		System.out.println("Enter a term to not search and filter on - AND");
		String and=input.nextLine().trim();
		analyze(word, and, not, or);
           
	}
	private static SparkSession setUpSpark() {
		SparkConf conf = new SparkConf()
				.setAppName("Simple Application")
				.setMaster("local[*]");

		SparkSession spark = SparkSession
			      .builder()
			      .appName("Example Program")
			      .master("local")
			      .appName("Java Spark SQL basic example")
			      .config("spark.executor.instances", "2")
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
	private static Dataset<Row> andNotOperation(SQLContext sqlContext, String and) {
		String andFile=getFileNumber(and);
		Dataset<Row> expanded= sqlContext.read().json(getFileNumber(and));
		 return expanded.select("id", "pos", "url", "word")
				 .filter(expanded.col("word").equalTo(and))
				 .withColumnRenamed("pos", "pos_1")
				 .withColumnRenamed("word", "word_1");

	}
	
	private static Dataset<Row> wordOrNotOperation(SQLContext sqlContext, String word, String or, String not) {
		String wordFile=getFileNumber(word);
		String orFile=getFileNumber(or);
		Dataset<Row> expanded = sqlContext.read().json(getFileNumber(word), getFileNumber(or), getFileNumber(not));

		return expanded.select("id", "pos", "url", "word")
				 .filter(expanded.col("word").isin(word, or))
				 .filter(expanded.col("word").notEqual(not))
		 .withColumnRenamed("pos", "pos_2")
		 .withColumnRenamed("word", "word_2");


	}
	

	private static void analyze(String word, String and, String not, String or) {
		SparkSession spark=setUpSpark();

		// reads the data
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(spark);
		System.out.println(getFileNumber(word));
		Dataset<Row> expanded = sqlContext.read()
				.json(getFileNumber(word), getFileNumber(or), getFileNumber(not), getFileNumber(and))
				.cache();
		expanded.createOrReplaceTempView("Data");
        Dataset<Row> resultsDB = expanded.repartition(400, expanded.col("word").asc());

		Dataset<Row> filter=expanded.where(expanded.col("word").equalTo("bag"));
		expanded.show();
		
//		Dataset<Row> andData = andNotOperation(sqlContext, and);
//		 andData.show();
//		 Dataset<Row> wordOrNot = wordOrNotOperation(sqlContext, word, or, not);
//		 wordOrNot.show();
//		 
//		 Dataset<Row> filtered=wordOrNot.join(andData).where(andData.col("url").equalTo(wordOrNot.col(("url"))));
//		 filtered.show();
//
//		 JavaRDD<Row> s=filtered.toJavaRDD();
//		 
//		 JavaRDD<Article> articles=s.map((line) -> {
//			 String id= line.getString(line.fieldIndex("id"));
//			 System.out.println(line.fieldIndex("id"));
//
//			 String pos1= line.getString(line.fieldIndex("pos_1"));
//			 String pos2= line.getString(line.fieldIndex("pos_2"));
//
//			 String url= line.getString(line.fieldIndex("url"));
//			 String word1= line.getString(line.fieldIndex("word_1"));
//			 String word2= line.getString(line.fieldIndex("word_2"));
//
//			 return  new Article(id, pos1, pos2, url, word1, word2);
//		 });
//		 
//		 List<Article> result = articles.collect();
//		 System.out.println(result);


		 
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