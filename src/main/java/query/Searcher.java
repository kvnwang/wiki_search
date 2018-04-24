package query;


import java.beans.Encoder; 
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

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

		analyze(word);
           
	}
	private static JavaSparkContext setUpSpark() {
		 SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        return sc;
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

	private static void analyze(String search) {
		JavaSparkContext spark=setUpSpark();
        JavaRDD<String> file = spark.textFile("output/part-r-00022");
        JavaRDD<String> articleJDD=file.filter(line -> {
    			JsonObject json=(JsonObject) new JsonParser().parse(line);
    			String keyword=json.get("word").getAsString();
    			return keyword.equals(search);       
        });
//        PairFunction<String, String, Article> keyData =
//        		new PairFunction<String, String, Article>() {
//					@Override
//					public Tuple2<String, Article> call(String t) throws Exception {
//						JsonObject json=(JsonObject) new JsonParser().parse(t);
//			    			String keyword=json.get("word").getAsString();
//			    			String url=json.get("url").getAsString();
//			    			String id=json.get("id").getAsString();
//			    			String pos=json.get("pos").getAsString();
//			    			String neighbor=json.get("neighbor").getAsString();
//			    			Article article=new Article(keyword, id, pos, url, neighbor);
//		        			return new Tuple2(keyword, article);
//					}
//        	};
        	Function<String, Article> mapFunction =
            		new Function<String, Article>() {
						@Override
						public Article call(String s) throws Exception {
							JsonObject json=(JsonObject) new JsonParser().parse(s);
			    			String keyword=json.get("word").getAsString();
			    			String url=json.get("url").getAsString();
			    			String id=json.get("id").getAsString();
			    			String pos=json.get("pos").getAsString();
			    			String neighbor=json.get("neighbor").getAsString();
			    			return new Article(keyword, id, pos, url, neighbor);
						}			
        };
        	
        List<Article> articles = articleJDD.map(mapFunction).collect();
        System.out.println(articles);
	        System.out.println();
	        System.out.println();
	        System.out.println();
	        System.out.println();
	        System.out.println();
	        System.out.println();
        	

        
//     
//        
//
//			@Override
//			public Tuple2<String, String> call(String x) {
//    			String [] row=x.split("\\[" , 2);
//    			return new Tuple2(row[0], row[1]);
//    		}
//
//        });
//        
//        	JavaPairRDD<String, String> pairs = articles.mapToPair(keyData);
//        	
//        

//        
//        JavaPairRDD<String, Article> deviceRdd = articles.mapToPair(new PairFunction<String, Article>() {   
//            public Tuple2<String, String> call(Article sensor) throws Exception {
//                Tuple2<Integer, Article>  tuple = new Tuple2<Integer, Sensor>(Integer.parseInt(sensor.getsId().trim()), sensor);
//                return tuple;
//            }
//        });
        
//        	JavaPairRDD<String, String> pairs = file.mapToPair(keyData);
        	
        

//        
//        file.filter(line -> {
//        		String key=new JsonParser().parse(line).getAsJsonObject().keySet().iterator().next();
//        		return key.contains(word);
//        	})
//        .foreach(filterLine -> {

//        });
//        
		
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