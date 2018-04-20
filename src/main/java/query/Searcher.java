package query;


import java.io.IOException;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

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
		
		System.out.println("Enter a term to not search and filter on");
		String filter=input.nextLine().trim();
		
		System.out.println("Enter a term to also search on");
		String word2=input.nextLine().trim();

		analyze(word, filter, word2);
        

       
	}
	
	private String getFileName(String text) {
		String value=""+text.toString().charAt(0)+text.toString().charAt(1);
		int hash= value.hashCode() % 676;
		return null;
	}

	
	private static void analyze(String word, String filter, String word2) {
		SparkConf conf = new SparkConf().setAppName("Boolean Search").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("output/part-r-00004").cache();
        
        file.filter(line -> {
        		String key=new JsonParser().parse(line).getAsJsonObject().keySet().iterator().next();
        		return key.contains(word);
        	})
        .foreach(filterLine -> {
    			String key=new JsonParser().parse(filterLine).getAsJsonObject().keySet().iterator().next();
    			JsonObject inner= new JsonParser().parse(filterLine).getAsJsonObject().getAsJsonObject(key);
	        JsonArray urls=inner.getAsJsonArray("urls");
	        JsonArray ids=inner.getAsJsonArray("ids");
	        JsonArray pos=inner.getAsJsonArray("positions");
	        JsonArray words=inner.getAsJsonArray("words");
        });
        
        
	}


	private static void process(String word, String filter, String word2) {
		SparkConf conf = new SparkConf().setAppName("Boolean Search").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> file = sc.textFile("output/part-r-00004").cache();
    
        PairFunction<String, String, String> keyData =
        		new PairFunction<String, String, String>() {
        		public Tuple2<String, String> call(String x) {
        			String [] row=x.split("\\[" ,  2);
        			return new Tuple2(row[0], row[1]);
        		}
        	};
     
        	JavaPairRDD<String, String> pairs = file.mapToPair(keyData);
        	
        
        	Function<Tuple2<String, String>, Boolean> longWordFilter =
        		  new Function<Tuple2<String, String>, Boolean>() {
        		    public Boolean call(Tuple2<String, String> keyValue) {
        		    		String term=keyValue._1();
        		    		return ((term.contains(word) || term.contains(word2)) && !term.contains(filter));
        		    }
        	};
        	
        	JavaPairRDD<String, String> selectedRows = pairs.filter(longWordFilter);
        	JavaRDD<String> finalResult = selectedRows.map(x -> x._2);
        	finalResult.foreach(data -> {
        		System.out.println(data);
        	});
        	finalResult.saveAsTextFile("spark_output");
        	
	}
}