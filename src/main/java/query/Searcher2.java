package query;


import java.beans.Encoder; 
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Stack;

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




public class Searcher2 {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Scanner input=new Scanner(System.in);		
		System.out.println("Enter a query");
		String query=input.nextLine().trim();
		//returns the query broken up into a stack
		Stack<String> searchTermList = queryParser(query);
        JavaPairRDD<String, Article> finalRDD= applyOperations(searchTermList);
        print(finalRDD);
	}
	private static JavaSparkContext setUpSpark() {
		 SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        return sc;
	}
	//=========================================================================================
	//returns the corresponding file for a given string
	//=========================================================================================

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
	//=========================================================================================
	//for a given query, returns a stack containing the query broken up with each entry either
	//a parenthesis or a word
	//=========================================================================================
	private static Stack<String> queryParser(String query){
		Stack<String> searchTermList = new Stack<String>();
		String[] tempArray= query.split(" ");
		return searchTermList;
	}
	
	//=========================================================================================
	//for a given search term, returns a javaPairRDD that contains the key: docId and value article
	//which contains neighbors, url, title
	//=========================================================================================

	private static JavaPairRDD<String,Article> returnRDD(String search) {
		JavaSparkContext spark=setUpSpark();
		//get info from file
        JavaRDD<String> file = spark.textFile(getFileNumber(search));
        JavaPairRDD<String, Article> returnRDD = file.mapToPair(new PairFunction<String, String, Article>() {
            public Tuple2<String, Article> call(String s) throws Exception {
            	JsonObject json=(JsonObject) new JsonParser().parse(s);
            	String docID = json.get("id").getAsString();
            	String neighbors = json.get("neighbor").getAsString();
            	String url = json.get("url").getAsString();
            	String title = json.get("title").getAsString();
            	Article output = new Article(neighbors, url, title);
                Tuple2<String, Article>  tuple = new Tuple2<String, Article>(docID, output);
                return tuple;
            }
        });
        return returnRDD;
	}
	//=========================================================================================
	//takes in a stack of search terms and applies the operation in order as specified by the
	//hot words and parenthesises
	//=========================================================================================
	private static JavaPairRDD<String, Article> applyOperations(Stack<String> searchTermList){
		while (!searchTermList.isEmpty()) {
			//for brackets
			while (!searchTermList.peek().equals("(")) {
				while (!searchTermList)
			}
		}
	}
}