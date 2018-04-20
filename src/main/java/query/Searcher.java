package query;


import java.io.IOException;
import java.util.Scanner;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.function.PairFunction;


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

		process(word, filter, word2);
        

       
	}

	private static void process(String word, String filter, String word2) {
		SparkConf conf = new SparkConf().setAppName("Boolean Search").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
       
        JavaRDD<String> file = sc.textFile("output").cache();
    
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