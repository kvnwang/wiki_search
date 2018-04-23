package query;

import java.io.Serializable;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

@Service
public class Query implements Serializable {
	@Autowired
	JavaSparkContext sc;

	public Query() {
	}
	
	public String search(String input) {
		String word="all";        
		JavaRDD<String> file = sc.textFile("output/part-r-00000");
        
        PairFunction<String, String, String> keyData =
        		new PairFunction<String, String, String>() {
        		public Tuple2<String, String> call(String x) {
        			String [] row=x.split("\\[" , 2);
        			return new Tuple2(row[0], row[1]);
        		}
        	};
     
        	JavaPairRDD<String, String> pairs = file.mapToPair(keyData);
        	
        
        	Function<Tuple2<String, String>, Boolean> longWordFilter =
        		  new Function<Tuple2<String, String>, Boolean>() {
        		    public Boolean call(Tuple2<String, String> keyValue) {
        		      return (keyValue._1().contains(word));
        		    }
        	};
        	JavaPairRDD<String, String> selectedRows = pairs.filter(longWordFilter);
        	JavaRDD<String> finalResult = selectedRows.map(x -> x._2);
        	return (String) finalResult.collect().toArray()[0];
      

	}
	
}
