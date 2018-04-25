package query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.Stack;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;

public class SparkSearch {
	public static HashSet<String> operatorTerms = new HashSet<String>();

	public SparkSearch() {
		
	}
	
	public List<Article> search(String s) {
		initHash();
		Scanner input = new Scanner(System.in);
		System.out.println("Enter a query");
		String query = input.nextLine().trim();
		// returns the query broken up into a stacka
		ArrayList<String> searchTermList = queryParser(query);
		JavaPairRDD<String, Article> finalRDD = applyOperations(searchTermList);
    		JavaRDD<Article> finalResult = finalRDD.map(x -> x._2);
		List<Tuple2<String, Article>> saf = finalRDD.take(30);
		Iterator<Tuple2<String, Article>> iter = saf.iterator();
		ArrayList<Article> searches=new ArrayList<Article>();		
		while(iter.hasNext()) {
			Article a=iter.next()._2();
			searches.add(a);
		}
		return searches;
	}
	private static ArrayList<String> fakeList() {
		ArrayList<String> searchTermList=new ArrayList<String>();
		searchTermList.add("cat");
		searchTermList.add("dog");
		searchTermList.add("AND");
		searchTermList.add("bird");
		searchTermList.add("fish");
		searchTermList.add("OR");
		searchTermList.add("NOT");
		return searchTermList;
	}
	
	private static JavaSparkContext setUpSpark() {
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		return sc;
	}

	private static void initHash() {
		operatorTerms.add("NOT");
		operatorTerms.add("AND");
		operatorTerms.add("OR");
	}

	// =========================================================================================
	// returns the corresponding file for a given string
	// =========================================================================================

	private static String getFileNumber(String s) {
		String value = "" + s.charAt(0) + s.charAt(1);
		String name = "output/part-r-00";
		int hash = value.hashCode() % 676;
		if (hash < 10) {
			name += ("00" + hash);
		} else if (hash < 100) {
			name += ("0" + hash);
		} else {
			name += ("" + hash);
		}
		return name;
	}

	// =========================================================================================
	// for a given query, returns a stack containing the query broken up with each
	// entry either
	// a parenthesis or a word
	// **THE WAY I HAVE IT SET, YOU CANNOT INPUT WORDS WITH SPACES (eg. Stephen
	// Curry)
	// =========================================================================================
	private static ArrayList<String> queryParser(String query) {
		String[] tempArray = query.split(" ");
		Stack<String> workingStack = new Stack<String>();
		Stack<String> operators = new Stack<String>();
		ArrayList<String> output = new ArrayList<String>();
		int i = 0;
		for (String s : tempArray) {
			if (s.equals("(")) {
				workingStack.push(s);
			} else if (s.equals(")")) {
				if (operatorTerms.contains(workingStack.peek())) {
					if (!operators.isEmpty()) {
						output.add(operators.pop());
						if (!workingStack.isEmpty()) operators.push(workingStack.pop());
					} else {
						if (!workingStack.isEmpty()) operators.push(workingStack.pop());
						while (!operatorTerms.contains(workingStack.peek()) && !workingStack.peek().equals("(")) {
							if (!workingStack.isEmpty()) output.add(workingStack.pop());
						}
					}
					if (!workingStack.isEmpty()) workingStack.pop();
				}else {
					if (!workingStack.isEmpty()) output.add(workingStack.pop());
					String operation = workingStack.pop();
					if (!workingStack.isEmpty()) output.add(workingStack.pop());
					output.add(operation);
					if (!workingStack.isEmpty()) workingStack.pop();
				}
			} else {
				workingStack.push(s);
			}
			i++;
		}
		while (!workingStack.isEmpty()) {
			if (!workingStack.peek().equals("C")) output.add(workingStack.pop());
		}
		if(!operators.isEmpty()) output.add(operators.pop());
		for (String S : output) {
			System.out.println(S);
		}
		return output;
	}
	

	// =========================================================================================
	// for a given search term, returns a javaPairRDD that contains the key: docId
	// and value article8 which contains neighbors, url, title
	// =========================================================================================

	private static JavaPairRDD<String, Article> returnRDD(String search) {
		JavaSparkContext spark = setUpSpark();
		// get info from file
		JavaRDD<String> file = spark.textFile(search);
		JavaPairRDD<String, Article> returnRDD = file.mapToPair(new PairFunction<String, String, Article>() {
			
			public Tuple2<String, Article> call(String s) throws Exception {
				JsonObject json = (JsonObject) new JsonParser().parse(s);
				String docID = json.get("id").getAsString();
				String neighbors = json.get("neighbor").getAsString();
				String url = json.get("url").getAsString();
//				String title = json.get("title").getAsString();
				String title=null;
				String word=json.get("word").getAsString();
				Article output = new Article(word, docID, url, neighbors);
				Tuple2<String, Article> tuple = new Tuple2<String, Article>(docID, output);
				return tuple;
			}
		});
		return returnRDD;
	}

	private static boolean isOperand(String s) {
		return (s.equals("AND") || s.equals("OR") || s.equals("NOT"));
	}
	
	
	private static String getFiles(List<String> output) {
		Set<String> files=new HashSet<String>();
		for(String s: output) {	
			files.add(getFileNumber(s));
		}
		return files.toString().replace("[", "").replace("]", "").replace(" ", "");
	}
	
	
	
	private static JavaPairRDD<String, Article> applyAnd(JavaPairRDD<String, Article> articles, String left, String right) {
		JavaPairRDD<String, Article> res= articles.filter( i-> i._2().getword().equals(left) && i._2().getword().equals(right)); 
//		JavaRDD<Article> rdd=articles.map(i->i._2);
//		rdd.filter(i->i.getword().equals(left));
//		rdd.foreach(i->System.out.println(i));
//		
		
		Function<Tuple2<String, Article>, Boolean> longWordFilter =
      		  new Function<Tuple2<String, Article>, Boolean>() {
      		    public Boolean call(Tuple2<String, Article> keyValue) 	{
      		      return (keyValue._2().getword().equals(left));
      		    }
      	};
      	articles.filter(longWordFilter);
       	
		return articles;
	}
	
	
	private static JavaPairRDD<String, Article> applyOr(JavaPairRDD<String, Article> articles, String left, String right) {
		Function<Tuple2<String, Article>, Boolean> longWordFilter =
	      		  new Function<Tuple2<String, Article>, Boolean>() {
	      		    public Boolean call(Tuple2<String, Article> keyValue) 	{
	      		      return (keyValue._2().getword().equals(left));
	      		    }
	      	};
	      	articles.filter(longWordFilter);
	       	
			return articles;
//		return articles.filter( i-> (i._2.getword().equals(left) || i._2.getword().equals(right)));
	}
	
	private static JavaPairRDD<String, Article> applyNot(JavaPairRDD<String, Article> articles, String left, String right) {
		Function<Tuple2<String, Article>, Boolean> longWordFilter =
	      		  new Function<Tuple2<String, Article>, Boolean>() {
	      		    public Boolean call(Tuple2<String, Article> keyValue) 	{
	      		      return (!keyValue._2().getword().equals(left) && !keyValue._2().getword().equals(right));
	      		    }
	      	};
	      	articles.filter(longWordFilter);
	      	
	      	return articles;
	      
//		return articles.filter( i-> (i._2.getword().equals(left) && !i._2.getword().equals(right)));
	}
	
	
	// =========================================================================================
	// takes in a pair of JavaPairRDD terms and an operation term and outputs the
	// resultant JavaPairRDD
	// =========================================================================================
	private static JavaPairRDD<String, Article> applyOperations(ArrayList<String> output) {
		String files=getFiles(output);
		JavaPairRDD<String, Article> articles= returnRDD(files);
		Stack<String> operatorStack=new Stack<String>();
		Stack<JavaPairRDD<String, Article>> JavaRDDStack=new Stack<JavaPairRDD<String, Article>>();
		JavaPairRDD<String, Article> current;
		String left="";
		String right="";
		int size=output.size();
		
		for(int i=0; i<size; i++) {	
			String s=output.get(i);
			if (isOperand(s)) {				
				if(operatorStack.size()!=0) {
					left=operatorStack.pop();
					right=operatorStack.pop();	
				}
				
				if (s.equals("AND")) {
					current=applyAnd(articles, left, right);

					if(!JavaRDDStack.empty() && i==size-1) {
						JavaPairRDD<String, Article> last= JavaRDDStack.pop();
						return last.intersection(current);
					} else {

						JavaRDDStack.push(current);
					}
					
				} else if (s.equals("OR"))  {

					current=applyOr(articles, left, right);
					if(!JavaRDDStack.empty() && i==size-1) {
						JavaPairRDD<String, Article> last= JavaRDDStack.pop();
						return last.union(current);
					} else {
						JavaRDDStack.push(current);
					}
				} else if (s.equals("NOT")) {
					
					current=applyNot(articles, left, right);
					if(!JavaRDDStack.empty() && i==size-1) {
						JavaPairRDD<String, Article> last= JavaRDDStack.pop();

						return last.subtract(current);

					} else {
						JavaRDDStack.push(current);
					}
				}
			} else {
				operatorStack.push(s);
			}
		}


		return articles;
	}
}
