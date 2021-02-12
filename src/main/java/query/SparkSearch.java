package query;

import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import scala.Tuple2;


//@ComponentScan("configuration")

public class SparkSearch {
//    @Autowired
//    JavaSparkContext spark;

    private static SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
    private static JavaSparkContext spark = new JavaSparkContext(conf);

	public SparkSearch() {}


	public List<Article> search(String query) {
		if(query.length()<3) {
			return new ArrayList<Article>();
		} else {
			QueryParser parser=new QueryParser();
			ArrayList<String> searchTermList= parser.convert(query);
		    List<Article> result = applyOperations1(searchTermList);
		    System.out.println(result);
		    return result;
		}

    }





// =========================================================================================
// returns the corresponding file for a given string
// =========================================================================================

private static int getHashCode(String word) {
    int first = word.charAt(0) - 'a';
    int second = word.charAt(1) - 'a';
    return Math.abs((first * 26 + second) % 676);
}

private static String getFileNumber(String s) {
	int hash=getHashCode(s);
	String name = "hdfs:/user/cs132g3/Wiki_Search_Engine/output/part-r-00";
	if (hash < 10) return name += ("00" + hash);
	if (hash < 100) return name += ("0" + hash);
	else return name += ("" + hash);
}


// =========================================================================================
// for a given search term, returns a javaPairRDD that contains the key: docId
// and value article8 which contains neighbors, url, title
// =========================================================================================

private static JavaPairRDD<String, Article> returnRDD(String search) {
	// get info from file
	JavaRDD<String> file = spark.textFile(search);
	JavaPairRDD<String, Article> returnRDD = file.mapToPair(new PairFunction<String, String, Article>() {

		public Tuple2<String, Article> call(String s) throws Exception {
			JsonObject json = (JsonObject) new JsonParser().parse(s);
            String docID = json.get("id").getAsString();
			String neighbors = json.get("neighbor").getAsString();
			String url = json.get("url").getAsString();
			String title = json.get("title").getAsString();
			String word=json.get("word").getAsString();
			Article output = new Article(word, docID, neighbors, title);
			Tuple2<String, Article> tuple = new Tuple2<String, Article>(docID, output);
			return tuple;
		}
	});
	return returnRDD.cache();
}

private static boolean isOperand(String s) {
	return (s.equals("AND") || s.equals("OR") || s.equals("NOT"));
}

private static String getFiles(List<String> output) {
	Set<String> files=new HashSet<String>();
	for(String s: output) {
		if(!isOperand(s)) {
			files.add(getFileNumber(s));
		}
	}
	return files.toString().replace("[", "").replace("]", "").replace(" ", "");
}

/**
 * returns a list of articles of all words that applies the stakc operations
 * @param output
 * @return
 */
private static List<Article> applyOperations1(ArrayList<String> output) {
	List<Set<String>> map = new ArrayList<>();
	String files=getFiles(output);
    Map<String, Set<Article>> articles=new HashMap<>();
//creates a map for each word that maps to a set for furhter processing
	spark.textFile(files).cache()
			.filter(line -> {
				JsonObject json = (JsonObject) new JsonParser().parse(line);
				String word = json.get("word").getAsString();

			    return output.contains(word) && !isOperand(word);
            })
            .map(filter -> {
                JsonObject json = (JsonObject) new JsonParser().parse(filter);
				String docID = json.get("id").getAsString();
				String neighbors = json.get("neighbor").getAsString();
				String url = json.get("url").getAsString();
				String title = json.get("title").getAsString();
				String word=json.get("word").getAsString();
				return new Article(word, docID, neighbors, title);
            })
            .collect()
            .forEach(i-> {
                String key=i.getword();
                if(articles.containsKey(key)) {
                    Set<Article> value=articles.get(key);
                    value.add(i);
                    articles.put(key, value);
                } else {
                    articles.put(key, new HashSet<>());
                }
            });
    List<Article> webResults=processQueryStack(output, articles);
    System.out.println("Completed Search");
    return webResults;
}

/**
 * processes query that applies and/not/or operators with a stack
 * @param queries
 * @param allArticles
 * @return
 */
private static  List<Article> processQueryStack(ArrayList<String> queries, Map<String, Set<Article>> allArticles) {
    int size=queries.size();
    Stack<Set<Article>> stck=new Stack<>();
    Set<Article> current=new HashSet<Article>();
    List<Article> returnValue=new ArrayList<>();
    for(int i=0; i<size; i++) {
        String s=queries.get(i);
        if (isOperand(s)) {

            Set<Article> left =stck.pop();
            Set<Article> right =stck.pop();

            if (s.equals("AND")) {
                left.removeAll(right);
                current=left;
            } else if (s.equals("OR")) {
                left.addAll(right);
                current=left;

            } else if(s.equals("NOT")) {

                left.removeAll(right);
                current=left;
            }

            stck.push(current);

            if(size-1==i) return new ArrayList<Article>(stck.pop());

        } else {
            Set<Article> curr = allArticles.get(s);
            if(curr==null) {
            		curr=new HashSet<Article>();
            }
            stck.push(curr);
            if(size-1==i) return new ArrayList<Article>(stck.pop());

        }
    }
    return new ArrayList<Article>();
}

}
