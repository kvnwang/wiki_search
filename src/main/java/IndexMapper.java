import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

import opennlp.tools.stemmer.PorterStemmer;

public class IndexMapper extends Mapper< LongWritable, Text, Text, Text> {
	private Set<String> stopWords = 		(Set<String>) EnglishAnalyzer.getDefaultStopSet();




	@Override	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] line=value.toString().split(",");
		String content=line[3];
		String docId=line[0];
		String words = content.toLowerCase().replaceAll("[^a-zA-Z ]", " ");
		StringTokenizer tokenizer=new StringTokenizer(words);
		Text file=new Text(docId);
//		
//		FileReader reads text files in the default encoding.
	     
		while(tokenizer.hasMoreTokens()) {
			String word=tokenizer.nextToken();
			if(word.isEmpty()) continue;
			Text wordText = new Text(word.toString());
//			PorterStemmer stemmer = new PorterStemmer();
//			String stem=stemmer.stem(word);
			if (!stopWords.contains(word)) {
				System.out.println(word);
				context.write(wordText, file);
			}
		}

	}
}
