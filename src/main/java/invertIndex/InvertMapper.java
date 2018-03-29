package invertIndex;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

//import opennlp.tools.stemmer.PorterStemmer;

public class InvertMapper extends Mapper< LongWritable, Text, Text, Text> {
	@SuppressWarnings("unchecked")
	private Set<String> stopWords = 	(Set<String>) EnglishAnalyzer.getDefaultStopSet();


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] line=value.toString().split(",");
		String content=line[3];
		String docId=line[0];
		String words = content.toLowerCase().replaceAll("[^a-zA-Z ]", " ");
		StringTokenizer tokenizer=new StringTokenizer(words);
		Text file=new Text(docId);
		int positionInDoc = 0;
		while(tokenizer.hasMoreTokens()) {
			String word=tokenizer.nextToken();
			if(word.isEmpty()) continue;
			positionInDoc++;
			String fileAsString = file.toString();
			fileAsString+=":"+Integer.toString(positionInDoc);
			Text fileText = new Text(fileAsString);
			Text wordText = new Text(word.toString());
			if (!stopWords.contains(word)) {
				context.write(wordText, fileText);
			}
		}

	}
	
	
}
