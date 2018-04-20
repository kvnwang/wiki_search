package mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.tartarus.snowball.ext.PorterStemmer;

import wordwrapper.WikiWord;

public class IndexMapper extends Mapper< LongWritable, Text, Text, WikiWord> {
	private Set<String> stopWords = (Set<String>) EnglishAnalyzer.getDefaultStopSet();
	 private final static PorterStemmer stemmer = new PorterStemmer();

/**
 * map function that reads and parses wiki files
 */
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] line=value.toString().split(",");
		String docId=line[0];
		String url=line[1];
		String title=line[2];
		String content=line[3];

		String words = content.toLowerCase().replaceAll("[^a-z ]", " ");
		StringTokenizer tokenizer=new StringTokenizer(words);

		int position=0;
		while(tokenizer.hasMoreTokens()) {
			String word=tokenizer.nextToken();
			String stemWord=createStem(word);

			if(stemWord.isEmpty() || stemWord.length()<3 || stopWords.contains(stemWord)) continue;
			Text wordText = new Text(stemWord);
			WikiWord wiki=new WikiWord(stemWord, Integer.valueOf(docId), position, url, title);
			context.write(wordText, wiki);
			position++;

		}

	}
/**
 *
 * @param s
 * @return stemme word using aapache lucene nlp package
 * i.e: run, ran, running will return the same word
 */
	protected String createStem(String s) {
		stemmer.setCurrent(s);
	    stemmer.stem();
	    return stemmer.getCurrent().trim();
	}


}
