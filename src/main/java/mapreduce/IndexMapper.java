package mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
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

		
		//added by geoffrey
		ArrayList<String> neighborsFrame = new ArrayList<String>();
		Queue<WikiWord> bufferOfWords = new LinkedList<>();
		int neighborLength = 8;
		
		Set<String> wordIDSet=new HashSet<String>();

		String words = content.toLowerCase().replaceAll("[^a-z ]", " ");
		StringTokenizer tokenizer=new StringTokenizer(words);

		int position=0;
		//geoffrey has changed
		while(tokenizer.hasMoreTokens()) {
			
			String word=tokenizer.nextToken();
			String stemWord=createStem(word);
			
			neighborsFrame.add(word);
			//write a buffered word to the reducers
			if (neighborsFrame.size() >= neighborLength && bufferOfWords.peek()!=null) {
				WikiWord temp =  bufferOfWords.poll();
				//get neighbors
				String neighbors = "";
				for (int i = 0; i < neighborsFrame.size(); i++) {
			            neighbors = neighbors + " " + (neighborsFrame.get(i));
			        }
				//System.out.println(neighbors);
				temp.modifyNeighbors(neighbors);
				context.write(temp.getWordText(), temp);
				neighborsFrame.remove(0);
				while (neighborsFrame.size() > neighborLength) {
					neighborsFrame.remove(0);
				}
			}
			
			if(stemWord.isEmpty() || stemWord.length()<3 || stopWords.contains(stemWord)) continue;
			Text wordText = new Text(stemWord);
			
			//checks that we don't output more than for a given word, don't output multiple positions - only output first position.
			String duplicateCheck=word+"-"+docId;
			if(!wordIDSet.contains(duplicateCheck)) {
				wordIDSet.add(duplicateCheck);
				WikiWord wiki=new WikiWord(stemWord, Integer.valueOf(docId), position, url, title, null, wordText);
				//context.write(wordText, wiki);
				bufferOfWords.add(wiki);
			}

			position++;

		}
		String neighbors = "";
		for (int i = 0; i < neighborsFrame.size(); i++) {
	            neighbors = neighbors + (neighborsFrame.get(i));
	        }
		//add the rest of the unwritten words
		while (bufferOfWords.size() != 0) {
			WikiWord temp =  bufferOfWords.poll();
			temp.modifyNeighbors(neighbors);
			context.write(temp.getWordText(), temp);
			neighborsFrame.remove(0);
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
