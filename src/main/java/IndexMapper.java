import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper< LongWritable, Text, Text, Text> {



	@Override	
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] line=value.toString().split(",");
		String content=line[3];
		String docId=line[0];
		String words = content.replaceAll("[^a-zA-Z ]", "").toLowerCase();
		StringTokenizer tokenizer=new StringTokenizer(words);
		Text file=new Text(docId);
		Set<String> stopWords = new HashSet<String>();
		String fileLine = null;

		// FileReader reads text files in the default encoding.
		FileReader fileReader = new FileReader("stop-words-english1.txt");
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		addSet(stopWords, fileReader, bufferedReader);
		fileReader = new FileReader("stop-words-english2.txt");
		addSet(stopWords, fileReader, bufferedReader);
		fileReader = new FileReader("stop-words-english3-google.txt");
		addSet(stopWords, fileReader, bufferedReader);
		fileReader = new FileReader("stop-words-english4.txt");
		addSet(stopWords, fileReader, bufferedReader);
		fileReader = new FileReader("stop-words-english5.txt");
		addSet(stopWords, fileReader, bufferedReader);
		bufferedReader.close();     
		while(tokenizer.hasMoreTokens()) {
			String word=tokenizer.nextToken();
			if(word.isEmpty()) continue;
			Text wordText = new Text(word.toString());
			if (!stopWords.contains(word)) {
				context.write(wordText, file);
			}
		}

	}
	public void addSet(Set<String> stopWords, FileReader fileReader, BufferedReader bufferedReader) throws IOException {
		String fileLine = null;
		bufferedReader = new BufferedReader(fileReader);
		while((fileLine = bufferedReader.readLine()) != null) {
			stopWords.add(fileLine);
		}   
	}
}
