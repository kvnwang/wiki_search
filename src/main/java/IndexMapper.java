import java.io.IOException;
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
		while(tokenizer.hasMoreTokens()) {
			String word=tokenizer.nextToken();
			if(word.isEmpty()) continue;
			Text wordText = new Text(word.toString());
			context.write(wordText, file);
		}
 		
	}
}
