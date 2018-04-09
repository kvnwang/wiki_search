package mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import wordwrapper.WikiWord;

public class IndexReducer extends Reducer<Text,WikiWord,Text,Text> {

	@Override
	  protected void reduce(Text key, Iterable<WikiWord> values, Context context)  throws IOException, InterruptedException {	
		Set<String> indices=new HashSet<String>(); 
		indices.add("->");
		while (values.iterator().hasNext()) {
			WikiWord text=values.iterator().next();
			String s=text.toString();
			if (s.isEmpty()) continue;
			indices.add(s);
        }
		indices.add("<-");
        context.write(key, new Text(indices.toString()));

	}
}