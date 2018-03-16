import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IndexReducer extends Reducer<Text,Text,Text,Text> {

	@Override
	  protected void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {	
		Set<String> indices=new HashSet<String>();        
		while (values.iterator().hasNext()) {
			String s=values.iterator().next().toString();
			if (s.isEmpty()) continue;
			indices.add(s);
        }
		
        context.write(key, new Text(indices.toString()));
        
		
	}
}