package mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import wordwrapper.WikiWord;

public class IndexReducer extends Reducer<Text,WikiWord,Text,NullWritable> {

	@Override
	  protected void reduce(Text key, Iterable<WikiWord> values, Context context)  throws IOException, InterruptedException {	
		String keyString=key.toString();
		Set<String> indices=new HashSet<String>(); 
		JSONObject json=new JSONObject();
		JSONArray ids=new JSONArray();
		JSONArray positions=new JSONArray();
		JSONArray urls=new JSONArray();
		JSONArray words=new JSONArray();

		while (values.iterator().hasNext()) {
			WikiWord text=values.iterator().next();
			String jsonString=text.toString();
			String s=text.toString();
			if (s.isEmpty()) continue;
			indices.add(jsonString);
			
			ids.add(text.getId().toString());
			positions.add(text.getPosition().toString());
			urls.add(text.getUrl().toString());
			words.add(text.getWord().toString());
		
        }
		json.put("ids", ids);
		json.put("positions", positions);
		json.put("urls", urls);
		json.put("words", words);
		JSONObject res=new JSONObject();
		res.put(key.toString(), json);
        context.write(new Text(res.toString()), null);

	}
}