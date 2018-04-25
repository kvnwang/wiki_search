package mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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
		JSONObject json=new JSONObject();
	
		while (values.iterator().hasNext()) {
			WikiWord text=values.iterator().next();
			String s=text.toString();
			if (s.isEmpty()) continue;
			String word=text.getWord().toString();
			String pos=text.getPosition().toString();
			String id=text.getId().toString();
			String url=text.getUrl().toString();
			String neighbor = text.getNeighbors().toString();
			String title = text.getTitle().toString();


			
			json.put("key", keyString);
			json.put("id", id);
			json.put("pos", pos);
			json.put("url", url);
			json.put("word", word);
			json.put("neighbor", neighbor);
			json.put("title", title);
	        context.write(new Text(json.toString()), null);

        }
		

//		while (values.iterator().hasNext()) {
//			WikiWord text=values.iterator().next();
//			String jsonString=text.toString();
//			String s=text.toString();
//			if (s.isEmpty()) continue;
//			indices.add(jsonString);
//			
//			ids.add(text.getId().toString());
//			positions.add(text.getPosition().toString());
//			urls.add(text.getUrl().toString());
//			words.add(text.getWord().toString());
//		
//        }
//		json.put("ids", ids);
//		json.put("positions", positions);
//		json.put("urls", urls);
//		json.put("words", words);
//		
//		JSONObject res=new JSONObject();
//		res.put(key.toString(), json);
//        context.write(new Text(json.toString()), null);

	}
}