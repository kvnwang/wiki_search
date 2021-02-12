package mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import net.minidev.json.JSONObject;

public class IndexReducer extends Reducer<Text,WikiWord,Text,NullWritable> {

	@Override
	  protected void reduce(Text key, Iterable<WikiWord> values, Context context)  throws IOException, InterruptedException {	

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


			
			json.put("id", id);
			json.put("pos", pos);
			json.put("url", url);
			json.put("neighbor", neighbor);
			json.put("title", title);
			json.put("word", word);
	        context.write(new Text(json.toString()), null);

        }

	}
	
	public static String createJSON(String s) {
		return s;
	}
}