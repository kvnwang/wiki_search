package invertIndex;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
		
		//write the values to the set to eliminate duplicates
		String prevFileID = "";
		String output = key + ": ";
		Boolean nextDoc = false;
		Map<String, HashSet<String>> container = new TreeMap<String, HashSet<String>>();
		while (values.iterator().hasNext()) {
			String s = values.iterator().next().toString();
			//s = 1004:1541
			//System.out.println("Key: " + key.toString() + " Token string: " + s);
			String[] stringArray = s.split(":");
			HashSet<String> check = container.get(stringArray[0]);
			//check if already placed
			if (check == null) {
			    HashSet<String> value = new HashSet<String>();
			    value.add(stringArray[1]);
			    //add it to map
			    container.put(stringArray[0], value);
			} else {
			    container.get(stringArray[0]).add(stringArray[1]);
			}
		}
		
		for (String docID : container.keySet()) {
		  //  System.out.println("DOCID " + docID);
//			String s1 = " DocID: ".concat(docID);
//			String s2 = s1.concat(container.get(docID).toString());
//			
//		    output += " DocID: " + docID + container.get(docID).toString() + "\n";
		    output = output.concat("DocID: ".concat(docID).concat(container.get(docID).toString()).concat("\n"));
		}
		//System.out.println("OUTPUT " + output);
		ctx.write(key,  new Text(output));
	}
}
