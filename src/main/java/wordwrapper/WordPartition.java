package wordwrapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartition extends Partitioner<Text, WikiWord> {
	  @Override
	  public int getPartition(Text text, WikiWord wikiWritable, int i) {
	        return getHash(text.toString(), i);

	  }
	 
	    public static int getHash(String word, int mod) {
	        int first = word.charAt(0) - 'a';
	        int second = word.charAt(1) - 'a';
	        return Math.abs((first * 26 + second) % mod);
	    }
}
