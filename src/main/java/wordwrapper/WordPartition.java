package wordwrapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartition extends Partitioner<Text, WikiWord> {
	  @Override
	  public int getPartition(Text text, WikiWord wikiWritable, int i) {
	    return Math.abs(wikiWritable.getWord().hashCode() % i);
	  }
}
