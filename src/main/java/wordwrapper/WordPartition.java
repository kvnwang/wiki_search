package wordwrapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartition extends Partitioner<WikiWord, Text> {
	  @Override
	  public int getPartition(WikiWord wikiWritable, Text text, int i) {
	    return Math.abs(wikiWritable.getWord().hashCode() % i);
	  }
}