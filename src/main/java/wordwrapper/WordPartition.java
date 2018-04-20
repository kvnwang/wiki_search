package wordwrapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordPartition extends Partitioner<Text, WikiWord> {
	  @Override
	  public int getPartition(Text text, WikiWord wikiWritable, int i) {
		  int hash1=(text.toString().charAt(0)-'a') % 26;
		  int hash2=(text.toString().charAt(1)-'a') % 26;
		  String value=""+text.toString().charAt(0)+text.toString().charAt(1);
		  return value.hashCode() % 676;
	  }
}
