package wordwrapper;


import org.apache.hadoop.io.WritableComparable; 
import org.apache.hadoop.io.WritableComparator;

public class WordComparator extends WritableComparator {
  public WordComparator() {
	  super(WikiWord.class, true);
  }

  @Override
  public int compare(WritableComparable wc1, WritableComparable wc2) { 
    WikiWord w1 = (WikiWord) wc1;
    WikiWord w2 = (WikiWord) wc2;
    return w1.getWord().compareTo(w2.getWord());
  } 
}
