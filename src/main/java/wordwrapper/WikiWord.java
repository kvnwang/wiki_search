package wordwrapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
/**
 * 
 * @author Kevin
 * Wrapper class of a stemmed text word that contains the document id, word, and position found in the document. 
 * Impelments writtable and writtablecomparable intefaces for hadoop/hdfs to sort word for reducers
 */
public class WikiWord  implements Writable, WritableComparable<WikiWord> {
	private Text word;
	private IntWritable id;
	private IntWritable position;
	
	public WikiWord(String word, int id, int position) {
		this.word=new Text(word);
		this.id=new IntWritable(id);
		this.position=new IntWritable(position);
	}

	  public WikiWord() {
	    this.word = new Text();
	    this.id = new IntWritable();
	    this.position = new IntWritable();
	  }
	
	public IntWritable getId() {
		return this.id;
	}
	
	public IntWritable getPosition() {
		return this.position;
	}
	
	public Text getWord() {
		return this.word;
	}
	
	public Text getText() {
		return this.getText();
	}
	
	public String toString() {
		return "["+this.word +"." + this.id + "." + this.position + "]"; 
	}
	
	
	@Override
	public void readFields(DataInput input) throws IOException {
		word.readFields(input);
		id.readFields(input);
	    position.readFields(input);
		
	}
	@Override
	public void write(DataOutput ouput) throws IOException {
		word.write(ouput);
		id.write(ouput);
		position.write(ouput);
	}
	
	@Override
	public int compareTo(WikiWord o) {
	    int result = this.word.compareTo(o.word);
	    result = result == 0 ? this.id.compareTo(o.id) : result;
	    result = result == 0 ? this.position.compareTo(o.position) : result;
	    return result;
	}
	
}
