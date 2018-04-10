import java.io.IOException; 

import mapreduce.IndexMapper;
import mapreduce.IndexReducer;
import wordwrapper.WikiWord;
import wordwrapper.WordPartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
public class Driver {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Path wiki = new Path(args[0]);
    Path out = new Path(args[1]);
    
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "wiki - g3");

    TextInputFormat.addInputPath(job, wiki);
    TextOutputFormat.setOutputPath(job, out);

    
    job.setJarByClass(Driver.class);
    job.setMapperClass(IndexMapper.class);
    job.setReducerClass(IndexReducer.class);
    
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(WikiWord.class);

    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(WikiWord.class);

    job.setPartitionerClass(WordPartition.class);

    job.waitForCompletion(true);
  }
}