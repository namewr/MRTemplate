package MRTemplate;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TestMR {
  
  static class MapperTemplate 
  	extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    @Override
    public void map(LongWritable key, Text value,
        Context context)
        throws IOException, InterruptedException {
    	StringTokenizer token = new StringTokenizer(value.toString());
    	
    	for(;token.hasMoreElements();){
    		word.set(token.nextToken());
    		context.write(word, one);
    	}
    }
  }
  
  static class ReducerTemplate 
  	extends Reducer<Text, IntWritable, Text, IntWritable> {
	  
	private IntWritable sum = new IntWritable();
	  
    @Override
    public void reduce(Text key, Iterable<IntWritable> values,
        Context context)
        throws IOException, InterruptedException {
    	int num;
    	
    	num = 0;
    	for(IntWritable val: values){
    		num += val.get();
    	}
    	sum.set(num);
    	context.write(key, sum);
    }
  }

  static class CombinerTemplate extends Reducer<Text, IntWritable, Text, IntWritable>{
	 
	  @Override
	  public void reduce(Text key, Iterable<IntWritable> values,
			  Context context) throws IOException, InterruptedException{
		  
	  }
  }
  
  static class PartitionerTemplate extends Partitioner<Text, IntWritable>{
	  
	  @Override
	  public int getPartition(Text key, IntWritable value, int numPartitions){
		  return 0;
	  }
  }
  
  public static void main(String[] args) 
		  throws IOException, InterruptedException, ClassNotFoundException {
    if (args.length != 2) {
      System.err.println("Usage: TestMR <input path> <output path>");
      System.exit(-1);
    }
    
    Job job = new Job();
    job.setJarByClass(TestMR.class);
    job.setJobName("MR template");

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    job.setMapperClass(MapperTemplate.class);
    job.setReducerClass(ReducerTemplate.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true)? 1: 0);
  }
 }
