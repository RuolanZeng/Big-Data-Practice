import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable.Comparator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopN {
	
	public static class Map1
	extends Mapper<LongWritable, Text, IntWritable, Text>{

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("\t");
			
			IntWritable outputkey = new IntWritable(Integer.parseInt(mydata[1]));
			String outputvalue = mydata[0];
//			System.out.println("outputkey= "+outputkey);
//			System.out.println("outputvalue= "+outputvalue);
			context.write(outputkey,new Text(outputvalue));
		}
	}
	
	public static class Reduce1
	extends Reducer<IntWritable,Text,Text,IntWritable> {

		int mCount = 0;

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			if(mCount < 5) {
				for (Text val:values) {
//					System.out.println("val= "+ val.toString());
					context.write(val, key); // create a pair <keyword, number of occurences>
					mCount++;
				}
			}
		}
	}
	
	//rewrite comparator
	private static class IntWritableDecreasingComparator extends IntWritable.Comparator {

	     public int compare(WritableComparable a, WritableComparable b) {
	         return -super.compare(a, b);
	      }
	     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
	                return -super.compare(b1, s1, l1, b2, s2, l2);
	       }
	}
	
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		// create a job with name "topn"
		Job job = new Job(conf, "topn");
		job.setJarByClass(TopN.class);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);


		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		
		job.setSortComparatorClass(IntWritableDecreasingComparator.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
