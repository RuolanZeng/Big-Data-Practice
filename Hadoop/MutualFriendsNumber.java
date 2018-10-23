package hw1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MutualFriendsNumber {
	
	public static class Map1
	extends Mapper<LongWritable, Text, Text, Text>{
		
		String[] keyArray = new String[2];
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
//			System.out.println("key= "+key.toString());
//			System.out.println("value= "+value.toString());
			
			String[] mydata = value.toString().split("\t");
			
			if (mydata.length > 1) {
				String[] friendsid = mydata[1].split(",");
				for(String id:friendsid) {
					keyArray[0] = mydata[0];
					keyArray[1] = id;
					Arrays.sort(keyArray);
					context.write(new Text(keyArray[0]+","+keyArray[1]),new Text(mydata[1]));
				}
			}
		}
	}
	
	public static class Reduce1 extends Reducer<Text,Text,Text,IntWritable> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<ArrayList<String>> lists = new ArrayList<>();
			IntWritable result = new IntWritable();
			
			for(Text val:values) {
				ArrayList<String> myList = new ArrayList<String>(Arrays.asList(val.toString().split(",")));
				lists.add(myList);
			}
			
			ArrayList<String> mutualFriends = lists.get(0);
			mutualFriends.retainAll(lists.get(1));
			
			int sum = 0;
			for(String fri:mutualFriends) {
				sum+=1;
			}
			result.set(sum);
			context.write(key,result);
		}
	}
	
	public static class Map2
	extends Mapper<LongWritable, Text, LongWritable, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			LongWritable frequency = new LongWritable();
			String[] mydata = value.toString().split("\t");
			
			int outputkey = Integer.parseInt(mydata[1]);
			String outputvalue = mydata[0];
//			System.out.println("outputkey= "+outputkey);
//			System.out.println("outputvalue= "+outputvalue);
			frequency.set(outputkey);
			context.write(frequency,new Text(outputvalue));
		}
	}
	
	public static class Reduce2
	extends Reducer<LongWritable,Text,Text,LongWritable> {
		int mCount = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			for (Text val:values) {
				if(mCount < 10) {
					mCount++;
					context.write(val, key);
				} 
			}
		}
	}
	
//	//rewrite comparator
//		private static class IntWritableDecreasingComparator extends IntWritable.Comparator {
//
//		     public int compare(WritableComparable a, WritableComparable b) {
//		         return -super.compare(a, b);
//		      }
//		     public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//		                return -super.compare(b1, s1, l1, b2, s2, l2);
//		       }
//		}
	
	
	
	// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			// get all args
			if (otherArgs.length != 2) {
				System.err.println("Usage: WordCount <in> <out>");
				System.exit(2);
			}

			// create a job with name "mutualfriendsnumberJob1"
			Job job = new Job(conf, "mutualfriendsnumberJob1");
			job.setJarByClass(MutualFriendsNumber.class);
			job.setMapperClass(Map1.class);
			job.setReducerClass(Reduce1.class);


			// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

			// set output key type
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(Text.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+"/temp"));
			
			
			// create a job with name "mutualfriendsnumberJob2"
			Job job2 = new Job(conf, "mutualfriendsnumberJob2");
			job2.setJarByClass(MutualFriendsNumber.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			
			
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(Text.class);
			// set output key type
			job2.setOutputKeyClass(Text.class);
			// set output value type
			job2.setOutputValueClass(LongWritable.class);
//			job2.setSortComparatorClass(IntWritableDecreasingComparator.class);
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+"/temp"));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+"/final"));
			
			
			//Wait till job completion
			System.exit(job.waitForCompletion(true)&job2.waitForCompletion(true) ? 0 : 1);
		}

}