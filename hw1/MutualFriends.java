package hw1;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
	public static class Map
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
	
	public static class Reduce
	extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<ArrayList<String>> lists = new ArrayList<>();
			
			for(Text val:values) {
				ArrayList<String> myList = new ArrayList<String>(Arrays.asList(val.toString().split(",")));
				lists.add(myList);
			}
			
			ArrayList<String> mutualFriends = lists.get(0);
			mutualFriends.retainAll(lists.get(1));
			
			StringBuffer friends = new StringBuffer();
			for(String fri:mutualFriends) {
				friends.append(fri+ ",");
			}
			context.write(key,new Text(friends.toString()));
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

		// create a job with name "mutualfriends"
		Job job = new Job(conf, "mutualfriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}