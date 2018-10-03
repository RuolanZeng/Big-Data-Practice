package hw1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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

public class ReduceSideJoin {
	
	public static class Map1 extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("\t");
			
			if (mydata.length > 1) {
				String[] friendsid = mydata[1].split(",");
				for(String id:friendsid) {
					context.write(new LongWritable(Integer.parseInt(mydata[0])),new LongWritable(Integer.parseInt(id)));
				}
			}
		}
	}
	
	public static class Reduce1 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
		
		HashMap<LongWritable,LongWritable> map = new HashMap<LongWritable,LongWritable>();
		
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			
			int minAge = 10000;
		
			for(LongWritable val:values) {
				if(map.containsKey(val)) {
					int age = Integer.parseInt(map.get(val).toString());
					if(age < minAge) {
						minAge = age;
					}
				}
			}
			context.write(key,new LongWritable(minAge));
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
		
			super.setup(context);
			//read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
			Path part=new Path(context.getConfiguration().get("ARGUMENT"));//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	String[] dob = arr[9].split("/");
		        	int year = Integer.parseInt(dob[2]);
		        	int age = 2018-year+1;
		        	//put (id, age) in the HashMap variable
		        	map.put(new LongWritable(Integer.parseInt(arr[0])), new LongWritable(age));
		        	line=br.readLine();
		        }
		    }
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, LongWritable, LongWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {	
			String[] mydata = value.toString().split("\t");
			context.write(new LongWritable(Integer.parseInt(mydata[1])),new LongWritable(Integer.parseInt(mydata[0])));
		}
	}
	
	public static class Reduce2 extends Reducer<LongWritable,LongWritable,Text,Text> {
		
		HashMap<String,String> map2 = new HashMap<String,String>();
		int Count = 0;
		
		public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
			
			for(LongWritable val:values) {
				if(map2.containsKey(val.toString())) {
					if(Count < 10) {
						Count++;
						String address = map2.get(val.toString());
						context.write(new Text(val.toString()),new Text(address + "," + key.toString()));
					}
				}
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			
			super.setup(context);
			//read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
			Path part=new Path(context.getConfiguration().get("ARGUMENT"));//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	//put (id, address) in the HashMap variable
		        	map2.put(arr[0],arr[3]+","+arr[4]+","+arr[5]);
		        	line=br.readLine();
		        }
		    }
		}
		
	}
	
	// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			// get all args
			if (otherArgs.length != 3) {
				System.err.println("Usage: ReduceSideJoin <in> <out>");
				System.exit(2);
			}
			
			conf.set("ARGUMENT",otherArgs[1]);

			// create a job with name "reducesidejoinJob1"
			Job job = new Job(conf, "reducesidejoinJob1");
			job.setJarByClass(ReduceSideJoin.class);
			job.setMapperClass(Map1.class);
			job.setReducerClass(Reduce1.class);


			// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

			// set output key type
			job.setOutputKeyClass(LongWritable.class);
			// set output value type
			job.setOutputValueClass(LongWritable.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]+"/temp"));
			
			
			// create a job with name "reducesidejoinJob2"
			Job job2 = new Job(conf, "reducesidejoinJob2");
			job2.setJarByClass(ReduceSideJoin.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			
			
			job2.setMapOutputKeyClass(LongWritable.class);
			job2.setMapOutputValueClass(LongWritable.class);
			// set output key type
			job2.setOutputKeyClass(Text.class);
			// set output value type
			job2.setOutputValueClass(Text.class);
			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job2, new Path(otherArgs[2]+"/temp"));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]+"/final"));
			
			
			//Wait till job completion
			System.exit(job.waitForCompletion(true)&job2.waitForCompletion(true) ? 0 : 1);
			
		}
}
