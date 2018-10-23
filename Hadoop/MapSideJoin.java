package hw1;

import java.awt.List;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapSideJoin {
	
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
	
	public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			ArrayList<ArrayList<String>> lists = new ArrayList<>();
			Text result = new Text();
			
			for(Text val:values) {
				ArrayList<String> myList = new ArrayList<String>(Arrays.asList(val.toString().split(",")));
				lists.add(myList);
			}
			
			ArrayList<String> mutualFriends = lists.get(0);
			mutualFriends.retainAll(lists.get(1));
			
			String ids = "";
			
			for(String fri:mutualFriends) {
				if(ids == "") {
					ids = fri;
				}else {
					ids += ","+ fri;
				}
			}
			result.set(ids);
			context.write(key,result);
		}
	}
	
	public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
		
		HashMap<String,String> map = new HashMap<String,String>();
		
		private Text pairs = new Text(); // type of output key
		private Text outputvalues = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] mydata = value.toString().split("\t");
			
			if(mydata.length>1) {
				String[] ids = mydata[1].split(",");
				for (String id : ids) {
					if (map.containsKey(id)){
						pairs.set(mydata[0]); 
						outputvalues.set(map.get(id));
						context.write(pairs,outputvalues); // create a pair <pairs, name:state>
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
		        	//put (id, name:state) in the HashMap variable
		        	map.put(arr[0], arr[1]+":"+arr[4]);
		        	line=br.readLine();
		        }
		    }
		}
	}
	
	public static class Reduce2 extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			String info = "";
			for(Text val:values) {
				if(info == "") {
					info = val.toString();
				}else {
					info += "," + val.toString();
				}
			}
			context.write(key,new Text(info.toString()));
		}
	}
	
	// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			
			// get all args
			if (otherArgs.length != 3) {
				System.err.println("Usage: MapSideJoin <in> <out>");
				System.exit(2);
			}
			
			conf.set("ARGUMENT",otherArgs[1]);

			// create a job with name "mapsidejoinJob1"
			Job job = new Job(conf, "mapsidejoinJob1");
			job.setJarByClass(MapSideJoin.class);
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
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]+"/temp"));
			
			
			// create a job with name "mapsidejoinJob2"
			Job job2 = new Job(conf, "mapsidejoinJob2");
			job2.setJarByClass(MapSideJoin.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			// set output key type
			job2.setOutputKeyClass(Text.class);
			// set output value type
			job2.setOutputValueClass(Text.class);
//			job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			//set the HDFS path of the input data
			FileInputFormat.addInputPath(job2, new Path(otherArgs[2]+"/temp"));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]+"/final"));
			
			
			//Wait till job completion
			System.exit(job.waitForCompletion(true)&job2.waitForCompletion(true) ? 0 : 1);
		}
}
