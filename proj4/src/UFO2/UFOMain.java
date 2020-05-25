package UFO2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class UFOMain {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length!=2) {  
			System.out.println("usage error");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf, "UFO");  
		
		job.setJarByClass(UFOMain.class);
		job.setMapperClass(UFOMapper.class);
		job.setReducerClass(UFOReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(UFO.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		
		job.setOutputKeyClass(UFO.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
