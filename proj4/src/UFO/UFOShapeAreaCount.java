package UFO;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class UFOShapeAreaCount {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		if(args.length!=2) {  
			System.out.println("usage error");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf, "UFO");  
		
		job.setJarByClass(UFOShapeAreaCount.class);
		job.setMapperClass(UFOShapeAreaMapper.class);
		job.setReducerClass(UFOShapeAreaReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Map의 Key, Value out 형태와 Reduce의 Key, Value out 형태가 다르면 따로 지정을 해주어야 한다.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Area.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
