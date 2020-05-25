package AirlinePerformanceMultiple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class DelayCountWithMultipleOutputs {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2) {
			System.out.println("usage error");
			System.exit(2);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Delay count with Counter and Multiple");
		
		job.setJarByClass(DelayCountWithMultipleOutputs.class);
		job.setMapperClass(DelayCountMapperWithMultipleOutputs.class);
		job.setReducerClass(DelayCountReducerWithMultipleOutputs.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//MultipleOutputs setting
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		//MultipleOutputs.addNamedOutput(conf, namedOutput, outputFormatClass, keyClass, valueClass); - 원형
		
		job.waitForCompletion(true);
	}
}






