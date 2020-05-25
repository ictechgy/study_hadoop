package AirlinePerformanceSecondarySort;

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



public class DelayCountWithDateKey {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2) {
			System.out.println("usage error");
			System.exit(2);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Delay count with Counter and Multiple");
		
		job.setJarByClass(DelayCountWithDateKey.class);
		job.setMapperClass(DelayCountMapperWithDateKey.class);
		job.setReducerClass(DelayCountReducerWithDateKey.class);
		
		job.setPartitionerClass(GroupKeyPartitioner.class);  //연도를 기준으로 나누어라
		job.setGroupingComparatorClass(GroupKeyComparator.class); //세 그룹으로 나눈 연도에 대해 연도자체로 정렬수행
		job.setSortComparatorClass(DateKeyComparator.class); //한 연도그룹 내에서 정렬수행
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(DateKey.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, Text.class, IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, Text.class, IntWritable.class);
		
		job.waitForCompletion(true);
	}

}
