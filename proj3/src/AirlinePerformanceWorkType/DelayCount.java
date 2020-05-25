package AirlinePerformanceWorkType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DelayCount extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new DelayCount(), args);
		//ToolRunner 를 통해 먼저 static run 메소드를 부른 다음에 그 안에서 다시 아래에 내가 오버라이드한 run 메소드를 호출한다. 
		//이 때 HDFS설정과 이 클래스에 대한 객체, 입력으로 들어온 args값을 같이 넘긴다. 
	}
	
	//이 클래스에서 Configured 는 왜 extends한 것인가 - Tool에게 HDFS의 설정값을 넘겨주기 위함
	//run메소드를 실행시키면서 -D 옵션값에 대한것은 자동으로 mapper에게 넘겨주는가? - 그렇다
	
	/*
	ToolRunner에서 구현한 run메소드
	
	public class ToolRunner{
		public static int run(Configuration conf, Tool tool, String[] args) throws Exception{
			HDFS설정값을 인자로 필요로 하며 Tool객체를 필요로 한다. 다형성을 이용해서 Tool을 상속받은 객체를 받는다.
		
		    if(conf == null) {     만약 설정값을 넘겨주지 않았다면 여기에서 HDFS설정을 읽는다.
		      conf = new Configuration();
		    }
		    GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		    //set the configuration back, so that Tool can configure itself
		    tool.setConf(conf);
		    
		    //get the args w/o generic hadoop args
		    String[] toolArgs = parser.getRemainingArgs();
		    return tool.run(toolArgs);
		}
	}
  	
  	하둡API - http://hadoop.apache.org/docs/r3.1.0/api/org/apache/hadoop/util/ToolRunner.html
	 */
	
	
	@Override
	public int run(String[] args) throws Exception {	
		//사용자 정의 옵션 읽기
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		
		if(otherArgs.length!=2) {
			System.out.println("usage error");
			System.exit(2);
		}
		
		Job job = Job.getInstance(getConf(), "Delay count");
		
		job.setJarByClass(DelayCount.class);
		job.setMapperClass(DelayCountMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		job.waitForCompletion(true);
		
		return 0;
	}

}
