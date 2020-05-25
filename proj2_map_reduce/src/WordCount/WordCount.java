package WordCount;

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


public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length!=2) {   //인자로는 어떤 파일의 통계를 낼 것인지 HDFS상의 해당 파일명과 나온 통계를 어디에 저장시킬것인지 HDFS상의 디렉토리 두개가 들어온다.
			System.out.println("usage error");
			System.exit(2);
		}
		
		//HDFS 환경 불러오기
		Configuration conf = new Configuration();

		//이번에는 HDFS를 불러오는 것이 아니라 Job 생성
		Job job = Job.getInstance(conf, "WordCount");   //IOException 발생. 작업명은 WordCount
		
		//어떤게 메인이고 어떤게 mapper고 어떤게 reducer인지 알려줘야 한다.
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//HDFS에서 어떤 타입의 파일을 읽어올 것인지 설정. 엔터값을 기준으로 입력받고 엔터값을 기준으로 출력
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//Mapper에서 출력하는 타입이 어떤 타입인지 지정해주어야함
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//Reducer에서 출력하는 타입도 지정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Mapper와 Reducer의 출력타입이 같다면 한쪽만 써줘도 된다.
		
		
		//input, output path setting
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		
	}
}
