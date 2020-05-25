package AirlinePerformanceWorkType;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private String workType; //사용자 옵션값 받아들이는 변수
	
	private Text outputKey = new Text();
	private static final IntWritable outputValue = new IntWritable(1);
	
	//Mapper 가 실행될 때 사용자 옵션값은 한번만 읽어들이면 되므로 setup메소드를 사용하자
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		//super.setup(context);
		workType = context.getConfiguration().get("workType");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(workType.equals("departure")) {
			if(parser.getDepartureDelayTime()>0) {  
				outputKey.set(parser.getYear()+","+parser.getMonth());
				context.write(outputKey, outputValue);
			}
		}else if(workType.equals("arrival")) {
			if(parser.getArriveDelayTime()>0) {  
				outputKey.set(parser.getYear()+","+parser.getMonth());
				context.write(outputKey, outputValue);
			}
		}
	
	}
	
	
}
