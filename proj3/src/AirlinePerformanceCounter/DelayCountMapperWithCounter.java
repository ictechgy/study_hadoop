package AirlinePerformanceCounter;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable>{

	private String workType; 
	
	private Text outputKey = new Text();
	private static final IntWritable outputValue = new IntWritable(1);
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		workType = context.getConfiguration().get("workType");
		
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(workType.equals("departure")) {
			if(parser.isDepartureDelayAvailable()) {  //일단 출발은 했다면
				if(parser.getDepartureDelayTime()>0) {  //지연출발
					outputKey.set(parser.getYear()+","+parser.getMonth());
					context.write(outputKey, outputValue);  //Reducer로는 얘만 감
				}else if(parser.getDepartureDelayTime() == 0) {  //제대로 출발
					context.getCounter(DelayCounters.SCHEDULED_DEPARTURE).increment(1);
				}else if(parser.getDepartureDelayTime()<0) {  //조기출발
					context.getCounter(DelayCounters.EARLY_DEPARTURE).increment(1);
				}
			}else {  //출발 못한경우
				context.getCounter(DelayCounters.NOT_AVAILABLE_DEPARTURE).increment(1);
			}
		}else if(workType.equals("arrival")) {
			if(parser.isArriveDelayAvailable()) {  //일단 도착은 했다면
				if(parser.getArriveDelayTime()>0) {  //지연도착
					outputKey.set(parser.getYear()+","+parser.getMonth());
					context.write(outputKey, outputValue);  //Reducer로는 얘만 감
				}else if(parser.getArriveDelayTime() == 0) {  //제대로 도착
					context.getCounter(DelayCounters.SCHEDULED_ARRIVAL).increment(1);
				}else if(parser.getArriveDelayTime()<0) {  //조기도착
					context.getCounter(DelayCounters.EARLY_ARRIVAL).increment(1);
				}
			}else {  //도착 못한경우
				context.getCounter(DelayCounters.NOT_AVAILABLE_ARRIVAL).increment(1);
			}
		}
	
		/*
		정시 출발, 도착 및 조기 출발, 도착에 대한 것은 reduce로 넘겨주지 않았다.
		결과를 보면 System.out을 하지 않았지만 결과 과정창에 각각의 정시, 조기 값이 보인다. 
		이는 context에서 getCounter로 값을 불러옴과 동시에 increment로 값 증가를 시키는데, 한줄한줄 진행하면서 map을 지속 반복시키다가
		마지막에 다 더해진 값이 불려져서 그런 것 같다. 단순히 리포트형식으로만 출력
		*/
	}
	
}
