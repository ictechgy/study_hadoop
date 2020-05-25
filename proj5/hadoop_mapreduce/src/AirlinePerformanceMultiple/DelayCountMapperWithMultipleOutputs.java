package AirlinePerformanceMultiple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class DelayCountMapperWithMultipleOutputs extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text outputKey = new Text();
	private static final IntWritable outputValue = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(parser.isDepartureDelayAvailable()) {
			if(parser.getDepartureDelayTime()>0) { 
				outputKey.set("D,"+parser.getYear()+","+parser.getMonth());
				context.write(outputKey, outputValue);  
			}else if(parser.getDepartureDelayTime() == 0) { 
				context.getCounter(DelayCounters.SCHEDULED_DEPARTURE).increment(1);
			}else if(parser.getDepartureDelayTime()<0) {  
				context.getCounter(DelayCounters.EARLY_DEPARTURE).increment(1);
			}
		}else {
			context.getCounter(DelayCounters.NOT_AVAILABLE_DEPARTURE).increment(1);
		}
		// ---------------------------------------
		if(parser.isArriveDelayAvailable()) {
			if(parser.getArriveDelayTime()>0) {  
				outputKey.set("A,"+parser.getYear()+","+parser.getMonth());
				context.write(outputKey, outputValue); 
			}else if(parser.getArriveDelayTime() == 0) {  
				context.getCounter(DelayCounters.SCHEDULED_ARRIVAL).increment(1);
			}else if(parser.getArriveDelayTime()<0) {  
				context.getCounter(DelayCounters.EARLY_ARRIVAL).increment(1);
			}
		}else {
			context.getCounter(DelayCounters.NOT_AVAILABLE_ARRIVAL).increment(1);
		}
		
	}
	
}
