package AirlinePerformanceSecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey,IntWritable>{
	private MultipleOutputs<DateKey, IntWritable> mos;  
	
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey,IntWritable>(context);
	}
	
	@Override
	protected void reduce(DateKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		String[] columns = key.getYear().split(",");  //"D, 1987" -> "D", "1987"
		
		int sum=0;
		Integer bMonth = key.getMonth();
		
		String outputFile = columns[0].equals("D") ? "departure" : "arrival";
		
		for(IntWritable data : values) {
			if(bMonth!=key.getMonth()) {
				result.set(sum);
				outputKey.setYear(columns[1]);
				outputKey.setMonth(bMonth);
				mos.write(outputFile, outputKey, result);
				bMonth=key.getMonth();
				sum=0;
			}
			sum+=data.get();
		}
		if(key.getMonth() == bMonth) {   //마지막월에 대한 처리. 12월
			result.set(sum);
			outputKey.setYear(columns[1]);
			outputKey.setMonth(bMonth);
			mos.write(outputFile, outputKey, result);
		}
	}
	
}
/* 
  기존에는 리듀서가 27개였다.
  1987,10    1,1,1,1,1
  1987,11    1,1,1,1,1
  등..
  
  이제는 리듀서 3개로 우리가 직접 만들어놓았다. - 그룹화하는 클래스와 이 클래스를 메인에서 실행만 하게 해주면 이렇게 매퍼에서 넘어온다.(프레임워크에서 수행)
  1987		10,1,10,1,10,1,11,1,11,1
  1988
  1989
  이렇게 처리가 된다. 
  사실 월은 키값인데 밸류와 같이 묶여보이는 듯한 상황
*/