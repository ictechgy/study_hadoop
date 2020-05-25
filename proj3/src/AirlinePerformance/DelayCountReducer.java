package AirlinePerformance;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
MapReduce framework에서는 mapper에서 나온 값들에 있어서 같은 키값에 대해서는 밸류쪽에 [1, 1, 1 ]형식으로 모아준다.
key			value
1987,10		[1, 1, 1]
1987,11		[1, 1]
1987,12		[1]

총 레코드 수는 27개이다. (87년도 10월 ~ 12월, 88년도 12월치, 89년도 12월치값)
*/

public class DelayCountReducer extends Reducer<Text, IntWritable, Text,IntWritable>{
	private IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable data : values) {
			sum+=data.get();
		}
		result.set(sum);
		context.write(key, result);
	}
	
	

}
