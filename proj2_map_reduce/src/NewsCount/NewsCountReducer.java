package NewsCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NewsCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable data : values) {
			sum+=data.get();
		}
		result.set(sum);
		context.write(key, result);
	}
	

}
