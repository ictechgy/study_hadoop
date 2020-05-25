package UFO2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UFOReducer extends Reducer<UFO, DoubleWritable, UFO, DoubleWritable>{

	private DoubleWritable result = new DoubleWritable();
	
	@Override
	protected void reduce(UFO u, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double sum = 0;
		int cnt = 0;
		for(DoubleWritable time : values) {
			sum+=time.get();
			cnt++;
		}
		result.set(sum/cnt);
		context.write(u, result);
		
		
	}
	
	

}
