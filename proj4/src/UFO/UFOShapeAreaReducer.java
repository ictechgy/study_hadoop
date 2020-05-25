package UFO;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class UFOShapeAreaReducer extends Reducer<Text, Area, Text, IntWritable>{
	//들어오는 키값 Text는 shape이다. 각각의 shape가 같은 것들끼리 묶이고, 밸류로는 Area값들이 존재할 것이다.

	protected void reduce(Text key, Iterable<Area> values, Context context) throws IOException, InterruptedException{
		//맵에서 만들어낸 Area들이 같은 키로 묶여서 Tuple(배열과 비슷한) 로 들어옴
		int[] ar = new int[3];
		for(Area a: values) {
			String state = a.getState();
			switch(state) {
			case "tx":
				ar[0]++; break;
			case "ny":
				ar[1]++; break;
			default:
				ar[2]++; break;
			}
		}
		String out = ar[0] + "\t" + ar[1] + "\t" + ar[2];
		context.write(key, new Text(out));
	}
	
}






