package UFO2;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UFOMapper extends Mapper<LongWritable, Text, UFO, DoubleWritable>{

	private DoubleWritable outputValue = new DoubleWritable();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] arr = value.toString().split(",");
		UFO u = new UFO(arr[3],arr[4]);
		arr[5] = arr[5].replaceAll("[^0-9]", "");  //초값에 불순물이 있는 경우 지우기 위해 만든 구문. 정규표현식 사용
		
		try {
		outputValue.set(Double.parseDouble(arr[5]));
		context.write(u, outputValue);
		}catch(Exception e) {
		 //만약 잘못된 데이터가 존재한다면 해당 줄에 대해서는 아무 일도 일어나지 않도록 설정
		}
	}
	//초값에 정수가 아닌 0.02 같은 값도 있으며 2` 같은 값도 존재..
	

}
