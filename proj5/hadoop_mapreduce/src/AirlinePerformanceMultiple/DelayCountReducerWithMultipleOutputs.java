package AirlinePerformanceMultiple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class DelayCountReducerWithMultipleOutputs extends Reducer<Text, IntWritable, Text,IntWritable>{
	private MultipleOutputs<Text, IntWritable> mos;  //null값 초기화
	//여러개의 output을 만들기 위한 변수
	
	private Text outputKey = new Text();
	private IntWritable result = new IntWritable();
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text,IntWritable>(context);
	}//여기서 mos변수에 대한 실질적 객체 생성
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		/*
		 아래와 같은 형식으로 데이터가 들어온다.
		 D,1987,10    [1, 1, 1]
		 A,1987,10	  [1, 1, 1, 1]
		 */
		String[] columns = key.toString().split(",");
		outputKey.set(columns[1]+","+columns[2]);
		
		if(columns[0].equals("D")) {
			int sum = 0;
			for(IntWritable data : values) {
				sum+=data.get();
			}
			result.set(sum);
			//context.write(key, result);  context로 내보내면 part-r-00000로 나가게 됨
			mos.write("departure", outputKey, result);  //파일명, 키값, 밸류값
		}else if(columns[0].equals("A")) {
			int sum = 0;
			for(IntWritable data : values) {
				sum+=data.get();
			}
			result.set(sum);
			mos.write("arrival", outputKey, result);  
		}
	}
	
	
}
