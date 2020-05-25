package WordCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{ //Reducer클래스 상속받아야 함. Mapper에서 출력한 데이터형태를 이곳에서는 입력으로 받는다.
																	//그리고 각각의 키에 해당하는 것들을 합한다음에 내보낼 것이다.
	
	//MapReduce프레임워크가 같은 키들을 묶어서 이 Reduce에 보내준다. apple : [1, 1, 1, 1] 이런 형태로
	//이제 합산을 해서 내보내줘야 한다. 키값은 그대로이고 밸류값만 합산하여 내보내주면 된다.
	
	private IntWritable result = new IntWritable();

	//setup은 한번만 발동하며 reduce는 key값(과일종류)마다 발동하게 된다.
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,  //키값은 상관없는데 밸류가 [1, 1, 1, 1] 이런 형태로 들어오므로 각각의 값에 접근하기 위한 반복자 제공
			Context context) throws IOException, InterruptedException {
		int sum = 0;
		for(IntWritable data : values) {  //values에 있는 [1, 1, 1, 1] 요소 하나하나씩을 data에 대입. 각각의 요소 자체도 IntWritable이다.
			sum += data.get();
		}//for each
		
		result.set(sum);
		context.write(key, result);
	}
	
	
	
}
