package AirlinePerformance;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartureDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	//파일의 한 줄을 읽어오고 offset값은 버린 뒤 한줄레코드값은 AirlinePerformanceParser객체에 저장시킨다. 그리고 객체에 담긴 값 중에서 
	//년도와 일을 새로운 키값으로 지정하고 지연이 된 경우 그 밸류값을 1로 만들 것이다.
	
	private Text outputKey = new Text();
	private static final IntWritable outputValue = new IntWritable(1);  //static final 은 정적상수를 만드는 예약어
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//key에는 파일의 각 줄에 대한 offset값, value는 그 한줄의 값
		//한 줄을 읽을 때마다 map메소드 실행 - 콜백메소드
		
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		if(parser.getDepartureDelayTime()>0) {  //출발지연값이 유효하다면(NA가 아니고 -값도 아니며 정시출발인 0값도 아니라면)
			outputKey.set(parser.getYear()+","+parser.getMonth());
			context.write(outputKey, outputValue);
		}
	
	}
	
	
}
/*
이 mapper를 통해서는 아래와 같은 키 - 밸류 쌍이 반환된다.
key			value
1987,10		1
1987,10		1
1987,10		1
1987,11		1
1987,11		1
1987,12		1


*/