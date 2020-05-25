package UFO;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UFOShapeAreaMapper extends Mapper<LongWritable, Text, Text, Area> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] ar = value.toString().split(",");
		Area a = new Area(ar[2], ar[1]);   //데이터 중에서 state값과 city값만을 취하기
		context.write(new Text(ar[4]), a);  //shape를 키값으로 삼고 state와 city가 있는 Area자료형을 밸류로 삼아서 내보내기
	}
}
