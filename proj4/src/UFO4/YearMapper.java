package UFO4;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class YearMapper extends Mapper<LongWritable, Text, Text, UFO>{

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//년도별 UFO를 밸류로 등록
		
		String[] ar = value.toString().split(",");
		String year = ar[0].split("/")[2];
		context.write(new Text(year), new UFO(ar[3], ar[4]));
	}
	
}
