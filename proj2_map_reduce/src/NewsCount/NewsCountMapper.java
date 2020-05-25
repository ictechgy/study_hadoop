package NewsCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NewsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable(1);
	
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		
		StringTokenizer st = new StringTokenizer(value.toString().toUpperCase());  
		// 그냥 바로 ""를 기준으로 각각의 알파벳을 구분지을 수 있을 줄 알았는데 아니었음
		//StringTokenizer st = new StringTokenizer(value.toString().toUpperCase(), ""); 이렇게 했는데 안됐음  
		//StringTokenizer st = new StringTokenizer(value.toString().toUpperCase().join(" ")); 이것도 안된다. - 이건 다른 join인듯. 파이썬에서는
		//각각의 단어사이에 특정문자를 넣어줄 수 있었다. 특정문자를 넣고 그거를 다시 구분문자로 삼게 하려고 했는데..
		
		
		/*while(st.hasMoreTokens()) {
			outputKey.set(st.nextToken());
			context.write(outputKey, outputValue);
		}*/
		while(st.hasMoreTokens()) {
			String str = st.nextToken();
			char ch;
			for(int i = 0; i< str.length(); i++ ) {
				ch = str.charAt(i);
				String one = String.valueOf(ch);
				outputKey.set(one);
				context.write(outputKey, outputValue);
			}
			
		}
		
		
		/* 선생님이 하신 방식
		 char ch;
		 String str = value.toString().toLowerCase().replaceAll(" ", "");     - 공백 제거
		 
		 for(int i = 0; i<str.length(); i++{
		 	ch = str.charAt[i]
		 	
		 	if(ch>='a' && ch<='z'){
		 		outputKey.set(ch+"");
		 		context.write(outputKey, outputValue);
		 	}
		 }
		 */
		
		
		
	}
	
	
	
}
