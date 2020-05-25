package UFO4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class YearReducer extends Reducer<Text, UFO, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<UFO> values, Context context) throws IOException, InterruptedException {
		//년도를 기준(키)으로 삼고 UFO값들이 밸류로서 묶여 들어오는데 정렬이 없고 Mapper에서 등록된 순서대로만 묶여서 들어온다.
		//UFOComparator를 만들어서 밸류에 있어 UFO객체 특정값 크기순으로 이차정렬이 되도록 유도할 것이다.
		String prev = null;
		int cnt = 0;
		for(UFO v : values) {
			String current = v.getCountry();
			if(prev == null || prev.equals(current)) {
				cnt++;
				prev=current;
			}else {
				String output = prev + ":" + cnt;
				context.write(key, new Text(output));
				
				cnt = 1;
				prev = current;
			}
		}
	}
}

/*
 1959  <us, circle> <gb, circle> <us, triangle> <au, cylinder> <gb, ractangular> 이런식으로 밸류는 뒤죽박죽 오게 된다.
main쪽에서 2차정렬을 하게 된다면
 1959  <au, circle> <au, cylinder> <gb, circle> <gb, cylinder> <us, triangle> <us, ~~>
 이런식으로 밸류가 정렬이 된다. 결국 국가별로 묶여있는 것과 비슷하게 된다.
 
 이제 위의 과정을 통해서 하나의 연도 속에서 국가별 UFO출현횟수를 구할 수 있게 된다.

 */
