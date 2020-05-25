package WordCount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> { //Mapper가 되려면 Mapper클래스를 상속받아야 한다.
	//Mapper클래스를 상속받을 때 데이터를 어떤 방식으로 입력받고 어떤 방식으로 출력할지 결정해야 한다. 그 부분은 제네릭으로 되어있으므로 해당 데이터타입을 기입해주면 된다.
	//우리는 입력도 Key, value 형태로 받을 것이고 출력도 Key, value형태로 출력할 것이다. 자료형 타입을 기입해줘야하는데 하둡에서는 데이터자리형에 대해 자체적 클래스를 만들어놓았다.
	
	// ~/jar/fruit.txt 파일을 읽어올 것인데 포맷은 기본포맷인 Text Input Format으로 입력받아올 것이며 형태는 LongWritable, Text 형식이다. offset값과 밸류값
	// 출력을 할 때에는 각각의 요소가 몇개씩 나왔는지 변환하여 내보낼 것이므로 Text, IntWritable 이다.
	
	
	//입력은 알아서 들어오므로 우리는 출력쪽만 신경써주면 된다.
	private Text outputKey = new Text();
	private IntWritable outputValue = new IntWritable(1);  //1로 자동되도록 세팅
	
	//setup메소드가 있는데 이건 딱 한번만 자동실행된다.
	
	@Override   //HDFS의 파일에서 한줄한줄 읽어들일때마다 map실행. 그 한줄의 시작 offset을 key로 받아오고 값을 Value로 받아온다.
	protected void map(LongWritable key, Text value, Context context)  //내가 Mapper이므로 Context를 그냥 받아와도 된다.
			throws IOException, InterruptedException {
		//super.map(key, value, context); 부모의 map 메소드 실행시킬 것 아니고 오버라이드해서 쓸 것임
		
		//그런데 텍스트의 파일을 읽어올 때 한 줄에 여러 값이 존재할 수 있고 그러면 value에 여러 값이 뭉쳐있을 수 있다. 이것을 분리해주어야 한다.
		//그냥 기본적 map 메소드를 쓰기만 한다면 단순히 줄마다 키:밸류 로 묶어놓은 것을 그대로 그냥 바로 Reducer로 출력해버릴것이다.
		StringTokenizer st = new StringTokenizer(value.toString());
		//Text로 들어온 value를 String으로 바꾸기. 그리고 공백을 기준으로 분리하며 각각의 문자열을 토큰화한다. split()을 사용하여도 된다.
		
		while(st.hasMoreTokens()) {  //각각의 문자열 토큰이 남아있다면
			outputKey.set(st.nextToken()); //그 토큰을 꺼내와서 Key로 세팅
			context.write(outputKey, outputValue);   //Map형태로 세팅한다음에 Context에 쓴 뒤 출력
		}
	}
	
	
	
	
}
