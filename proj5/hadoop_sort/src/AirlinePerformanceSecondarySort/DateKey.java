package AirlinePerformanceSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

//복합키
//년도를 그룹키로서 생성 및 사용하고 월값을 기준으로 sort할 것이다.
public class DateKey implements WritableComparable<DateKey>{
	//Comparable은 기존의 비교기준을 쓰고자 할때, Comparator는 새로운 비교기준을 정해서 비교시키고자 할 때 사용한다.
	private String year;
	private Integer month;
	
	public DateKey() {}  //기본생성자는 필수이다. 리듀서에서 키값 객체를 만들고 데이터를 입력받을 준비를 하기 위해 필요

	public DateKey(String year, Integer month) {
		this.year = year;
		this.month = month;
	}
	
	//String은 Serializable 과 Comparable 인터페이스를 implements하였다.
	//네트워크를 통해 데이터를 보낼 때는 직렬화해야하며 네트워크를 통해 데이터를 받을 때에는 쪼개진 데이터를
	//다시 뭉쳐야하므로 역직렬화가 이루어져야 한다.
	//이전에 Text값과 같은 기본자료형으로 데이터 송수신을 할 때에는 안에 이미 직렬화방식이 구현되어있었기 때문에 그냥 쓸 수 있었다.

	@Override
	public void readFields(DataInput in) throws IOException {
		//readFields : 역직렬화에 사용되는 메소드
		//입력스트립에서 키값을 조회하여 멤버변수에 저장
		year = WritableUtils.readString(in);
		month = in.readInt();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//write : 직렬화에 사용되는 메소드
		//멤버변수의 키값을 출력스트림에 출력
		WritableUtils.writeString(out, year);
		out.writeInt(month);
		
	}

	@Override
	public int compareTo(DateKey key) {  //각각의 년도값들 묶인 상태에서 비교하고자 할 때
		int result = year.compareTo(key.year);
		if(result == 0)
			result = month.compareTo(key.month);
		
		return result;
	}
	//비교에 대한 기준 정의

	//객체를 그냥 출력하고자 할 때 기본적으로 클래스이름과 16진수 레퍼런스값이 나오는데 이것을 바꿔주고자..
	@Override
	public String toString() {
		//매퍼와 리듀서에서 복합키의 toString()을 호출해서 값을 출력 및 사용하고자 할 때 쓸 수 있다.
		return year+","+month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Integer getMonth() {
		return month;
	}

	public void setMonth(Integer month) {
		this.month = month;
	}
	
	
	
	
	
}
