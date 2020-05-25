package UFO4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class UFO implements WritableComparable<UFO>{
	//키값으로 쓸 객체를 만든다면 나중에 정렬을 해줘야 하므로 추가적으로 Comparable이 필요하다.

	private String country;
	private String shape;
	
	public UFO(String country, String shape) {
		this.country = country;
		this.shape = shape;
	}
	public UFO() {}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		country = in.readUTF();
		shape = in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(country);
		out.writeUTF(shape);
	}
	@Override
	public int compareTo(UFO o) {  //UFO객체끼리 비교했을 때 무엇이 작고 큰지 그 기준을 잡아주어야 한다.
		//그래야 이 UFO값을 가지고 키를 삼았을 때 키 정렬을 할 수 있다.
		
		//먼저 국가값을 기준으로 정렬을 한다.
		int r = country.compareTo(o.country);
		if(r!=0)
			return r;
		
		//국가값까지 같다면 모양값도 비교한다.
		return shape.compareTo(o.shape);
	
		//마이너스값을 반환한다는 것은 이 객체가 더 작다는 것을 의미하고 양수를 반환하면 비교하는 대상보다 큼을 의미한다.
	}
	
	public String toString() {
		return country + "\t" + shape;
	}


	public String getCountry() {
		return country;
	}


	public void setCountry(String country) {
		this.country = country;
	}


	public String getShape() {
		return shape;
	}


	public void setShape(String shape) {
		this.shape = shape;
	}
	

	
	
	
}
