package UFO3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Area implements Writable{
	
	private String state;
	private String city;

	@Override
	public void readFields(DataInput in) throws IOException {
		//byte to Area
		state = in.readUTF();
		city = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(state);
		out.writeUTF(city);
	}
	
	public Area(String state, String city) {
		this.state = state;
		this.city = city;
	}
	
	public String toString() {
		return state + "\t" + city;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	
	
}
