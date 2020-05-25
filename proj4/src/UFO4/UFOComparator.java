package UFO4;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class UFOComparator extends WritableComparator{
	
	public UFOComparator() {
		super(UFO.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		UFO aa = (UFO)a;
		UFO bb = (UFO)b;
		if (!aa.getCountry().equals(bb.getCountry()))
			return aa.getCountry().compareTo(bb.getCountry());
		
		return aa.getShape().compareTo(bb.getShape());
	}

	
	
}
