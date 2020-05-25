package AirlinePerformanceSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//복합키대 복합키 비교하고자 할 때 이 비교기를 사용한다.
public class DateKeyComparator extends WritableComparator{  
	public DateKeyComparator() {
		super(DateKey.class, true);  //DateKey를 비교하겠다
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		//
		DateKey k1 = (DateKey) a;
		DateKey k2 = (DateKey) b;
		
		int comp = k1.getYear().compareTo(k2.getYear());
		if(comp!=0) return comp;
		
		return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth()<k2.getMonth() ? -1 : 1);
	}
	
	
}
