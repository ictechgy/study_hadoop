package AirlinePerformanceSecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//리듀서는 그룹키 비교기를 사용해 같은 연도에 해당하는 모든 데이터를 하나의 Reducer그룹에서 처리
//복합키 비교기가 연도와 월을 비교했다면 그룹키 비교기는 연도만 비교한다.
public class GroupKeyComparator extends WritableComparator{
	public GroupKeyComparator() {
		super(DateKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		DateKey k1 = (DateKey)a;
		DateKey k2 = (DateKey)b;
		
		return k1.getYear().compareTo(k2.getYear());
	}
	
	
	
}
