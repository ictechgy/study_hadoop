package UFO3;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AreaComparator extends WritableComparator{
	public AreaComparator() {
		
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		Area aa = (Area)a;
		Area bb = (Area)b;
		return aa.getState().compareTo(bb.getState());
	}
	//Mapper에는 shape값을 키값으로 하고, Area(state와 city)값을 밸류로 하는 키-밸류 쌍 처리를 진행한다.
	//FrameWork에서는 이 shape값들끼리 묶어서 shape - [Area, Area, Area..] 로 묶어서 Reducer 에게 넘겨준다. 이 때 이 밸류값들을 정렬하게 된다.
	//다만 Area객체들끼리도 비교가능하도록 WritableComparable되어있어야 한다.
	
}
