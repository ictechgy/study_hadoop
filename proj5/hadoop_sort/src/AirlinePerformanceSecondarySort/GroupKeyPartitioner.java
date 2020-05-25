package AirlinePerformanceSecondarySort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

//맵 태스크의 출력 데이터를 리듀스 태스크의 입력 데이터로 보낼지 결정
//파티셔닝된 데이터는 맵 태스크의 출력 데이터의 키의 값에 따라 정렬된다.
//구현하는 그룹키 파티셔너는 그룹핑키로 사용하는 연도에 대한 파티셔닝을 수행
//즉 매퍼에서 출력되어 나오는 키값(연도값)에 따라 이 파티셔너에서는 분류를 해주라는 것
public class GroupKeyPartitioner extends Partitioner<DateKey, IntWritable>{  //매퍼에서 출력되어 나오는 데이터형에 대해 작성

	@Override
	public int getPartition(DateKey key, IntWritable val, int numPartitions) {
		int hash = key.getYear().hashCode();  //해시값 가져오기
		int partition = hash%numPartitions;
		return partition;
		//연도가 같다면 해시값도 같을 것
	}
	
}
