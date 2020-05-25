package SingleFileMenu;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleFileRead {

	public void read() throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Scanner sc = new Scanner(System.in);
		System.out.print("파일명 입력 : ");
		String filename = sc.next();
		Path path = new Path(filename);
		FSDataInputStream inputStream = hdfs.open(path);
		
		if(inputStream==null) {
			System.out.println("파일이 존재하지 않습니다.");
		}else {
			String result = inputStream.readUTF();
			inputStream.close();
			System.out.println(result);
		}
		
		/*  선생님은 아래와 같이 하셨다.
		 *	 테스트해보니 내가 한 방식은 파일이 존재하지 않을 때 오류가 난다. File Does Not Exist
		    따라서 해당 path파일을 open하기 전 hdfs.open(path) 하기 전에 먼저 파일이 존재하는지를 확인해야 할 듯
		
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Scanner sc = new Scanner(System.in);
		System.out.println("파일명 입력 : ");
		String filename = sc.next();
		Path path = new Path(filename);
		
		if(!hdfs.exists(path)){
			System.out.println("파일이 존재하지 않습니다.");
			return;
		}else{
		
			FSDataInputStream inputStream = hdfs.open(path);
			String result = inputStream.readUTF();
			inputStream.close();
			System.out.println(result);
		}
		
		*/
		
		
	}
	
}
