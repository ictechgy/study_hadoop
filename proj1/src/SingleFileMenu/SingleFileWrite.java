package SingleFileMenu;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleFileWrite {
	
	public void write() throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		
		Scanner sc = new Scanner(System.in);
		
		System.out.print("파일명 입력 : ");
		String filename = sc.next();
		System.out.print("메시지 입력 : ");
		String msg = sc.next();
		
		Path path = new Path(filename);
		if(hdfs.exists(path)) {
			System.out.println("hdfs상에 해당 파일이 이미 존재합니다. 해당 파일을 먼저 삭제합니다.");
			hdfs.delete(path, true);
		}
		
		FSDataOutputStream outputStream = hdfs.create(path);
		outputStream.writeUTF(msg);
		outputStream.close();
		
		
	}

}
