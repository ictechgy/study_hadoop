package SingleFileWriteRead;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class SingleFileWriteRead {

	public static void main(String[] args) throws IOException {
		if(args.length!=2) {  //메인에 인자 2개가 들어오지 않는다면..
			System.out.println("usage error");
			System.exit(0);
		}
		//C언어의 main에서는 int argc, char* argv[](또는 char** argv)가 있었으며 argv[0]에는 무조건 파일의 경로에 대한  문자열주소가 존재
		
		//HDFS 환경 불러오기
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);  //파일단위로 움직이므로 IOException발생
		
		//path setting. 프로그램 실행시 첫번째로 들어오는 인자를 경로로 잡겠다.
		Path path = new Path(args[0]);
		
		//if parh already exitsts, delete
		if(hdfs.exists(path)) {   //hdfs공간에 이미 path에 해당하는 파일이 있다면 지워라! (hdfs는 덮어쓰기를 못한다)
			hdfs.delete(path, true);
		}
		
		//HDFS file output stream create - HDFS공간에 파일을 생성
		FSDataOutputStream outputStream = hdfs.create(path);
		//HDFS 공간에 path에 해당하는 파일을 만들고 그 파일에 대해 출력Stream 생성
		
		//file write
		outputStream.writeUTF(args[1]);  //args[1]의 내용을 파일에다가 써라(UTF방식으로)
		outputStream.close();
		
		
		
		//HDFS file input stream create
		FSDataInputStream inputStream = hdfs.open(path);
		
		//file read
		String result = inputStream.readUTF();
		inputStream.close();
		
		System.out.println("결과 : "+result);
	
	}
	

}
