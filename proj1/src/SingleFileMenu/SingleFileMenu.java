package SingleFileMenu;

import java.io.IOException;
import java.util.Scanner;

public class SingleFileMenu {
	
	public static void main(String[] args) throws IOException {
		menu();
	}
	
	public static void menu() throws IOException {
		//선생님은 이 함수를 static으로 만들지 않고 main에서 SingleFileMenu의 객체 singleFileMenu만든 뒤
		//singleFileMenu.menu()작동되도록 만드심

		Scanner sc = new Scanner(System.in);
		System.out.println("1. 파일 읽기");
		System.out.println("2. 파일 저장");
		System.out.print("번호 입력 : ");
		int input = sc.nextInt();
	
		if(input==1) {  //파일 읽기
			SingleFileRead readObject = new SingleFileRead();
			readObject.read();
			
		}else if(input==2) {	//파일쓰기
			SingleFileWrite writeObject = new SingleFileWrite();
			writeObject.write();
			
			//또는 한줄로 써버릴 수도 있다.(일회성) - new SingleFileWrite().write();
			
		}else {
			System.out.println("잘못입력하셨습니다.");
		}
		
	}
	
}
