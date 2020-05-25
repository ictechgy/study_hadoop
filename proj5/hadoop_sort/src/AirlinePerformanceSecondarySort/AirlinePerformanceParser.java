package AirlinePerformanceSecondarySort;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {  
	//한줄에 해당하는 데이터를 읽어온다면 그 한줄데이터에서 필요한 값들만 추출해서 이 객체의 멤버변수에 저장시킬 것이다. 

	private int year;
	private int month;
	private int arriveDelayTime = 0;
	private int departureDelayTime = 0;
	private int distance = 0;  //나중에 거리값을 가지고 검색도 가능?
	private boolean arriveDelayAvailable = true;
	private boolean departureDelayAvailable = true;    //도착 또는 출발지연의 가능성
	private boolean distanceAvailable = true;
	private String uniqueCarrier;
	
	
	public AirlinePerformanceParser(Text text) {  //객체를 생성하면서 생성자의 매개변수로 파일의 한 줄이 들어온다면
//Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
//1987,10,14,3,741,730,912,849,PS,1451,NA,91,79,NA,23,11,SAN,SFO,447,NA,NA,0,NA,0,NA,NA,NA,NA,NA
		String[] columns = text.toString().split(",");
		
		year = Integer.parseInt(columns[0]);
		month = Integer.parseInt(columns[1]);
		uniqueCarrier = columns[8];
		
		if(columns[15].equals("NA")) {  //15번째 인덱스에는 출발지연에 대한 값이 들어있다. 만약 값이 NA라면 출발이나 도착에 대한 것이 없다는 것(기상악화 등) - 따라서 지연정보도 존재하지 않음
			departureDelayAvailable = false;
		}else {
			departureDelayTime = Integer.parseInt(columns[15]);
		}
		
		if(columns[14].equals("NA")) {
			arriveDelayAvailable = false;
		}else {
			arriveDelayTime = Integer.parseInt(columns[14]);
		}
		//만약 값이 +값이라면 지연됐다는 것이고 - 라면 일찍 출발했다는 것. 0은 정시출발/도착
		
		if(columns[18].equals("NA")) {
			distanceAvailable = false;
		}else {
			distance = Integer.parseInt(columns[18]);
		}

	}


	public int getYear() {
		return year;
	}


	public void setYear(int year) {
		this.year = year;
	}


	public int getMonth() {
		return month;
	}


	public void setMonth(int month) {
		this.month = month;
	}


	public int getArriveDelayTime() {
		return arriveDelayTime;
	}


	public void setArriveDelayTime(int arriveDelayTime) {
		this.arriveDelayTime = arriveDelayTime;
	}


	public int getDepartureDelayTime() {
		return departureDelayTime;
	}


	public void setDepartureDelayTime(int departureDelayTime) {
		this.departureDelayTime = departureDelayTime;
	}


	public int getDistance() {
		return distance;
	}


	public void setDistance(int distance) {
		this.distance = distance;
	}


	public boolean isArriveDelayAvailable() {
		return arriveDelayAvailable;
	}


	public void setArriveDelayAvailable(boolean arriveDelayAvailable) {
		this.arriveDelayAvailable = arriveDelayAvailable;
	}


	public boolean isDepartureDelayAvailable() {
		return departureDelayAvailable;
	}


	public void setDepartureDelayAvailable(boolean departureDelayAvailable) {
		this.departureDelayAvailable = departureDelayAvailable;
	}


	public boolean isDistanceAvailable() {
		return distanceAvailable;
	}


	public void setDistanceAvailable(boolean distanceAvailable) {
		this.distanceAvailable = distanceAvailable;
	}


	public String getUniqueCarrier() {
		return uniqueCarrier;
	}


	public void setUniqueCarrier(String uniqueCarrier) {
		this.uniqueCarrier = uniqueCarrier;
	}
	
	
	
	
}
