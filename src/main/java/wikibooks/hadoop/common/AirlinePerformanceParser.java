package wikibooks.hadoop.common;

import org.apache.hadoop.io.Text;

public class AirlinePerformanceParser {
    private int year;
    private int month;

    private int arriveDelayTime = 0;
    private int departureDelayTime = 0;
    private int distance = 0;

    private boolean arriveDelayAvailable = true;
    private boolean departureDelayAvailable = true;
    private boolean distanceAvailable = true;

    private String uniqueCarrier;

    public AirlinePerformanceParser(Text text) {
        try {
            String[] columns = text.toString().split(",");

            // 운항 연도 설정
            year = Integer.parseInt(columns[0]);

            // 운항 월 설정
            month = Integer.parseInt(columns[1]);

            // 항공사 코드 설정
            uniqueCarrier = columns[8];

            // 항공기 출발 지연 시간 설정
            if (!columns[15].equals("NA")) {
                departureDelayTime = Integer.parseInt(columns[15]);
            } else {
                departureDelayAvailable = false;
            }

            // 항공기 도착 지연 시간 설정
            if (!columns[14].equals("NA")) {
                arriveDelayTime = Integer.parseInt(columns[14]);
            } else {
                arriveDelayAvailable = false;
            }

            // 운항 거리 설정
            if (!columns[18].equals("NA")) {
                distance = Integer.parseInt(columns[18]);
            } else {
                distanceAvailable = false;
            }
        } catch (Exception e) {
            System.out.println("Error parsing a record: " + e.getMessage());
        }
    }

    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getArriveDelayTime() {
        return arriveDelayTime;
    }

    public int getDepartureDelayTime() {
        return departureDelayTime;
    }

    public int getDistance() {
        return distance;
    }

    public boolean isArriveDelayAvailable() {
        return arriveDelayAvailable;
    }

    public boolean isDepartureDelayAvailable() {
        return departureDelayAvailable;
    }

    public boolean isDistanceAvailable() {
        return distanceAvailable;
    }

    public String getUniqueCarrier() {
        return uniqueCarrier;
    }
}
