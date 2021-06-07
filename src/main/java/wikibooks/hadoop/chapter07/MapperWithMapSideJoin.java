package wikibooks.hadoop.chapter07;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wikibooks.hadoop.common.AirlinePerformanceParser;
import wikibooks.hadoop.common.CarrierCodeParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Hashtable;

public class MapperWithMapSideJoin extends Mapper<LongWritable, Text, Text, Text> {

    private Hashtable<String, String> joinMap = new Hashtable<>();

    // 맵 출력키
    private Text outputKey = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            // 분산 캐시 조회
            URI[] cacheFiles = context.getCacheFiles();

            // 조인 데이터 생성
            if (cacheFiles != null && cacheFiles.length > 0) {
                String line;
                FileSystem fs = FileSystem.get(context.getConfiguration());
                Path getFilePath = new Path(cacheFiles[0].toString());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));
                try {
                    while ((line = br.readLine()) != null) {
                        CarrierCodeParser codeParser = new CarrierCodeParser(line);
                        joinMap.put(codeParser.getCarrierCode(), codeParser.getCarrierName());
                    }
                } finally {
                    br.close();
                }
            } else {
                System.out.println("cache files is null!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
        outputKey.set(parser.getUniqueCarrier());
        context.write(outputKey, new Text(joinMap.get(parser.getUniqueCarrier()) + "\t" + value.toString()));
    }
}
