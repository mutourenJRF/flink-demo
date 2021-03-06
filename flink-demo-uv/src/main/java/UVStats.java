import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sources.SourceDemo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class UVStats {

    public static final String DELIM = "\t";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);


        DataStreamSource<String> data=env.addSource(new SourceDemo());


        data
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    public long extractAscendingTimestamp(String s) {
                        String dateTime = s.split(DELIM, -1)[2];
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        return LocalDateTime.parse(dateTime, formatter).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                })
                .keyBy(new KeySelector<String, String>() {
                    public String getKey(String s) {
                        String url = s.split(DELIM, -1)[0];
                        return url;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new UVFunction())
                .print();

        env.execute();
    }
}