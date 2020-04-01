import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sources.SourceDemo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class PvState {

    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(1000L);

        env.addSource(new SourceDemo())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String s) {
                        String dateTime = s.split(UVStats.DELIM, -1)[2];
                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        return LocalDateTime.parse(dateTime, formatter).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                })
                .keyBy(x->x.split(UVStats.DELIM, -1)[0])
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                .process(new PVFunction())
                .print();

        env.execute("start");
    }
}
