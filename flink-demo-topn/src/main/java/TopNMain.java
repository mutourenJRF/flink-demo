import functions.TopNFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import sources.SourceDemo;

public class TopNMain {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String,Integer>>  data=env.addSource(new SourceDemo());

        DataStream<Tuple2<String,Integer>> wcount=data .keyBy(0) //按照Tuple2<String, Integer>的第一个元素为key，也就是单词
                 .window(SlidingProcessingTimeWindows.of(Time.seconds(20),Time.seconds(1)))
                //key之后的元素进入一个总时间长度为600s,每20s向后滑动一次的滑动窗口
                .sum(1);// 将相同的key的元素第二个count值相加

        DataStream<Tuple2<String,Integer>> ret = wcount
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                //所有key元素进入一个20s长的窗口（选20秒是因为上游窗口每20s计算一轮数据，topN窗口一次计算只统计一个窗口时间内的变化）
                .process(new TopNFunction(1));//计算该窗口TopN

        ret.print();
        System.out.println("**************");

        env.execute("start");
    }
}
