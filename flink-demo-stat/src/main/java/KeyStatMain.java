import functions.KeyStatFlatMap;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyStatMain {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple2<Long,Long>, Tuple> data=env.fromElements(
                Tuple2.of(1L, 3L)
                , Tuple2.of(1L, 5L)
                , Tuple2.of(1L, 7L)
                , Tuple2.of(1L, 4L)
                , Tuple2.of(1L, 4L)
                , Tuple2.of(1L, 4L)
                , Tuple2.of(1L, 4L)
                , Tuple2.of(1L, 4L)
                , Tuple2.of(1L, 4L)
                , Tuple2.of(1L, 2L))
            .keyBy(0);

//        data.print();

        data.flatMap(new KeyStatFlatMap())
            .print();

        env.execute("ManagerKeyedState");
    }



}
