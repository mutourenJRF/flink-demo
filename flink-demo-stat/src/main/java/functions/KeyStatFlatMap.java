package functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class KeyStatFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private ValueState<Tuple2<Long, Long>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<Tuple2<Long, Long>>("", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }), Tuple2.of(0L, 0L));

        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
        Tuple2<Long, Long> currentSum = state.value();
        System.out.println("currentSum: " + currentSum + "  value: " + value);
        currentSum.f0 += 1;
        currentSum.f1 += value.f1;

        state.update(currentSum);
        System.out.println("currentSum: " + currentSum );
        System.out.println("*************************** " );

//        if (currentSum.f0 >= 2) {
//        out.collect(Tuple2.of(value.f0, currentSum.f1 / currentSum.f0));
//        state.clear();
//        }
    }
}