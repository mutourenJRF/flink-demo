import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PVFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {
    private ValueState<Long> valueState;
    private StateTtlConfig ttlConfig;
    @Override
    public void process(String s, Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
        System.out.println(s);

        long time=context.window().getStart();
        String day = new SimpleDateFormat("yyyyMMdd").format(new Date(time));

        ValueStateDescriptor<Long> pvDes = new ValueStateDescriptor(day + "pv", Long.class);
        pvDes.enableTimeToLive(ttlConfig);
        valueState=context.globalState().getState(pvDes);

        if(valueState.value()==null){
            valueState.update(0L);
        }


        long cnt=valueState.value();
        for (String log: iterable) {
            cnt++;
        }

        valueState.update(cnt);

        collector.collect(day+"\t"+s+"\t"+valueState.value());


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ttlConfig = StateTtlConfig
                .newBuilder(Time.days(1))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                .cleanupInRocksdbCompactFilter(1000L)
                .build();
    }

}
