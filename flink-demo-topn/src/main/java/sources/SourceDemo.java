package sources;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SourceDemo extends RichSourceFunction<Tuple2<String,Integer>> {

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
        while(isRunning){
            int rand=(int)(Math.random()*100);
            sourceContext.collect(new Tuple2<String, Integer>(rand+"",rand));
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
