package sources;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class SourceDemo extends RichSourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String>sourceContext) throws Exception {
        String[] logs = {
                "http://www.example.com/index1\tuser1\t2019-08-09 23:40:15",
                "http://www.example.com/index1\tuser2\t2019-08-09 23:42:50",
                "http://www.example.com/index1\tuser1\t2019-08-09 23:56:15",
                "http://www.example.com/index1\tuser3\t2019-08-09 23:57:15",
                "http://www.example.com/index1\tuser1\t2019-08-10 00:05:15",
                "http://www.example.com/index1\tuser2\t2019-08-10 00:06:15",
                "http://www.example.com/index2\tuser1\t2019-08-10 00:07:15",
                "http://www.example.com/index2\tuser1\t2019-08-10 00:06:15",
                "http://www.example.com/index6\tuser6\t2019-08-10 00:15:15"//这行记录只是为了让水印(Watermarks)到达2019-08-10 00:15:15，触发上一个窗口的计算
        };
        for (String log: logs) {
            sourceContext.collect(log);
        }
        while(isRunning) {
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
