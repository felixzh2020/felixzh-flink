import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class MyProcess<T> extends ProcessAllWindowFunction<T, List<T>, TimeWindow> {
    @Override
    public void process(ProcessAllWindowFunction<T, List<T>, TimeWindow>.Context context, Iterable<T> iterable, Collector<List<T>> collector) throws Exception {
        List<T> elements = new ArrayList<>();
        iterable.forEach(elements::add);
        collector.collect(elements);
    }
}
