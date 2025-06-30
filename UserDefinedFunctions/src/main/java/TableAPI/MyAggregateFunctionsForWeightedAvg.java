package TableAPI;

import org.apache.flink.table.functions.AggregateFunction;
import utils.WeightedAvgAccumulator;

public class MyAggregateFunctionsForWeightedAvg extends AggregateFunction<Long, WeightedAvgAccumulator> {
    @Override
    public Long getValue(WeightedAvgAccumulator acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum / acc.count;
        }
    }

    @Override
    public WeightedAvgAccumulator createAccumulator() {
        return new WeightedAvgAccumulator();
    }

    public void accumulate(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccumulator acc, Long iValue, Integer iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }

    public void merge(WeightedAvgAccumulator acc, Iterable<WeightedAvgAccumulator> it) {
        for (WeightedAvgAccumulator a : it) {
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }

    public void resetAccumulator(WeightedAvgAccumulator acc) {
        acc.count = 0;
        acc.sum = 0L;
    }
}
