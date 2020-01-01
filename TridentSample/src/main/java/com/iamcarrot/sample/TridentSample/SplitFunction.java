package com.iamcarrot.sample.TridentSample;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Arrays;

public class SplitFunction extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        String sentence = tuple.getString(0);
        Arrays.stream(sentence.split(" ")).forEach(word -> {
            String trimmed = word.trim();
            if (trimmed.isEmpty())
                return;
            collector.emit(new Values(trimmed));
        });
    }
}
