package com.iamcarrot.sample.messageReliabilitySample;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomFailureBolt extends BaseRichBolt {

    // setting up a failure rate of 60%
    private static final int SUCCESS_RATE = 3;
    private OutputCollector collector;

    private Random random;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.random = new Random();
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        int r = random.nextInt(10);
        if (r >= SUCCESS_RATE) {
            collector.fail(input);
            return;
        }

        collector.emit(input, new Values(input.getStringByField("word")));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
