package com.iamcarrot.samples.wordCounterSample;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class WordCounter extends BaseRichBolt {

    private Map<String, Integer> counters;

    private String fileName;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        String name = context.getThisComponentId();
        int id = context.getThisTaskId();
        this.fileName = stormConf.get("dirToWrite").toString() + "Output-" + name + id + ".txt";
    }

    public void execute(Tuple input) {

        String word = input.getStringByField("word");

        if (!counters.containsKey(word))
            counters.put(word, 0);

        counters.put(word, counters.get(word) + 1);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {

        try {
            PrintWriter writer = new PrintWriter(fileName, "UTF-8");

            counters.forEach((word, count) -> {
                writer.println(word + ": " + count);
            });

            writer.close();
        } catch (Exception e) {
            throw new RuntimeException("Error while writing output to file");
        }
    }
}
