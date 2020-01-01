package com.iamcarrot.samples.wordCounterSample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-counter", new WordCounter(), 2)
//                .shuffleGrouping("word-reader");
//                .fieldsGrouping("word-reader", new Fields("word"));
//                .allGrouping("word-reader");
                .customGrouping("word-reader", new AlphaGrouping());

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "X:\\1. Projects\\Java\\personal\\ApacheStorm-Sample\\WordCounterSample\\input.txt");
        conf.put("dirToWrite", "X:\\1. Projects\\Java\\personal\\ApacheStorm-Sample\\WordCounterSample\\output\\");

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Word-Counter-Topology", conf, topology);
            Thread.sleep(10000);
        } catch (Exception ignored) {

        } finally {
            cluster.shutdown();
        }
    }

}
