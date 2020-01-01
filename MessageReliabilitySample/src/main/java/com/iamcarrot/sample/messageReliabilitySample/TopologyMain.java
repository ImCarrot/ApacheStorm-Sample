package com.iamcarrot.sample.messageReliabilitySample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new ReliableWordReaderSpout());
        builder.setBolt("word-counter", new RandomFailureBolt(), 2)
                .shuffleGrouping("word-reader");

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToRead", "X:\\1. Projects\\Java\\personal\\ApacheStorm-Sample\\WordCounterSample\\input.txt");

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
