package com.iamcarrot.personal.stormFinanceExample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class TopologyMain {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-Finance-Spout", new yfSpout());
        builder.setBolt("Yahoo-Finance-Bolt", new yfBolt()).shuffleGrouping("Yahoo-Finance-Spout");

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setDebug(true);
        conf.put("fileToWrite", "X:\\1. Projects\\Java\\personal\\stormFinanceExample\\output.txt");

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Stock-Tracker-Topology", conf, topology);
            Thread.sleep(10000);
        } catch (Exception ignored) {

        } finally {
            cluster.shutdown();
        }
    }

}
