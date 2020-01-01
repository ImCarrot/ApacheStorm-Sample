package com.iamcarrot.sample.TridentSample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class TridentMain {

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();

        topology.newStream("lines", new WordReader())
                .each(new Fields("word"), new SplitFunction(), new Fields("word_split"))
                .groupBy(new Fields("word_split"))
                .aggregate(new Count(), new Fields("count"))
                .each(new Fields("word_split", "count"), new Debug());

        Config conf = new Config();
        conf.setDebug(false);
        conf.put("fileToRead", "X:\\1. Projects\\Java\\personal\\ApacheStorm-Sample\\TridentSample\\sentences.txt");

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Trident-Topology", conf, topology.build());
            Thread.sleep(10000);
        } catch (Exception ignored) {

        } finally {
            cluster.shutdown();
        }
    }

}
