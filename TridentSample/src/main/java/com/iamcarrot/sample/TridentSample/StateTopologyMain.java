package com.iamcarrot.sample.TridentSample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;

public class StateTopologyMain {

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();

        TridentState wordCount = topology.newStream("lines", new WordReader())
                .each(new Fields("word"), new SplitFunction(), new Fields("word_split"))
                .groupBy(new Fields("word_split"))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));

        LocalDRPC drpc = new LocalDRPC();

        topology.newDRPCStream("count", drpc)
                .stateQuery(wordCount, new Fields("args"), new MapGet(), new Fields("count"));

        Config conf = new Config();
        conf.setDebug(false);
        conf.put("fileToRead", "X:\\1. Projects\\Java\\personal\\ApacheStorm-Sample\\TridentSample\\sentences.txt");

        LocalCluster cluster = new LocalCluster();

        try {
            cluster.submitTopology("Trident-Topology", conf, topology.build());
            Thread.sleep(10000);
            for (String word : new String[]{"you", "carrot", "do"}) {
                System.out.println("Result for: " + word + ": " + drpc.execute("split", word));
            }
        } catch (Exception ignored) {

        } finally {
            cluster.shutdown();
        }
    }

}
