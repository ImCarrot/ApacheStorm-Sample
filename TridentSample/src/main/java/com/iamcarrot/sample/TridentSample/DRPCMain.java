package com.iamcarrot.sample.TridentSample;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;

public class DRPCMain {

    public static void main(String[] args) {

        LocalDRPC drpc = new LocalDRPC();

        TridentTopology topology = new TridentTopology();

        topology.newDRPCStream("split", drpc)
                .each(new Fields("args"), new SplitFunction(), new Fields("word_split"))
                .groupBy(new Fields("word_split"))
                .aggregate(new Count(), new Fields("count"));

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("Trident-Topology", conf, topology.build());
        for (String word : new String[]{"This is a very very short stick", "this is a very very long stick"}) {
            System.out.println("Result for: " + word + ": " + drpc.execute("split", word));
        }

        cluster.shutdown();
    }

}
