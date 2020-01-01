package com.iamcarrot.sample.TridentSample;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

public class WordReader extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private FileReader fileReader;

    private BufferedReader reader;

    private boolean completed = false;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            this.fileReader = new FileReader(conf.get("fileToRead").toString());
            this.reader = new BufferedReader(this.fileReader);

        } catch (Exception e) {
            throw new RuntimeException("Error while reading the file");
        }
    }

    public void nextTuple() {

        if (!completed) {
            try {
                String word = reader.readLine();

                if (word == null) {
                    completed = true;
                    fileReader.close();
                    return;
                }
                word = word.trim().toLowerCase();
                collector.emit(new Values(word));


            } catch (Exception ignored) {

            }
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
