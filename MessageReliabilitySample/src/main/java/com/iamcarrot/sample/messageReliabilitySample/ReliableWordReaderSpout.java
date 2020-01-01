package com.iamcarrot.sample.messageReliabilitySample;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReliableWordReaderSpout extends BaseRichSpout {

    public static final int MAX_FAILS = 3;

    private SpoutOutputCollector collector;

    private List<Integer> toSend;

    private Map<Integer, String> allMessages;

    private Map<Integer, Integer> msgFailedCount;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;

        try {

            FileReader fileReader = new FileReader(conf.get("fileToRead").toString());
            BufferedReader reader = new BufferedReader(fileReader);

            this.toSend = new ArrayList<>();
            this.allMessages = new HashMap<>();

            int i = 0;
            while (reader.readLine() != null) {
                allMessages.put(i++, reader.readLine());
                toSend.add(i);
            }

            this.msgFailedCount = new HashMap<>();

        } catch (Exception e) {
            throw new RuntimeException("Error while reading the file");
        }
    }

    @Override
    public void nextTuple() {

        if (!this.toSend.isEmpty()) {
            toSend.forEach(messageId -> collector.emit(new Values(allMessages.get(messageId)), messageId));
            toSend.clear();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("Successfully processed: " + msgId);
    }

    @Override
    public void fail(Object msgId) {

        int messageId = (int) msgId;

        msgFailedCount.put(messageId, msgFailedCount.getOrDefault(messageId, 0) + 1);

        if (msgFailedCount.get(messageId) > MAX_FAILS) {
            System.out.println("Message Failed: " + messageId);
            return;
        }
        System.out.println("Resending: " + messageId);
        this.toSend.add(messageId);
    }
}
