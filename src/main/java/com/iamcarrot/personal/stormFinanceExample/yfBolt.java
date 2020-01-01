package com.iamcarrot.personal.stormFinanceExample;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.PrintWriter;
import java.util.Map;

public class yfBolt extends BaseBasicBolt {

    private PrintWriter writer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            String filename = stormConf.get("fileToWrite").toString();
            this.writer = new PrintWriter(filename, "UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("Error opening the file");
        }

    }

    public void execute(Tuple input, BasicOutputCollector collector) {

        String symbol = input.getStringByField("company");
        String timestamp = input.getStringByField("timestamp");

        Double price = input.getDoubleByField("price");
        Double prevClose = input.getDoubleByField("prev_close");

        boolean gain = price > prevClose;

        collector.emit(new Values(symbol, timestamp, price, gain));

        writer.println(symbol + ", " + timestamp + ", " + price + ", " + gain);
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("company", "timestamp", "price", "gain"));
    }

    @Override
    public void cleanup() {
        writer.close();
    }
}
