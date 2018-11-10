package com.mycompany.filetopology;

/**
 *
 * @author shivaraj1
 */
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.Producer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WriteToFile implements IRichBolt {

    private OutputCollector collector;

    String topicName;

    PrintWriter writer;

    @Override
    public void prepare(Map map, TopologyContext tc, OutputCollector oc) {
        try {
            this.collector = oc;
            this.topicName = "wordsoutput";
            writer = new PrintWriter("/home/shivaraj1/Downloads/output.txt", "UTF-8");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(WriteToFile.class.getName()).log(Level.SEVERE, null, ex);
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(WriteToFile.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String w = tuple.getString(0);
        int i = tuple.getInteger(1);
        System.out.println("Values    " + w + ": " + Integer.toString(i));
        writer.append(w + ": " + Integer.toString(i));
    }

    @Override
    public void cleanup() {
        writer.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
