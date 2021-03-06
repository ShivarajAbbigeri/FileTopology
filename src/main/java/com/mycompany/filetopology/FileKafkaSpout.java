package com.mycompany.filetopology;

/**
 *
 * @author shivaraj1
 */
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import java.util.UUID;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.kafka.*;

public class FileKafkaSpout {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        String zkConnString = "192.168.56.102:2080,192.168.56.103:2080,192.168.56.105:2080";
        String topic = "file-topic";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic,
                UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");
        builder.setBolt("forwardToFile", new WriteToFile()).shuffleGrouping("word-counter");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("FileKafkaSpout", config, builder.createTopology());

        Thread.sleep(100000);

        cluster.shutdown();
    }
}
