package mirantis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author slukjanov
 */
public class WordCounter {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("source", new RandSentenceGenerator(), 3);
        builder.setSpout("ping", new PingSpout());

        builder.setBolt("split", new SplitSentence(), 8)
                .shuffleGrouping("source");
        builder.setBolt("count", new WordCount(), 12)
                .fieldsGrouping("split", new Fields("word"))
                .allGrouping("ping");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            if (args.length > 1) {
                conf.setNumWorkers(Integer.parseInt(args[1]));
            }

            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(30000);

            cluster.shutdown();
        }
    }

}
