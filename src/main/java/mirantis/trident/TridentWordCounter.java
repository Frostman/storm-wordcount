package mirantis.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.thrift7.TException;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

/**
 * @author slukjanov
 */
public class TridentWordCounter {
    public static void main(String[] args) throws TException, DRPCExecutionException {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("the cow jumped over the moon"),
                new Values("the man went to the store and bought some candy"),
                new Values("four score and seven years ago"),
                new Values("how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts =
                topology.newStream("spout1", spout)
                        .each(new Fields("sentence"), new Split(), new Fields("word"))
                        .groupBy(new Fields("word"))
                        .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                        .parallelismHint(6);

        LocalDRPC client = new LocalDRPC(); // comment in cluster mode
        topology.newDRPCStream("words", client /* don't pass client in cluster mode*/)
                .each(new Fields("args"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .stateQuery(wordCounts, new Fields("word"), new MapGet(), new Fields("count"))
                .each(new Fields("count"), new FilterNull())
                .aggregate(new Fields("count"), new Sum(), new Fields("sum"));

        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setMaxSpoutPending(100);
        cluster.submitTopology("test", config, topology.build());

        //DRPCClient client = new DRPCClient("localhost", 3772); // uncomment in cluster mode

        while (true) {
            Utils.sleep(1000);
            System.out.println(client.execute("words", "cat dog the man"));
            System.out.println(client.execute("words", "cat"));
            System.out.println(client.execute("words", "dog"));
            System.out.println(client.execute("words", "the"));
            System.out.println(client.execute("words", "man"));
            System.out.println("============================");
            // prints the JSON-encoded result, e.g.: "[[5078]]"
        }

    }
}
