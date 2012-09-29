package mirantis;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.HashMultiset;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * @author slukjanov
 */
public class WordCount extends BaseBasicBolt {

    private Logger logger;
    private String name;
    private int task;

    private HashMultiset<String> words = HashMultiset.create();

    @Override
    public void prepare(Map conf, TopologyContext ctx) {
        super.prepare(conf, ctx);
        logger = Logger.getLogger(this.getClass());
        name = ctx.getThisComponentId();
        task = ctx.getThisTaskIndex();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String source = tuple.getSourceComponent();
        if ("split".equals(source)) {
            words.add(tuple.getString(0));
        } else if ("ping".equals(source)) {
            logger.warn("RESULT " + name + ":" + task + " :: " + words);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
