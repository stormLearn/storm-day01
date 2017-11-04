package learning.storm.day01.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * 本Bolt是一个位于数据流末端的bolt，只接受数据流，不发射任何数据流，
 *
 * Created by admin on 2017/11/3.
 */
public class ReportBolt extends BaseRichBolt {

    private Map<String, Long> counts = null;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<String, Long>();
    }

    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word2");
        Long count = tuple.getLongByField("count");
        this.counts.put(word, count);
        System.out.println(word);
        if (word.equals("shutDown")){
            cleanup();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void cleanup() {
        System.out.println("--------FINAL COUNTS -------");
        List<String> keys = new ArrayList<String>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for (String key : keys){
            System.out.println(key + " : " + this.counts.get(key));
        }
        System.out.println("--------FINAL COUNTS -------");
    }
}
