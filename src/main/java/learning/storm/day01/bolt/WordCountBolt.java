package learning.storm.day01.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * WordCountBolt是topology中实际进行单词计数的组件，该Bolt的prepare()方法中，实例化了一个HashMap实例，用来存储单词和对应的计数。
 * 大部分实例变量通常是在prepare方法中进行实例化，这个设计模式是由topology的部署方式决定的。
 * 当topology发布时，所有的bolt和Spout组件首先会进行序列化，然后通过网络发布到集群中。如果spout或者bolt在序列化之前实例化了任何
 * 无法序列化的实例变量，在进行序列化是会抛出NotSerializableException导致topology部署失败。
 *
 * Created by admin on 2017/11/3.
 */
public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> counts = null;

    /**
     * 初始化Bolt时调用，做一些准备工作
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String, Long>();
    }

    /**
     * 当收到订阅的tuple时，调用该方法，即该方法是Bolt处理逻辑的入口
     * @param tuple
     */
    public void execute(Tuple tuple) {
//        String word = tuple.getStringByField("word");
        String word = tuple.getString(0);
        Long count = this.counts.get(word);
        if(count == null){
            count = 0L;
        }
        count++;
        this.counts.put(word, count);
        this.collector.emit(new Values(word, count));
    }

    /**
     * 声明一个输出流，其中的tuple包括了单词和对应的计数
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }
}
