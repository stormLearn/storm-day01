package learning.storm.day01.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * BaseRichBolt类是IComponent和IBolt接口的一个简便实现，
 *
 * Created by admin on 2017/11/3.
 */
public class SplitSentenceBolt extends BaseRichBolt {

    private OutputCollector collector;

    /**
     * 该方法在IBolt接口中定义，等同于ISpout接口中定义的open()方法，这个方法在bolt初始化时调用，可以用来准备bolt用到的资源，如数据库连接
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /**
     * 该方法是IBolt接口定义的，每当从订阅的数据流中接收一个tuple，都会调用这个方法
     *
     * @param tuple
     */
    public void execute(Tuple tuple) {
        String sentence = tuple.getStringByField("sentence");
        String [] words = sentence.split(" ");
        for (String word : words){
            this.collector.emit(new Values(new Values(word)));
        }
    }

    /**
     * 声明本Bolt产生包含word字段的tuple
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
