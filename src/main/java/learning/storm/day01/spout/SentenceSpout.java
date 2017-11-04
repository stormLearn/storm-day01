package learning.storm.day01.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * BaseRichSpout类是ISpout接口和IComponent接口的一个简便的实现
 *
 * Created by admin on 2017/11/3.
 */
public class SentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homeword",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index = 0;

    /**
     * 该方法在ISpout接口中定义， 所有的Spout组件在初始化的时候调用这个方法。
     * open()接收三个参数，一个包含了Storm的配置信息的Map，TopoLogyContext对象提供了Topology中组件的信息，SpoutOutputCollector对象提供了发射tuple的方法
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * 该方法是所有spout实现的核心所在，Storm通过调用这个方法向输出的collector发射tuple
     */
    public void nextTuple() {
        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length){
            index = 0;
        }
    }

    /**
     * 该方法是在IComponent接口中定义的，所有的Storm组件都必须实现这个接口，Strom的组件通过这个方法告诉Storm该组件会发射哪些数据流，
     * 每个数据流的tuple中包含哪些字段。本例中，我们声明了Spout会发射一个数据流，其中的tuple包含一个字段（sentence)
     *
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    /**
     * 因为cleanup方法并不可靠，它只在 local mode 下生效，Storm集群模式下cleanup不会被调用执行，很多资源得不到释放，
     * 解决方案：
     * 所以在 kill topology 之前，先deactivate 相应的topology.在Spout中实现 deactivate() 方法，deactivate()方法中给bolt emit特殊的数据
     * ，bolt中判断接收的数据为特殊数据时，调用cleanup()方法，这样就可以保证cleanup()方法得以调用
     */
    @Override
    public void deactivate() {
        super.deactivate();
        System.out.println("deactivate to spout and bolt");
        collector.emit(new Values("shutDown"));
    }
}
