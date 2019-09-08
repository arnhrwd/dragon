package dragon.examples;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Fields;
import dragon.tuple.Tuple;
import dragon.tuple.Values;

import java.util.Map;

public class UpdateBolt extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3633120829946782762L;
	OutputCollector collector;
    private int uniqueNumbers;

    public UpdateBolt(int uniqueNumbers) {
        this.uniqueNumbers = uniqueNumbers;
    }

    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}
	public void execute(Tuple tuple) {
		int sendValue = ((Integer)tuple.getValueByField("number"))%uniqueNumbers;
		collector.emit("numbers",new Values(sendValue));

	}
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare("numbers",new Fields("number"));
	}
}
