package dragon.examples;

import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichBolt;
import dragon.tuple.Fields;
import dragon.tuple.Tuple;
import dragon.tuple.Values;

import java.util.Map;

/**
 * 
 * @author aaron
 *
 */
public class UpdateBolt extends BaseRichBolt {
	private static final long serialVersionUID = -3633120829946782762L;
	
	/**
	 * 
	 */
	OutputCollector collector;
    
	/**
     * 
     */
    private int uniqueNumbers;

    /**
     * @param uniqueNumbers
     */
    public UpdateBolt(int uniqueNumbers) {
        this.uniqueNumbers = uniqueNumbers;
    }

    /* (non-Javadoc)
     * @see dragon.topology.base.BaseRichBolt#prepare(java.util.Map, dragon.task.TopologyContext, dragon.task.OutputCollector)
     */
    public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, OutputCollector collector) {
		this.collector=collector;
	}
	/* (non-Javadoc)
	 * @see dragon.topology.base.BaseRichBolt#execute(dragon.tuple.Tuple)
	 */
	public void execute(Tuple tuple) {
		int sendValue = ((Integer)tuple.getValueByField("number"))%uniqueNumbers;
		collector.emit("numbers",new Values(sendValue));

	}
	/* (non-Javadoc)
	 * @see dragon.topology.base.BaseRichBolt#declareOutputFields(dragon.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare("numbers",new Fields("number"));
	}
}
