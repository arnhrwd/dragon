package dragon.topology.base;

import java.util.Map;

import dragon.Config;
import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class IRichSpout implements Runnable {
	@SuppressWarnings("rawtypes")
	private Map conf;
	private TopologyContext context;
	private SpoutOutputCollector collector;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	
	private enum NEXTACTION {
		open,
		nextTuple,
		close
	};
	
	private NEXTACTION nextAction;
	
	public IRichSpout() {
		nextAction=NEXTACTION.open;
		outputFieldsDeclarer = new OutputFieldsDeclarer();
	}
	
	public void prepareToOpen(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.conf=conf;
		this.context=context;
		this.collector=collector;
	}
	
	public void run() {
		switch(nextAction){
		case open:
			open(conf,context,collector);
			nextAction=NEXTACTION.nextTuple;
			break;
		case nextTuple:
			nextTuple();
			break;
		case close:
			close();
		}
		
	}
	
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	public void ack(Object id) {
		
	}
	
	public void fail(Object id) {
		
	}
	
	public void nextTuple() {
	
	}
	
	public void close() {
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}
	
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		return conf;
	}

}
