package dragon.topology.base;

import java.util.Map;

import dragon.spout.SpoutOutputCollector;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;


public class IRichBolt implements Runnable {

	@SuppressWarnings("rawtypes")
	private Map conf;
	private TopologyContext context;
	private OutputCollector outputCollector;
	private InputCollector inputCollector;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	private enum NEXTACTION {
		open,
		nextTuple,
		close
	};
	
	private NEXTACTION nextAction;
	
	
	public IRichBolt() {
		nextAction=NEXTACTION.open;
		
	}
	
	public OutputFieldsDeclarer getOutputFieldsDeclarer() {
		return outputFieldsDeclarer;
	}
	
	public InputCollector getInputCollector() {
		return inputCollector;
	}
	
	public void prepareToOpen(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector outputCollector, InputCollector inputCollector) {
		this.conf=conf;
		this.context=context;
		this.outputCollector=outputCollector;
		this.inputCollector=inputCollector;
	}
	
	public void run() {
		switch(nextAction){
		case open:
			open(conf,context,outputCollector);
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
			OutputCollector collector) {
		
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

}
