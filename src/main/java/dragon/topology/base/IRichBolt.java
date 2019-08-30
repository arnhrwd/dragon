package dragon.topology.base;

import java.util.Map;

import dragon.Config;
import dragon.spout.SpoutOutputCollector;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;


public class IRichBolt implements Runnable {

	@SuppressWarnings("rawtypes")
	private Map conf;
	private TopologyContext context;
	private OutputCollector outputCollector;
	private InputCollector inputCollector;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	private enum NEXTACTION {
		prepare,
		execute,
		emitPending,
		close
	};
	
	private NEXTACTION nextAction;
	
	
	public IRichBolt() {
		nextAction=NEXTACTION.prepare;
		
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
	
	public String getComponentId(){
		return context.getThisComponentId();
	}
	
	public Integer getTaskId(){
		return context.getThisTaskIndex();
	}
	
	public void run() {
		switch(nextAction){
		case prepare:
			prepare(conf,context,outputCollector);
			nextAction=NEXTACTION.execute;
			break;
		case execute:
			Tuple tuple = inputCollector.getQueue().peek();
			if(tuple!=null){
				execute(tuple);
				inputCollector.getQueue().poll();
				// TODO: reschedule without delay
			} else {
				// TODO: reschedule after a small delay
			}
			nextAction=NEXTACTION.execute;
			break;
		case close:
			close();
		}
	}
	
	public void prepare(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			OutputCollector collector) {
		
	}
	
	public void ack(Object id) {
		
	}
	
	public void fail(Object id) {
		
	}
	
	public void execute(Tuple tuple) {
	
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
