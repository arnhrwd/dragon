package dragon.topology.base;

import java.util.Map;

import dragon.Config;
import dragon.task.InputCollector;
import dragon.task.OutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.tuple.Tuple;


public class IRichBolt implements Runnable, Cloneable {
	private TopologyContext context;
	private InputCollector inputCollector;
	private OutputFieldsDeclarer outputFieldsDeclarer;
	private enum NEXTACTION {
		execute,
		emitPending,
		close
	};
	
	private NEXTACTION nextAction;
	
	
	public IRichBolt() {
		nextAction=NEXTACTION.execute;
		
	}
	
	public void setTopologyContext(TopologyContext context) {
		this.context=context;
	}
	
	public OutputFieldsDeclarer getOutputFieldsDeclarer() {
		return outputFieldsDeclarer;
	}
	
	public void setInputCollector(InputCollector inputCollector) {
		this.inputCollector = inputCollector;
	}
	
	public InputCollector getInputCollector() {
		return inputCollector;
	}
	
	public String getComponentId(){
		return context.getThisComponentId();
	}
	
	public Integer getTaskId(){
		return context.getThisTaskIndex();
	}
	
	public void setOutputFieldsDeclarer(OutputFieldsDeclarer declarer) {
		this.outputFieldsDeclarer=declarer;
	}
	
	
	
	public void run() {
		switch(nextAction){
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
	
	public Object clone()throws CloneNotSupportedException{  
		return super.clone();  
	}  


}
