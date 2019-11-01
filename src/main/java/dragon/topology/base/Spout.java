package dragon.topology.base;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.LocalCluster;
import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class Spout extends Component {
	private static final long serialVersionUID = -2734635234747476875L;
	private static final Log log = LogFactory.getLog(Spout.class);

	@Override
	public final void run() {
		getOutputCollector().resetEmit();
		if(closed) {
			log.warn("spout is already closed");
			return;
		}
		if(closing) {
			close();
			log.debug(getComponentId()+":"+getTaskId()+" closed");
			closed=true;
			return;
		}
		try {
			nextTuple();
		} catch (DragonEmitRuntimeException e) {
			log.warn("spout ["+getComponentId()+"]: "+e.getMessage());
			if(getLocalCluster().getState()==LocalCluster.State.RUNNING) getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
		} catch (Exception e) {
			log.warn("spout ["+getComponentId()+"]: "+e.getMessage());
			if(getLocalCluster().getState()==LocalCluster.State.RUNNING) getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
		} 
//		if(getOutputCollector().didEmit()) {
//			getLocalCluster().componentPending(this);
//		} else {
//			try {
//				Thread.sleep(1);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			getLocalCluster().componentPending(this);
//		}
	}

	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
	}
	
	public void nextTuple() {
		
	}
	
	public void close() {
		
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
