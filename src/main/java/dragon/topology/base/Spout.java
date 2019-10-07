package dragon.topology.base;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;

public class Spout extends Component {
	private static final long serialVersionUID = -2734635234747476875L;
	private static final Log log = LogFactory.getLog(Spout.class);

	@Override
	public final synchronized void run() {
		if(isClosing()) {
			close();
			//getOutputCollector().emitTerminateTuple();
			setClosed();
			return;
		}
		getOutputCollector().resetEmit();
		try {
			nextTuple();
		} catch (DragonEmitRuntimeException e) {
			log.fatal("spout ["+getComponentId()+"]: "+e.getMessage());
			getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
		} catch (Exception e) {
			log.fatal("spout ["+getComponentId()+"]: "+e.getMessage());
			getLocalCluster().componentException(this,e.getMessage(),e.getStackTrace());
		} 
		if(getOutputCollector().didEmit()) {
			getLocalCluster().componentPending(this);
		} else {
//			try {
//				Thread.sleep(1);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			getLocalCluster().componentPending(this);
		}
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
