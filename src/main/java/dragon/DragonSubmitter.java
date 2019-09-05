package dragon;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.IComms;
import dragon.network.Node;
import dragon.network.NodeDescriptor;
import dragon.network.TcpComms;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.topology.DragonTopology;

public class DragonSubmitter {
	private static Log log = LogFactory.getLog(Node.class);
	public static NodeDescriptor node;
	public static void submitTopology(String string, Config conf, DragonTopology topology) {
		IComms comms = new TcpComms();
		comms.open(node);
		comms.sendServiceMessage(new RunTopologyMessage(string,conf,topology));
		ServiceMessage message = comms.receiveServiceMessage();
		switch(message.getType()){
		case TOPOLOGY_EXISTS:
			log.error("topology ["+string+"] already exists");
			break;
		case TOPOLOGY_SUBMITTED:
			log.info("topology ["+string+"] submitted");
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
		}
	}

}
