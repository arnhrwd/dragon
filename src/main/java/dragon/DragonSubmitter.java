package dragon;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.IComms;
import dragon.network.Node;
import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.TcpComms;
import dragon.network.messages.service.GetNodeContextMessage;
import dragon.network.messages.service.NodeContextMessage;
import dragon.network.messages.service.RunTopologyMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.topology.DragonTopology;
import dragon.topology.RoundRobinEmbedding;

public class DragonSubmitter {
	private static Log log = LogFactory.getLog(Node.class);
	public static NodeDescriptor node;
	public static void submitTopology(String string, Config conf, DragonTopology topology) throws IOException {
		IComms comms = new TcpComms(conf);
		comms.open(node);
		comms.sendServiceMessage(new GetNodeContextMessage());
		ServiceMessage message = comms.receiveServiceMessage();
		NodeContext context;
		switch(message.getType()) {
		case NODE_CONTEXT:
			NodeContextMessage nc = (NodeContextMessage) message;
			context=nc.context;
			break;
		default:
			log.error("unexpected response: "+message.getType().name());
			throw new RuntimeException("could not obtain node context");
		}
		
		RoundRobinEmbedding rre = new RoundRobinEmbedding();
		topology.embedding = rre.generateEmbedding(topology, context);
		
		comms.sendServiceMessage(new RunTopologyMessage(string,conf,topology));
		message = comms.receiveServiceMessage();
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
