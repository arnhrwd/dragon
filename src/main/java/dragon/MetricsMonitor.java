package dragon;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import dragon.network.NodeContext;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.GetNodeContextMessage;
import dragon.network.messages.service.MetricsMessage;
import dragon.network.messages.service.NodeContextMessage;
import dragon.network.messages.service.ServiceDoneMessage;
import dragon.network.messages.service.ServiceMessage;

public class MetricsMonitor {
	private static Log log = LogFactory.getLog(MetricsMonitor.class);
	private static NodeDescriptor node;
	private static String topologyId;
	private static IComms comms;
	private static Config conf;
	
	private static void initComms(Config conf) throws UnknownHostException{
		comms=null;
		
		comms = new TcpComms(conf);
			
		
	}
	
	public static void runMonitor() throws IOException {
		initComms(conf);
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
		
		
		log.debug("received context  ["+context+"]");
		ArrayList<MetricsMessage> metricsMessages = new ArrayList<MetricsMessage>();
		while(true){
			try {
				Thread.sleep((long)conf.get("DRAGON_METRICS_SAMPLE_PERIOD_MS"));
			} catch (InterruptedException e) {
				break;
			}
			for(NodeDescriptor desc : context.values()){
				comms.open(desc);
				comms.sendServiceMessage(new GetMetricsMessage(topologyId));
				ServiceMessage response = comms.receiveServiceMessage();
				switch(response.getType()){
				case METRICS:
					MetricsMessage m = (MetricsMessage) response;
					log.info(response.toString());
					break;
				case METRICS_ERROR:
					break;
				}
				comms.sendServiceMessage(new ServiceDoneMessage());
				comms.close();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		final Properties properties = new Properties();
		properties.load(Run.class.getClassLoader().getResourceAsStream("project.properties"));
		log.debug("metrics monitor version "+properties.getProperty("project.version"));
		conf = new Config(Constants.DRAGON_PROPERTIES);
		Options options = new Options();
		Option topologyOption = new Option("t","topology",true,"name of the topology");
		topologyOption.setRequired(false);
		options.addOption(topologyOption);
		Option nodeOption = new Option("h","host",true,"hostname of existing node");
		nodeOption.setRequired(false);
		options.addOption(nodeOption);
		Option portOption = new Option("p","port",true,"port number of existing node");
		portOption.setRequired(false);
		options.addOption(portOption);
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
		
        try{
			cmd = parser.parse(options, args);
			node = new NodeDescriptor((String)conf.get(Config.DRAGON_NETWORK_REMOTE_HOST),
					(Integer)conf.get(Config.DRAGON_NETWORK_REMOTE_SERVICE_PORT));
			if(cmd.hasOption("host")) {
				node.setHost(cmd.getOptionValue("host"));
			}
			if(cmd.hasOption("port")) {
				node.setPort(Integer.parseInt(cmd.getOptionValue("port")));
			}
			if(!cmd.hasOption("topology")){
				throw new ParseException("must provide a topology name with -t option");
			}
			topologyId=cmd.getOptionValue("topology");
			runMonitor();
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("metrics [-h host] [-p port] -t topology", options);
            System.exit(1);
        }
	}
}
