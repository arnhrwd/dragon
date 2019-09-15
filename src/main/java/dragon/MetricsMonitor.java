package dragon;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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

import dragon.metrics.Sample;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.GetMetricsMessage;
import dragon.network.messages.service.MetricsMessage;
import dragon.network.messages.service.ServiceDoneMessage;
import dragon.network.messages.service.ServiceMessage;

public class MetricsMonitor {
	private static Log log = LogFactory.getLog(MetricsMonitor.class);
	private static String topologyId;
	private static IComms comms;
	private static Config conf;
	
	public static void runMonitor() throws IOException {
		comms = new TcpComms(conf);
		log.debug("remote service hosts are "+conf.getServiceHosts());
		
		while(true){
			try {
				Thread.sleep((int)conf.get(Config.DRAGON_METRICS_SAMPLE_PERIOD_MS));
			} catch (InterruptedException e) {
				break;
			}
			HashMap<String,Float> meanInputQueueLength = new HashMap<String,Float>();
			HashMap<String,Float> meanOutputQueueLength = new HashMap<String,Float>();
			HashMap<String,Long> totalProcessed = new HashMap<String,Long>();
			HashMap<String,Long> totalEmitted = new HashMap<String,Long>();
			HashMap<String,Long> totalTransferred = new HashMap<String,Long>();
			int meanInputQueueLengthCount=0;
			int meanOutputQueueLengthCount=0;
			for(NodeDescriptor desc : conf.getServiceHosts()){
				log.debug("requesting metrics from ["+desc.toString()+"]");
				comms.open(desc);
				comms.sendServiceMessage(new GetMetricsMessage(topologyId));
				ServiceMessage response = comms.receiveServiceMessage();
				switch(response.getType()){
				case METRICS:
					MetricsMessage m = (MetricsMessage) response;
					for(String componentId : m.samples.keySet()) {
						if(!meanInputQueueLength.containsKey(componentId)) {
							meanInputQueueLength.put(componentId,(float) 0.0);
							meanOutputQueueLength.put(componentId,(float) 0.0);
							totalProcessed.put(componentId,0L);
							totalEmitted.put(componentId,0L);
							totalTransferred.put(componentId,0L);
						}
						for(Integer taskId : m.samples.get(componentId).keySet()) {
							ArrayList<Sample> samples = m.samples.get(componentId).get(taskId);
							meanInputQueueLength.put(componentId,(meanInputQueueLength.get(componentId)*meanInputQueueLengthCount+
									samples.get(samples.size()-1).inputQueueSize)/(meanInputQueueLengthCount+1));
							meanInputQueueLengthCount++;
							meanOutputQueueLength.put(componentId,(meanOutputQueueLength.get(componentId)*meanOutputQueueLengthCount+
									samples.get(samples.size()-1).outputQueueSize)/(meanOutputQueueLengthCount+1));
							meanOutputQueueLengthCount++;
							totalProcessed.put(componentId,totalProcessed.get(componentId)+samples.get(samples.size()-1).processed);
							totalEmitted.put(componentId,totalEmitted.get(componentId)+samples.get(samples.size()-1).emitted);
							totalTransferred.put(componentId,totalTransferred.get(componentId)+samples.get(samples.size()-1).transferred);
						}
					}
					
					break;
				case GET_METRICS_ERROR:
					break;
				}
				comms.sendServiceMessage(new ServiceDoneMessage());
				comms.close();
			}
			for(String componentId : meanInputQueueLength.keySet()) {
				log.info("["+componentId+"] meanInputQueueLength="+meanInputQueueLength.get(componentId));
				log.info("["+componentId+"] meanOutputQueueLength="+meanOutputQueueLength.get(componentId));
				log.info("["+componentId+"] totalProcessed="+totalProcessed.get(componentId));
				log.info("["+componentId+"] totalEmitted="+totalEmitted.get(componentId));
				log.info("["+componentId+"] totalTransferred="+totalTransferred.get(componentId));
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
	
		
		CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
		
        try{
			cmd = parser.parse(options, args);
			
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
