package dragon.rl;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import dragon.Config;
import dragon.Constants;
import dragon.metrics.Sample;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.service.ServiceDoneSMsg;
import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;
import dragon.network.messages.service.execRlAction.ExecRlActionSMsg;
import dragon.network.messages.service.getRLMetrics.GetRLMetricsSMsg;
import dragon.network.messages.service.getRLMetrics.RLMetricsSMsg;
import dragon.network.messages.service.getmetrics.GetMetricsSMsg;
import dragon.network.messages.service.getmetrics.MetricsSMsg;

/**
 * Stand-alone utility to interact with an RL Agent via RabbitMQ.
 * 
 * @author maria
 *
 */
public class RLAgentAdapter {

	private Logger log = LogManager.getLogger(RLAgentAdapter.class);
	private String topologyId;
	private IComms comms;
	private Config conf;

	// RabbitMQ host and queues
	private static final String ACTION_QUEUE_NAME = "action_queue";
	private static final String STATE_QUEUE_NAME = "state_queue";
	private static String rabbitMqHost = "45.113.232.7";
	private static int rabbitMqPort = 5672;

	public RLAgentAdapter(String topologyId) {
		this.topologyId = topologyId;
	}

	public void runRlAdapter() throws Exception {

		conf = new Config(Constants.DRAGON_PROPERTIES, true);
		initalizeRabbitMq();

	}

	private void initalizeRabbitMq() throws Exception {

		// Connect to the rabbitmq server
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(rabbitMqHost);
		factory.setPort(rabbitMqPort);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		// Declare the queues from which the agent is going to consume
		channel.queueDeclare(ACTION_QUEUE_NAME, true, false, false, null);
		channel.queueDeclare(STATE_QUEUE_NAME, true, false, false, null);
		channel.basicQos(1);

		// callback for action queue
		DeliverCallback actionCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			//log.debug(" Received action messge from RL Agent: '" + message + "'");
			System.out.println("Received action messge from RL Agent: '" + message + "'");
			try {
				processAction(message);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
		};

		// callback for state queue
		DeliverCallback stateCallback = (consumerTag, delivery) -> {
			AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
					.correlationId(delivery.getProperties().getCorrelationId()).build();
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println("Received '" + message + "'");
			String response = "";
			try {
				response = getCurrentState();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("Publishing response: " + response);
			channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
			channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

		};

		channel.basicConsume(ACTION_QUEUE_NAME, false, actionCallback, consumerTag -> {
		});
		channel.basicConsume(STATE_QUEUE_NAME, false, stateCallback, consumerTag -> {
		});

	}

	private String getCurrentState() throws Exception {
		String state = "";

		log.debug("hosts are " + conf.getHosts());

		HashMap<String, Integer> maxInputQueueLengthBolts = new HashMap<String, Integer>();
		HashMap<String, Integer> maxOutputQueueLengthBolts = new HashMap<String, Integer>();
		HashMap<String, Double> maxRatioInputQueueLengthBolts = new HashMap<String, Double>();
		HashMap<String, Double> maxRatioOutputQueueLengthBolts = new HashMap<String, Double>();
		HashMap<String, Long> totalProcessedBolts = new HashMap<String, Long>();
		HashMap<String, Long> totalEmittedBolts = new HashMap<String, Long>();
		HashMap<String, Long> totalTransferredBolts = new HashMap<String, Long>();
		
		//new
		HashMap<String, Integer> maxOutputQueueLengthSpouts = new HashMap<String, Integer>();
		HashMap<String, Double> maxRatioOutputQueueLengthSpouts = new HashMap<String, Double>();
		HashMap<String, Double> maxAvgLatencyBolts = new HashMap<String, Double>();
		HashMap<String, Long> totalProcessedSpouts = new HashMap<String, Long>();
		HashMap<String, Long> totalEmittedSpouts = new HashMap<String, Long>();
		HashMap<String, Long> totalTransferredSpouts = new HashMap<String, Long>();
		
		// TODO refine metrics that will be part of the env's state
		Integer aggMaxInputQueueLengthBolts = 0;
		Integer aggMaxOutputQueueLengthBolts = 0;
		Double aggMaxRatioInputQueueLengthBolts = 0D;
		Double aggMaxRatioOutputQueueLengthBolts = 0D;
		Double aggMaxAvgLatencyBolts = 0D;
		Long aggTotalProcessedBolts = 0L;
		Long aggTotalEmittedBolts = 0L;
		Long aggTotalTransferredBolts = 0L;
		Integer aggMaxOutputQueueLengthSpouts = 0;
		Double aggMaxRatioOutputQueueLengthSpouts = 0D;
		Long aggTotalProcessedSpouts = 0L;
		Long aggTotalEmittedSpouts = 0L;
		Long aggTotalTransferredSpouts = 0L;
		int totalSpouts = 0;
		int totalBolts = 0;
		
		//Is combining all metrics for all nodes, all components a good idea?
		//should we capture the relationship between components, their placement, and track their individual metrics?
		for (NodeDescriptor desc : conf.getHosts()) {

			comms = new TcpComms(conf);
			log.debug("requesting metrics from [" + desc.toString() + "] on service port [" + desc.getServicePort()
					+ "]");

			comms.open(desc);
			comms.sendServiceMsg(new GetRLMetricsSMsg(topologyId));
			ServiceMessage response = comms.receiveServiceMsg();

			if (response.getType().equals(ServiceMessageType.RL_METRICS)) {

				RLMetricsSMsg m = (RLMetricsSMsg) response;

				for (String componentId : m.samples.keySet()) {
					
					//compute totals
					for (Integer taskId : m.samples.get(componentId).keySet()) {
						ArrayList<Sample> samples = m.samples.get(componentId).get(taskId);
						Sample sample = samples.get(samples.size() - 1);
						if(sample.isSpout) { //spout metrics
							//initialize maps if needed
							if(!maxOutputQueueLengthSpouts.containsKey(componentId)) {
								maxOutputQueueLengthSpouts.put(componentId, 0);
								maxRatioOutputQueueLengthSpouts.put(componentId, 0.0);
								totalProcessedSpouts.put(componentId, 0L);
								totalEmittedSpouts.put(componentId, 0L);
								totalTransferredSpouts.put(componentId, 0L);
							}
							
							//calculate totals
							int maxLegth = maxOutputQueueLengthSpouts.get(componentId) > sample.outputQueueSize ? maxOutputQueueLengthSpouts.get(componentId) : sample.outputQueueSize;
							maxOutputQueueLengthSpouts.put(componentId, maxLegth);//(((maxOutputQueueLengthSpouts.get(componentId) * spoutTasks) + sample.outputQueueSize))/(spoutTasks + 1));
							double maxRatio = maxRatioOutputQueueLengthSpouts.get(componentId) > sample.outputQueueRatio ? maxRatioOutputQueueLengthSpouts.get(componentId) : sample.outputQueueRatio;
							maxRatioOutputQueueLengthSpouts.put(componentId, maxRatio);//(((maxRatioOutputQueueLengthSpouts.get(componentId) * spoutTasks) + sample.outputQueueRatio))/(spoutTasks + 1));

							totalProcessedSpouts.put(componentId, totalProcessedSpouts.get(componentId) + sample.processed);
							totalEmittedSpouts.put(componentId, totalEmittedSpouts.get(componentId) + sample.emitted);
							totalTransferredSpouts.put(componentId, totalTransferredSpouts.get(componentId) + sample.transferred);
							
						} else { //bolt metrics //ADD AVGLATENCY, TEST NEW ON DEMAND METRIC MESSAGE, change to max queue size
							//initialize maps if needed
							if (!maxInputQueueLengthBolts.containsKey(componentId)) {
								maxInputQueueLengthBolts.put(componentId, 0);
								maxOutputQueueLengthBolts.put(componentId, 0);
								maxRatioInputQueueLengthBolts.put(componentId, 0.0);
								maxRatioOutputQueueLengthBolts.put(componentId, 0.0);
								totalProcessedBolts.put(componentId, 0L);
								totalEmittedBolts.put(componentId, 0L);
								totalTransferredBolts.put(componentId, 0L);
								maxAvgLatencyBolts.put(componentId, 0.0);
							}
							//calculate totals
							int maxLength = maxInputQueueLengthBolts.get(componentId) > sample.inputQueueSize ? maxInputQueueLengthBolts.get(componentId) : sample.inputQueueSize;
							maxInputQueueLengthBolts.put(componentId, maxLength); //(((meanInputQueueLengthBolts.get(componentId) * boltTasks) + sample.inputQueueSize))/(boltTasks + 1));
							maxLength = maxOutputQueueLengthBolts.get(componentId) > sample.outputQueueSize ? maxOutputQueueLengthBolts.get(componentId) : sample.outputQueueSize;
							maxOutputQueueLengthBolts.put(componentId, maxLength);//(((meanOutputQueueLengthBolts.get(componentId) * boltTasks) + sample.outputQueueSize))/(boltTasks + 1));
							double maxRatio = maxRatioInputQueueLengthBolts.get(componentId) > sample.inputQueueRatio ? maxRatioInputQueueLengthBolts.get(componentId) : sample.inputQueueRatio;
							maxRatioInputQueueLengthBolts.put(componentId, maxRatio);//(((meanRatioInputQueueLengthBolts.get(componentId) * boltTasks) + sample.inputQueueRatio))/(boltTasks + 1));
							maxRatio = maxRatioOutputQueueLengthBolts.get(componentId) > sample.outputQueueRatio ? maxRatioOutputQueueLengthBolts.get(componentId) : sample.outputQueueRatio;
							maxRatioOutputQueueLengthBolts.put(componentId, maxRatio);//(((meanRatioOutputQueueLengthBolts.get(componentId) * boltTasks) + sample.outputQueueRatio))/(boltTasks + 1));
							double maxLatency = maxAvgLatencyBolts.get(componentId) > sample.avgLatency ? maxAvgLatencyBolts.get(componentId) : sample.avgLatency;
							maxAvgLatencyBolts.put(componentId, maxLatency);
							totalProcessedBolts.put(componentId, totalProcessedBolts.get(componentId) + sample.processed);
							totalEmittedBolts.put(componentId, totalEmittedBolts.get(componentId) + sample.emitted);
							totalTransferredBolts.put(componentId, totalTransferredBolts.get(componentId) + sample.transferred);
							
						}
					}
				}
			}
			comms.sendServiceMsg(new ServiceDoneSMsg());
			comms.close();

			// TODO refine metrics that will be part of the env's state
			// outqueue size of spouts
			// inqueue and outqueue size of bolts
			// queue capacity ratios
			// spout emitted (emmission rate?)
			// total completely processed?? harder to implement...
			// all of these have to be relative to a time period, not cumulative...the agent should ensure this
			
			//Spouts
			for (String componentId : maxOutputQueueLengthSpouts.keySet()) {
				aggMaxOutputQueueLengthSpouts = aggMaxOutputQueueLengthSpouts > maxOutputQueueLengthSpouts.get(componentId) ? aggMaxOutputQueueLengthSpouts : maxOutputQueueLengthSpouts.get(componentId);
				aggMaxRatioOutputQueueLengthSpouts	= aggMaxRatioOutputQueueLengthSpouts > maxRatioOutputQueueLengthSpouts.get(componentId) ?  aggMaxRatioOutputQueueLengthSpouts : maxRatioOutputQueueLengthSpouts.get(componentId);	
				
				aggTotalProcessedSpouts += totalProcessedSpouts.get(componentId);
				aggTotalEmittedSpouts += totalEmittedSpouts.get(componentId);
				aggTotalTransferredSpouts += totalTransferredSpouts.get(componentId);
				totalSpouts++;
			}
			
			//Bolts
			for (String componentId : maxInputQueueLengthBolts.keySet()) {
				aggMaxOutputQueueLengthBolts = aggMaxOutputQueueLengthBolts > maxOutputQueueLengthBolts.get(componentId) ? aggMaxOutputQueueLengthBolts : maxOutputQueueLengthBolts.get(componentId);
				aggMaxRatioOutputQueueLengthBolts	= aggMaxRatioOutputQueueLengthBolts > maxRatioOutputQueueLengthBolts.get(componentId) ?  aggMaxRatioOutputQueueLengthBolts : maxRatioOutputQueueLengthBolts.get(componentId);	
				aggMaxInputQueueLengthBolts = aggMaxInputQueueLengthBolts > maxInputQueueLengthBolts.get(componentId) ? aggMaxInputQueueLengthBolts : maxInputQueueLengthBolts.get(componentId);
				aggMaxRatioInputQueueLengthBolts = aggMaxRatioInputQueueLengthBolts > maxRatioInputQueueLengthBolts.get(componentId) ?  aggMaxRatioInputQueueLengthBolts : maxRatioInputQueueLengthBolts.get(componentId);	
				aggMaxAvgLatencyBolts = aggMaxAvgLatencyBolts > maxAvgLatencyBolts.get(componentId) ? aggMaxAvgLatencyBolts : maxAvgLatencyBolts.get(componentId);
				aggTotalProcessedBolts += totalProcessedBolts.get(componentId);
				aggTotalEmittedBolts += totalEmittedBolts.get(componentId);
				aggTotalTransferredBolts += totalTransferredBolts.get(componentId);
				totalBolts++;
			}
		}
		
		if (totalBolts != 0) {
			aggTotalProcessedBolts = aggTotalProcessedBolts / totalBolts;
			aggTotalEmittedBolts = aggTotalEmittedBolts / totalBolts;
			aggTotalTransferredBolts = aggTotalTransferredBolts / totalBolts;
		}

		if (totalSpouts != 0) {
			aggTotalProcessedSpouts = aggTotalProcessedSpouts / totalSpouts;
			aggTotalEmittedSpouts = aggTotalEmittedSpouts / totalSpouts;
			aggTotalTransferredSpouts = aggTotalTransferredSpouts / totalSpouts;
		}
		
		state = new StringBuilder().append(System.currentTimeMillis()).append(',')
				.append(aggMaxInputQueueLengthBolts).append(',')
				.append(aggMaxOutputQueueLengthBolts).append(',')
				.append(aggMaxRatioInputQueueLengthBolts).append(',')
				.append(aggMaxRatioOutputQueueLengthBolts).append(',')
				.append(aggTotalProcessedBolts).append(',')
				.append(aggTotalEmittedBolts).append(',')
				.append(aggTotalTransferredBolts).append(',')
				.append(aggMaxOutputQueueLengthSpouts).append(',')
				.append(aggMaxRatioOutputQueueLengthSpouts).append(',')
				.append(aggTotalProcessedSpouts).append(',')
				.append(aggTotalEmittedSpouts).append(',')
				.append(aggTotalTransferredSpouts).append(',')
				.append(aggMaxAvgLatencyBolts).toString();
		
		return state;
	}

	private void processAction(String message) throws Exception {
		System.out.println("Processing action message: delta spout emission rate = " + message);
		long delta = Long.valueOf(message);

		log.debug("hosts are " + conf.getHosts());

		for (NodeDescriptor desc : conf.getHosts()) {

			comms = new TcpComms(conf);
			log.debug("sending RL action request to [" + desc.toString() + "] on service port [" + desc.getServicePort()
					+ "]");

			comms.open(desc);
			comms.sendServiceMsg(new ExecRlActionSMsg(topologyId, delta));
			ServiceMessage response = comms.receiveServiceMsg();

			if (response.getType().equals(ServiceMessageType.RL_ACTION_EXECUTED)) {
				System.out.println("action executed");
				//TODO there should be some kind of ack to the agent
			} else if (response.getType().equals(ServiceMessageType.EXEC_RL_ACTION_ERROR)) {
				ServiceErrorMessage error = (ServiceErrorMessage) response;
				System.out.println("action not executed: " + error.getError());
			}

			comms.sendServiceMsg(new ServiceDoneSMsg());
			comms.close();
		}
		
	}

	public static void main(String[] args) throws Exception {

		Options options = new Options();
		Option option = new Option("t", "topology", true, "name of the topology");
		option.setRequired(false);
		options.addOption(option);
		option = new Option("h", "mqhost", true, "rabbitMQ host");
		option.setRequired(false);
		options.addOption(option);
		option = new Option("p", "mqport", true, "rabbitMQ port");
		option.setRequired(false);
		options.addOption(option);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;

		try {
			cmd = parser.parse(options, args);

			if (!cmd.hasOption("topology")) {
				throw new ParseException("must provide a topology name with -t option");
			}
			String topologyId = cmd.getOptionValue("topology");
			if (cmd.hasOption("mqhost")) {
				rabbitMqHost = cmd.getOptionValue("mqhost");
			}
			if (cmd.hasOption("mqport")) {
				rabbitMqPort = Integer.valueOf(cmd.getOptionValue("mqport"));
			}
			RLAgentAdapter agentAdapter = new RLAgentAdapter(topologyId);
			agentAdapter.runRlAdapter();
		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("rladapter [-h host] [-p port] -t topology", options);
			System.exit(1);
		}
	}
}
