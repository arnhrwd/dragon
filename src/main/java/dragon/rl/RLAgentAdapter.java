package dragon.rl;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import dragon.Config;
import dragon.Constants;
import dragon.Run;
import dragon.metrics.Sample;
import dragon.network.NodeDescriptor;
import dragon.network.comms.IComms;
import dragon.network.comms.TcpComms;
import dragon.network.messages.node.NodeMessage.NodeMessageType;
import dragon.network.messages.service.ServiceDoneSMsg;
import dragon.network.messages.service.ServiceErrorMessage;
import dragon.network.messages.service.ServiceMessage;
import dragon.network.messages.service.ServiceMessage.ServiceMessageType;
import dragon.network.messages.service.execRlAction.ExecRlActionSMsg;
import dragon.network.messages.service.getmetrics.GetMetricsSMsg;
import dragon.network.messages.service.getmetrics.MetricsSMsg;
import dragon.tuple.NetworkTask;

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
		//comms = new TcpComms(conf);
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

		HashMap<String, Float> meanInputQueueLengthBolts = new HashMap<String, Float>();
		HashMap<String, Float> meanOutputQueueLengthBolts = new HashMap<String, Float>();
		HashMap<String, Double> meanRatioInputQueueLengthBolts = new HashMap<String, Double>();
		HashMap<String, Double> meanRatioOutputQueueLengthBolts = new HashMap<String, Double>();
		HashMap<String, Long> totalProcessedBolts = new HashMap<String, Long>();
		HashMap<String, Long> totalEmittedBolts = new HashMap<String, Long>();
		HashMap<String, Long> totalTransferredBolts = new HashMap<String, Long>();
		
		//new
		HashMap<String, Float> meanOutputQueueLengthSpouts = new HashMap<String, Float>();
		HashMap<String, Double> meanRatioOutputQueueLengthSpouts = new HashMap<String, Double>();
		HashMap<String, Long> totalProcessedSpouts = new HashMap<String, Long>();
		HashMap<String, Long> totalEmittedSpouts = new HashMap<String, Long>();
		HashMap<String, Long> totalTransferredSpouts = new HashMap<String, Long>();
		
		// TODO refine metrics that will be part of the env's state
		Float aggMeanInputQueueLengthBolts = 0F;
		Float aggMeanOutputQueueLengthBolts = 0F;
		Double aggMeanRatioInputQueueLengthBolts = 0D;
		Double aggMeanRatioOutputQueueLengthBolts = 0D;
		Long aggTotalProcessedBolts = 0L;
		Long aggTotalEmittedBolts = 0L;
		Long aggTotalTransferredBolts = 0L;
		Float aggMeanOutputQueueLengthSpouts = 0F;
		Double aggMeanRatioOutputQueueLengthSpouts = 0D;
		Long aggTotalProcessedSpouts = 0L;
		Long aggTotalEmittedSpouts = 0L;
		Long aggTotalTransferredSpouts = 0L;
		int totalSpouts = 0;
		int totalBolts = 0;
		
		//TODO queue size not captured acurately, it may be that it was just flushed when queriedadd queue rations
		//count number of flushes maybe?
				
		

		//Is combining all metrics for all nodes, all components a good idea?
		//should we capture the relationship between components, their placement, and trakc their individual metrics?
		for (NodeDescriptor desc : conf.getHosts()) {

			comms = new TcpComms(conf);
			log.debug("requesting metrics from [" + desc.toString() + "] on service port [" + desc.getServicePort()
					+ "]");

			comms.open(desc);
			comms.sendServiceMsg(new GetMetricsSMsg(topologyId));
			ServiceMessage response = comms.receiveServiceMsg();

			if (response.getType().equals(ServiceMessageType.METRICS)) {

				MetricsSMsg m = (MetricsSMsg) response;

				for (String componentId : m.samples.keySet()) {
					
					//compute totals
					for (Integer taskId : m.samples.get(componentId).keySet()) {
						ArrayList<Sample> samples = m.samples.get(componentId).get(taskId);
						Sample sample = samples.get(samples.size() - 1);
						int spoutTasks = 0;
						int boltTasks = 0;
						if(sample.isSpout) { //spout metrics
							//initialize maps if needed
							if(!meanOutputQueueLengthSpouts.containsKey(componentId)) {
								meanOutputQueueLengthSpouts.put(componentId, 0F);
								meanRatioOutputQueueLengthSpouts.put(componentId, 0.0);
								totalProcessedSpouts.put(componentId, 0L);
								totalEmittedSpouts.put(componentId, 0L);
								totalTransferredSpouts.put(componentId, 0L);
							}
							
							//calculate totals
							meanOutputQueueLengthSpouts.put(componentId, (((meanOutputQueueLengthSpouts.get(componentId) * spoutTasks) + sample.outputQueueSize))/(spoutTasks + 1));
							meanRatioOutputQueueLengthSpouts.put(componentId, (((meanRatioOutputQueueLengthSpouts.get(componentId) * spoutTasks) + sample.outputQueueRatio))/(spoutTasks + 1));

							totalProcessedSpouts.put(componentId, totalProcessedSpouts.get(componentId) + sample.processed);
							totalEmittedSpouts.put(componentId, totalEmittedSpouts.get(componentId) + sample.emitted);
							totalTransferredSpouts.put(componentId, totalTransferredSpouts.get(componentId) + sample.transferred);
							spoutTasks++;
						} else { //bolt metrics
							//initialize maps if needed
							if (!meanInputQueueLengthBolts.containsKey(componentId)) {
								meanInputQueueLengthBolts.put(componentId, (float) 0.0);
								meanOutputQueueLengthBolts.put(componentId, (float) 0.0);
								meanRatioInputQueueLengthBolts.put(componentId, 0.0);
								meanRatioOutputQueueLengthBolts.put(componentId, 0.0);
								totalProcessedBolts.put(componentId, 0L);
								totalEmittedBolts.put(componentId, 0L);
								totalTransferredBolts.put(componentId, 0L);
							}
							//calculate totals
							meanInputQueueLengthBolts.put(componentId,(((meanInputQueueLengthBolts.get(componentId) * boltTasks) + sample.inputQueueSize))/(boltTasks + 1));
							meanOutputQueueLengthBolts.put(componentId,(((meanOutputQueueLengthBolts.get(componentId) * boltTasks) + sample.outputQueueSize))/(boltTasks + 1));
							meanRatioInputQueueLengthBolts.put(componentId,(((meanRatioInputQueueLengthBolts.get(componentId) * boltTasks) + sample.inputQueueRatio))/(boltTasks + 1));
							meanRatioOutputQueueLengthBolts.put(componentId,(((meanRatioOutputQueueLengthBolts.get(componentId) * boltTasks) + sample.outputQueueRatio))/(boltTasks + 1));
							
							totalProcessedBolts.put(componentId, totalProcessedBolts.get(componentId) + sample.processed);
							totalEmittedBolts.put(componentId, totalEmittedBolts.get(componentId) + sample.emitted);
							totalTransferredBolts.put(componentId, totalTransferredBolts.get(componentId) + sample.transferred);
							boltTasks++;
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
			// all of these have to be relative to a time period, not cummulative...the agent can look after it
			
			//Spouts
			for (String componentId : meanOutputQueueLengthSpouts.keySet()) {
				aggMeanOutputQueueLengthSpouts += meanOutputQueueLengthSpouts.get(componentId);
				aggMeanRatioOutputQueueLengthSpouts += meanRatioOutputQueueLengthSpouts.get(componentId);
				aggTotalProcessedSpouts += totalProcessedSpouts.get(componentId);
				aggTotalEmittedSpouts += totalEmittedSpouts.get(componentId);
				aggTotalTransferredSpouts += totalTransferredSpouts.get(componentId);
				totalSpouts++;
			}
			
			//Bolts
			for (String componentId : meanInputQueueLengthBolts.keySet()) {
				aggMeanInputQueueLengthBolts += meanInputQueueLengthBolts.get(componentId);
				aggMeanOutputQueueLengthBolts += meanOutputQueueLengthBolts.get(componentId);
				aggMeanRatioOutputQueueLengthBolts += meanRatioOutputQueueLengthBolts.get(componentId);
				aggMeanRatioInputQueueLengthBolts += meanRatioInputQueueLengthBolts.get(componentId);
				aggTotalProcessedBolts += totalProcessedBolts.get(componentId);
				aggTotalEmittedBolts += totalEmittedBolts.get(componentId);
				aggTotalTransferredBolts += totalTransferredBolts.get(componentId);
				totalBolts++;
			}
			

		}
		
		if (totalBolts != 0) {
			aggMeanInputQueueLengthBolts = aggMeanInputQueueLengthBolts / totalBolts;
			aggMeanOutputQueueLengthBolts = aggMeanOutputQueueLengthBolts / totalBolts;
			aggMeanRatioOutputQueueLengthBolts = aggMeanRatioOutputQueueLengthBolts / totalBolts;
			aggMeanRatioInputQueueLengthBolts = aggMeanRatioInputQueueLengthBolts / totalBolts;
			aggTotalProcessedBolts = aggTotalProcessedBolts / totalBolts;
			aggTotalEmittedBolts = aggTotalEmittedBolts / totalBolts;
			aggTotalTransferredBolts = aggTotalTransferredBolts / totalBolts;
		}

		if (totalSpouts != 0) {
			aggMeanOutputQueueLengthSpouts = aggMeanOutputQueueLengthSpouts / totalSpouts;
			aggMeanRatioOutputQueueLengthSpouts = aggMeanRatioOutputQueueLengthSpouts / totalSpouts;
			aggTotalProcessedSpouts = aggTotalProcessedSpouts / totalSpouts;
			aggTotalEmittedSpouts = aggTotalEmittedSpouts / totalSpouts;
			aggTotalTransferredSpouts = aggTotalTransferredSpouts / totalSpouts;
		}
		
		state = new StringBuilder().append(System.currentTimeMillis()).append(',')
				.append(aggMeanInputQueueLengthBolts).append(',')
				.append(aggMeanOutputQueueLengthBolts).append(',')
				.append(aggMeanRatioInputQueueLengthBolts).append(',')
				.append(aggMeanRatioOutputQueueLengthBolts).append(',')
				.append(aggTotalProcessedBolts).append(',')
				.append(aggTotalEmittedBolts).append(',')
				.append(aggTotalTransferredBolts).append(',')
				.append(aggMeanOutputQueueLengthSpouts).append(',')
				.append(aggMeanRatioOutputQueueLengthSpouts).append(',')
				.append(aggTotalProcessedSpouts).append(',')
				.append(aggTotalEmittedSpouts).append(',')
				.append(aggTotalTransferredSpouts).toString();
		
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
