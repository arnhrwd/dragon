package dragon.examples;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.TopologyBuilder;


public class Topology1 {

	public static void main(String[] args) {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("numberSpout", new NumberSpout(), 1).setNumTasks(1);
		topologyBuilder.setSpout("textSpout", new TextSpout(), 1).setNumTasks(1);
		topologyBuilder.setBolt("shuffleBolt", new ShuffleBolt(), 100)
			.shuffleGrouping("numberSpout");
		topologyBuilder.setBolt("shuffleTextBolt", new ShuffleTextBolt(), 100)
			.shuffleGrouping("textSpout");
		topologyBuilder.setBolt("numberBolt", new NumberBolt(), 1).setNumTasks(1)
			.allGrouping("shuffleBolt","even")
			.allGrouping("shuffleBolt","odd")
			.allGrouping("shuffleTextBolt","uuid");
		
		LocalCluster localCluster = new LocalCluster();
		Config conf = new Config();
		localCluster.submitTopology("numberTopology", conf, topologyBuilder.createTopology());
	}

}
