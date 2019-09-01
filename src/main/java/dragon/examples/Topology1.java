package dragon.examples;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.TopologyBuilder;


public class Topology1 {

	public static void main(String[] args) {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("numberSpout", new NumberSpout(), 1).setNumTasks(1);
		topologyBuilder.setBolt("shuffleBolt", new ShuffleBolt(), 100).shuffleGrouping("numberSpout");
		topologyBuilder.setBolt("numberBolt", new NumberBolt(), 1).setNumTasks(1)
			.allGrouping("shuffleBolt","even")
			.allGrouping("shuffleBolt","odd");
		
		LocalCluster localCluster = new LocalCluster();
		Config conf = new Config();
		localCluster.submitTopology("numberTopology", conf, topologyBuilder.createTopology());
	}

}
