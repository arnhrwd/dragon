package dragon.examples;

import dragon.Config;
import dragon.DragonSubmitter;
import dragon.LocalCluster;
import dragon.topology.TopologyBuilder;
import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;


/**
 * 
 * @author aaron
 *
 */
public class Topology1 {

	/**
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("numberSpout", (Spout)new NumberSpout(), 1).setNumTasks(1);
		topologyBuilder.setSpout("textSpout", (Spout)new TextSpout(), 1).setNumTasks(1);
		topologyBuilder.setBolt("shuffleBolt", (Bolt)new ShuffleBolt(), 10).setNumTasks(100)
			.shuffleGrouping("numberSpout");
		topologyBuilder.setBolt("shuffleTextBolt", (Bolt)new ShuffleTextBolt(), 20).setNumTasks(100)
			.shuffleGrouping("textSpout");
		topologyBuilder.setBolt("numberBolt", (Bolt)new NumberBolt(), 1).setNumTasks(1)
			.allGrouping("shuffleBolt","even")
			.allGrouping("shuffleBolt","odd")
			.allGrouping("shuffleTextBolt","uuid");
		
		if(args.length==0) {
			LocalCluster localCluster = new LocalCluster();
			Config conf = new Config();
			localCluster.submitTopology("numberTopology", conf, topologyBuilder.createTopology());
		} else {
			Config conf = new Config();
			System.out.println("topology name "+args[0]);
			DragonSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());
		}
	}

}
