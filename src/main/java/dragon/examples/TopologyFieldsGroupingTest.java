package dragon.examples;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.TopologyBuilder;
import dragon.topology.base.Bolt;
import dragon.topology.base.Spout;
import dragon.tuple.Fields;

/**
 * 
 * @author aaron
 *
 */
public class TopologyFieldsGroupingTest {

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("numberSpout", (Spout)new NumberSpout(), 1).setNumTasks(1);
		int uniqueNumberCount = 100;
		topologyBuilder.setBolt("updateBolt", (Bolt)new UpdateBolt(uniqueNumberCount), 100)
			.shuffleGrouping("numberSpout");
		//fieldBoltParallelism should be a factor of uniqueNumberCount
		int fieldBoltParallelism = 10;
		int numbersPerTask = uniqueNumberCount/fieldBoltParallelism;
		topologyBuilder.setBolt("fieldBolt", (Bolt)new FieldsBolt(numbersPerTask), fieldBoltParallelism)
				.fieldsGrouping("updateBolt","numbers", new Fields("number"));


		LocalCluster localCluster = new LocalCluster();
		Config conf = new Config();
		localCluster.submitTopology("numberTopology", conf, topologyBuilder.createTopology());
	}

}
