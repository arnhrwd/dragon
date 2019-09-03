package dragon.examples;

import dragon.Config;
import dragon.LocalCluster;
import dragon.topology.TopologyBuilder;
import dragon.tuple.Fields;


public class TopologyFieldsGroupingTest {

	public static void main(String[] args) {
		
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		topologyBuilder.setSpout("numberSpout", new NumberSpout(), 1).setNumTasks(1);
		int uniqueNumberCount = 100;
		topologyBuilder.setBolt("updateBolt", new UpdateBolt(uniqueNumberCount), 100)
			.shuffleGrouping("numberSpout");
		//fieldBoltParallelism should be a factor of uniqueNumberCount
		int fieldBoltParallelism = 10;
		int numbersPerTask = uniqueNumberCount/fieldBoltParallelism;
		topologyBuilder.setBolt("fieldBolt", new FieldsBolt(numbersPerTask), fieldBoltParallelism)
				.fieldsGrouping("updateBolt","numbers", new Fields("number"));


		LocalCluster localCluster = new LocalCluster();
		Config conf = new Config();
		localCluster.submitTopology("numberTopology", conf, topologyBuilder.createTopology());
	}

}
