package dragon.examples;

import java.util.Map;
import java.util.UUID;

import dragon.spout.SpoutOutputCollector;
import dragon.task.TopologyContext;
import dragon.topology.OutputFieldsDeclarer;
import dragon.topology.base.BaseRichSpout;
import dragon.tuple.Fields;
import dragon.tuple.Values;

public class TextSpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	int num=0;
	
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector=collector;
	}
	
	@Override
	public void nextTuple() {
		UUID uuid = UUID.randomUUID();
        String randomUUIDString = uuid.toString();
		if(num<1000000) {
			//System.out.println("emitting "+num);
			collector.emit(new Values(randomUUIDString));
			num++;
		} else if(num==1000000) {
			System.out.println("finished emitting");
			num++;
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("uuid"));
	}
}
