package dragon.spout;

import java.util.ArrayList;
import java.util.List;

import dragon.tuple.Tuple;
import dragon.tuple.Values;

public class SpoutOutputCollector {
	
	public List<Integer> emit(Tuple anchorTuple, Values values){
		return emit(values);
	}
	
	public List<Integer> emit(Values values){
		List<Integer> receivingTaskIds = new ArrayList<Integer>();
		return receivingTaskIds;
	}
}
