package dragon.topology.base;

import dragon.task.InputCollector;

public class Bolt extends Component {

	private InputCollector inputCollector;
	
	public void setInputCollector(InputCollector inputCollector) {
		this.inputCollector = inputCollector;
	}
	
	public InputCollector getInputCollector() {
		return inputCollector;
	}
	
}
