package dragon.network.messages;

import java.io.Serializable;

/**
 * The base class for a Dragon message.
 * There is a design choice to make this class extend HashMap<String,Object> and
 * thereby allow "classless" messages where all message parameters are in a hash
 * map. Such a design choice was considered and abandoned.
 * @author aaron
 *
 */
public class Message implements Serializable {
	private static final long serialVersionUID = -6123498202112069826L;
}
