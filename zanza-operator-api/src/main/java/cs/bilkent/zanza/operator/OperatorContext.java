package cs.bilkent.zanza.operator;

import java.util.List;

public interface OperatorContext
{

	String getName();

	OperatorConfig getConfig();

	List<Port> getIncomingPorts();

	List<Port> getOutgoingPorts();

}
