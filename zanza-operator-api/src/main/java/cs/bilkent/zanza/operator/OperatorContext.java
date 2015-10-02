package cs.bilkent.zanza.operator;

import cs.bilkent.zanza.operator.kvstore.KVStore;

import java.util.List;

public interface OperatorContext
{

	String getName();

	OperatorConfig getConfig();

	List<Port> getIncomingPorts();

	List<Port> getOutgoingPorts();

	KVStore getKVStore();

}
