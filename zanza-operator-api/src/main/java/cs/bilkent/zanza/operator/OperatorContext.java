package cs.bilkent.zanza.operator;

import java.util.List;

import cs.bilkent.zanza.operator.kvstore.KVStore;

public interface OperatorContext
{
    String getName();

    OperatorConfig getConfig();

    List<Port> getIncomingPorts();

    List<Port> getOutgoingPorts();

    KVStore getKVStore();
}
