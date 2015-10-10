package cs.bilkent.zanza.operators;

import java.util.List;

import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.OperatorContext;
import cs.bilkent.zanza.operator.Port;
import cs.bilkent.zanza.operator.kvstore.KVStore;

public class SimpleOperatorContext implements OperatorContext
{
    private String name;

    private OperatorConfig config = new OperatorConfig();

    private List<Port> incomingPorts;

    private List<Port> outgoingPorts;

    private KVStore kvStore;

    public void setName ( final String name )
    {
        this.name = name;
    }

    public void setIncomingPorts ( final List<Port> incomingPorts )
    {
        this.incomingPorts = incomingPorts;
    }

    public void setOutgoingPorts ( final List<Port> outgoingPorts )
    {
        this.outgoingPorts = outgoingPorts;
    }

    public void setKvStore ( final KVStore kvStore )
    {
        this.kvStore = kvStore;
    }

    @Override
    public String getName ()
    {
        return name;
    }

    @Override
    public OperatorConfig getConfig ()
    {
        return config;
    }

    @Override
    public List<Port> getIncomingPorts ()
    {
        return incomingPorts;
    }

    @Override
    public List<Port> getOutgoingPorts ()
    {
        return outgoingPorts;
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStore;
    }
}
