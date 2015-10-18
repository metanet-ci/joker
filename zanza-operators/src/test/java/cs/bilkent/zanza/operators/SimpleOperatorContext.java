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

    private List<Port> inputPorts;

    private List<Port> outputPorts;

    private KVStore kvStore;

    public void setName ( final String name )
    {
        this.name = name;
    }

    public void setInputPorts ( final List<Port> inputPorts )
    {
        this.inputPorts = inputPorts;
    }

    public void setOutputPorts ( final List<Port> outputPorts )
    {
        this.outputPorts = outputPorts;
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
    public List<Port> getInputPorts ()
    {
        return inputPorts;
    }

    @Override
    public List<Port> getOutputPorts ()
    {
        return outputPorts;
    }

    @Override
    public KVStore getKVStore ()
    {
        return kvStore;
    }
}
