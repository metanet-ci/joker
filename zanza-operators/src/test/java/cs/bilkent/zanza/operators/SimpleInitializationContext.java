package cs.bilkent.zanza.operators;

import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.OperatorConfig;

public class SimpleInitializationContext implements InitializationContext
{
    private String name;

    private OperatorConfig config = new OperatorConfig();

    public void setName ( final String name )
    {
        this.name = name;
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

}
