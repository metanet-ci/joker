package cs.bilkent.zanza.engine.impl;

import java.util.concurrent.Future;

import cs.bilkent.zanza.engine.RuntimeEngine;
import cs.bilkent.zanza.flow.FlowDefinition;


public class SingleThreadRuntimeEngine implements RuntimeEngine
{
    @Override
    public void init ( final FlowDefinition flowDef )
    {

    }

    @Override
    public void run ()
    {

    }

    @Override
    public Future<Void> shutdown ()
    {
        return null;
    }

}
