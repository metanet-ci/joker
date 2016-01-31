package cs.bilkent.zanza.engine;

import java.util.concurrent.Future;

import cs.bilkent.zanza.flow.FlowDefinition;


public interface RuntimeEngine
{

    void init ( FlowDefinition flowDef );

    void run ();

    Future<Void> shutdown ();

}
