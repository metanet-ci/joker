package cs.bilkent.zanza.engine.pipeline;

public interface OperatorInstanceLifecycleListener
{

    void onChange ( String operatorId, OperatorInstanceStatus status );

}
