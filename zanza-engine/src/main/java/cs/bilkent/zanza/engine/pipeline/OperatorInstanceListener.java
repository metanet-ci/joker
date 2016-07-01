package cs.bilkent.zanza.engine.pipeline;

public interface OperatorInstanceListener
{

    void onStatusChange ( String operatorId, OperatorInstanceStatus status );

}
