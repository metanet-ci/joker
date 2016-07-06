package cs.bilkent.zanza.engine.pipeline;

public interface OperatorReplicaListener
{

    void onStatusChange ( String operatorId, OperatorReplicaStatus status );

}
