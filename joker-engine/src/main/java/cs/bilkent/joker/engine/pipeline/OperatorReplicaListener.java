package cs.bilkent.joker.engine.pipeline;

public interface OperatorReplicaListener
{

    void onStatusChange ( String operatorId, OperatorReplicaStatus status );

}
