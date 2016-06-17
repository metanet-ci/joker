package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.Future;

import cs.bilkent.zanza.flow.Port;
import cs.bilkent.zanza.operator.impl.TuplesImpl;

public abstract class PipelineInstanceConnection implements DownstreamTupleSender
{

    protected final Port upstreamPort;

    protected final Port[] downstreamPorts;

    protected final PipelineInstanceId upstreamPipelineInstanceId;

    protected final PipelineInstanceId[] downstreamPipelineInstanceIds;

    public PipelineInstanceConnection ( final Port upstreamPort,
                                        final Port[] downstreamPorts,
                                        final PipelineInstanceId upstreamPipelineInstanceId,
                                        final PipelineInstanceId[] downstreamPipelineInstanceIds )
    {
        this.upstreamPort = upstreamPort;
        this.downstreamPorts = downstreamPorts;
        this.upstreamPipelineInstanceId = upstreamPipelineInstanceId;
        this.downstreamPipelineInstanceIds = downstreamPipelineInstanceIds;
    }

    public Port getUpstreamPort ()
    {
        return upstreamPort;
    }

    public PipelineInstanceId getUpstreamPipelineInstanceId ()
    {
        return upstreamPipelineInstanceId;
    }


    public boolean hasUpstreamOperator ( final String operatorId )
    {
        return upstreamPort.operatorId.equals( operatorId );
    }

    public boolean hasDownstreamOperator ( final String operatorId )
    {
        return downstreamPorts[ 0 ].operatorId.equals( operatorId );
    }

    abstract public Future<Void> send ( TuplesImpl tuples );

}
