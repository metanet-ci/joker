package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

class PipelineInstanceRunnerCommand
{

    public enum PipelineInstanceRunnerCommandType
    {
        PAUSE,
        RESUME,
        UPDATE_PIPELINE_UPSTREAM_CONTEXT
    }

    public static PipelineInstanceRunnerCommand pause ()
    {
        return new PipelineInstanceRunnerCommand( PipelineInstanceRunnerCommandType.PAUSE );
    }

    public static PipelineInstanceRunnerCommand resume ()
    {
        return new PipelineInstanceRunnerCommand( PipelineInstanceRunnerCommandType.RESUME );
    }

    public static PipelineInstanceRunnerCommand updatePipelineUpstreamContext ()
    {
        return new PipelineInstanceRunnerCommand( PipelineInstanceRunnerCommandType.UPDATE_PIPELINE_UPSTREAM_CONTEXT );
    }


    private final PipelineInstanceRunnerCommandType type;

    private final CompletableFuture<Void> future = new CompletableFuture<>();

    private PipelineInstanceRunnerCommand ( final PipelineInstanceRunnerCommandType type )
    {
        this.type = type;
    }

    public PipelineInstanceRunnerCommandType getType ()
    {
        return type;
    }

    public boolean hasType ( final PipelineInstanceRunnerCommandType type )
    {
        return this.type == type;
    }

    public CompletableFuture<Void> getFuture ()
    {
        return future;
    }

    public void complete ()
    {
        future.complete( null );
    }

    public void completeExceptionally ( final Throwable throwable )
    {
        checkNotNull( throwable );
        future.completeExceptionally( throwable );
    }

}
