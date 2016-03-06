package cs.bilkent.zanza.engine.pipeline;

import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

class PipelineInstanceRunnerCommand
{

    public enum PipelineInstanceRunnerCommandType
    {
        PAUSE,
        RESUME,
        STOP
    }

    public static PipelineInstanceRunnerCommand pause ()
    {
        return new PipelineInstanceRunnerCommand( PipelineInstanceRunnerCommandType.PAUSE );
    }

    public static PipelineInstanceRunnerCommand resume ()
    {
        return new PipelineInstanceRunnerCommand( PipelineInstanceRunnerCommandType.RESUME );
    }

    public static PipelineInstanceRunnerCommand stop ()
    {
        return new PipelineInstanceRunnerCommand( PipelineInstanceRunnerCommandType.STOP );
    }


    private PipelineInstanceRunnerCommandType type;

    private final CompletableFuture<Void> future = new CompletableFuture<>();

    private PipelineInstanceRunnerCommand ( final PipelineInstanceRunnerCommandType type )
    {
        this.type = type;
    }

    public PipelineInstanceRunnerCommandType getType ()
    {
        return type;
    }

    public CompletableFuture<Void> getFuture ()
    {
        return future;
    }

    public void setType ( final PipelineInstanceRunnerCommandType type )
    {
        checkNotNull( type );
        this.type = type;
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
