package cs.bilkent.joker;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import com.google.inject.Guice;
import com.google.inject.Injector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.JokerEngine;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecutionPlan;
import cs.bilkent.joker.engine.pipeline.PipelineId;
import cs.bilkent.joker.engine.region.RegionExecutionPlanFactory;
import cs.bilkent.joker.flow.FlowDef;

public class Joker
{

    private final JokerEngine engine;

    private final Injector injector;

    public Joker ()
    {
        this( new JokerConfig() );
    }

    public Joker ( final JokerConfig config )
    {
        this( UUID.randomUUID().toString(), config, null );
    }

    private Joker ( final Object jokerId, final JokerConfig config, final RegionExecutionPlanFactory regionExecutionPlanFactory )
    {
        this.injector = Guice.createInjector( new JokerModule( jokerId, config, regionExecutionPlanFactory ) );
        this.engine = injector.getInstance( JokerEngine.class );
    }

    public FlowExecutionPlan run ( final FlowDef flow )
    {
        return engine.run( flow );
    }

    public FlowStatus getStatus ()
    {
        return engine.getStatus();
    }

    public Future<FlowExecutionPlan> mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIds )
    {
        return engine.mergePipelines( flowVersion, pipelineIds );
    }

    public Future<FlowExecutionPlan> splitPipeline ( final int flowVersion,
                                                     final PipelineId pipelineId,
                                                     final List<Integer> pipelineOperatorIndices )
    {
        return engine.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );
    }

    public Future<FlowExecutionPlan> rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        return engine.rebalanceRegion( flowVersion, regionId, newReplicaCount );
    }

    public Future<Void> shutdown ()
    {
        return engine.shutdown();
    }

    public static class JokerBuilder
    {

        private Object jokerId = UUID.randomUUID().toString();

        private JokerConfig jokerConfig = new JokerConfig();

        private RegionExecutionPlanFactory regionExecutionPlanFactory;

        private boolean built;

        public JokerBuilder ()
        {
        }

        public JokerBuilder ( final JokerConfig jokerConfig )
        {
            this.jokerConfig = jokerConfig;
        }

        public JokerBuilder setJokerConfig ( final JokerConfig jokerConfig )
        {
            checkArgument( jokerConfig != null );
            checkState( !built, "Joker is already built!" );
            this.jokerConfig = jokerConfig;
            return this;
        }

        public JokerBuilder setRegionExecutionPlanFactory ( final RegionExecutionPlanFactory regionExecutionPlanFactory )
        {
            checkArgument( regionExecutionPlanFactory != null );
            checkState( !built, "Joker is already built!" );
            this.regionExecutionPlanFactory = regionExecutionPlanFactory;
            return this;
        }

        public JokerBuilder setJokerId ( final Object jokerId )
        {
            checkArgument( jokerId != null );
            this.jokerId = jokerId;
            return this;
        }

        public Joker build ()
        {
            checkState( !built, "Joker is already built!" );
            built = true;
            return new Joker( jokerId, jokerConfig, regionExecutionPlanFactory );
        }

    }

}
