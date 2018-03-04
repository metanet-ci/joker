package cs.bilkent.joker;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import javax.annotation.concurrent.ThreadSafe;

import com.google.inject.Guice;
import com.google.inject.Injector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.JokerEngine;
import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.flow.FlowExecPlan;
import cs.bilkent.joker.engine.flow.PipelineId;
import cs.bilkent.joker.engine.region.RegionExecPlanFactory;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.spec.OperatorType;

/**
 * Executes the {@link FlowDef} given by the user. It a thread-safe class, which can be used from multiple threads.
 * It contains methods to start an execution, change the execution model and shutdown it.
 */
@ThreadSafe
public class Joker
{

    private final JokerEngine engine;

    private final Injector injector;

    Joker ()
    {
        this( new JokerConfig() );
    }

    Joker ( final JokerConfig config )
    {
        this( UUID.randomUUID().toString(), config, null, null );
    }

    private Joker ( final Object jokerId,
                    final JokerConfig config, final RegionExecPlanFactory regionExecPlanFactory,
                    final AdaptationTracker adaptationTracker )
    {
        this.injector = Guice.createInjector( new JokerModule( jokerId, config, regionExecPlanFactory, adaptationTracker ) );
        this.engine = injector.getInstance( JokerEngine.class );
    }

    /**
     * Starts the execution of the given {@link FlowDef}.
     * Please keep in mind that the given {@link FlowDef} can be optimized before the execution starts and a new {@link FlowDef}, which
     * is semantically equivalent to the given {@link FlowDef}, can be used.
     *
     * @param flow
     *         flow to be executed by the runtime
     *
     * @return the flow execution plan which represents the current execution model of the given {@link FlowDef}
     */
    public FlowExecPlan run ( final FlowDef flow )
    {
        return engine.run( flow );
    }

    /**
     * Returns current status of the execution
     *
     * @return current status of the execution
     */
    public FlowStatus getStatus ()
    {
        return engine.getStatus();
    }

    /**
     * Merges pipelines given with {@code pipelineIds} parameter of the flow execution plan given with {@code flowVersion} parameter.
     * Only consecutive pipelines of a single region can be merged.
     *
     * @param flowVersion
     *         version of the flow execution model which is currently running
     * @param pipelineIds
     *         ids of the pipelines to be merged
     *
     * @return future to be notified once the merge is completed, along with the new {@link FlowExecPlan} after merge
     */
    public Future<FlowExecPlan> mergePipelines ( final int flowVersion, final List<PipelineId> pipelineIds )
    {
        return engine.mergePipelines( flowVersion, pipelineIds );
    }

    /**
     * Splits a pipeline given with {@code pipelineId} parameter into new pipelines, which will start by the operator indices given with
     * {@code pipelineOperatorIndices} parameter.
     *
     * @param flowVersion
     *         version of the flow execution model which is currently running
     * @param pipelineId
     *         id of the pipeline to be split
     * @param pipelineOperatorIndices
     *         operator indices within the split pipeline to create new pipelines
     *
     * @return future to be notified once the split is completed, along with the new {@link FlowExecPlan} after split
     */
    public Future<FlowExecPlan> splitPipeline ( final int flowVersion,
                                                final PipelineId pipelineId,
                                                final List<Integer> pipelineOperatorIndices )
    {
        return engine.splitPipeline( flowVersion, pipelineId, pipelineOperatorIndices );
    }

    /**
     * Rebalances a {@link OperatorType#PARTITIONED_STATEFUL} region given with the {@code regionId} parameter to number of replicas
     * given with {@code newReplicaCount} parameter
     *
     * @param flowVersion
     *         version of the flow execution model which is currently running
     * @param regionId
     *         id of the {@link OperatorType#PARTITIONED_STATEFUL} region to be rebalanced
     * @param newReplicaCount
     *         new replica count of the region
     *
     * @return future to be notified once the rebalance is completed, along with the new {@link FlowExecPlan} after rebalance
     */
    public Future<FlowExecPlan> rebalanceRegion ( final int flowVersion, final int regionId, final int newReplicaCount )
    {
        return engine.rebalanceRegion( flowVersion, regionId, newReplicaCount );
    }

    /**
     * Triggers the graceful shutdown process for the current execution.
     *
     * @return future to be notified once the shutdown is completed.
     */
    public Future<Void> shutdown ()
    {
        return engine.shutdown();
    }

    public Future<Void> disableAdaptation ()
    {
        return engine.disableAdaptation();
    }

    public static class JokerBuilder
    {

        private Object jokerId = UUID.randomUUID().toString();

        private JokerConfig jokerConfig = new JokerConfig();

        private RegionExecPlanFactory regionExecPlanFactory;

        private AdaptationTracker adaptationTracker;

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

        public JokerBuilder setRegionExecPlanFactory ( final RegionExecPlanFactory regionExecPlanFactory )
        {
            checkArgument( regionExecPlanFactory != null );
            checkState( !built, "Joker is already built!" );
            this.regionExecPlanFactory = regionExecPlanFactory;
            return this;
        }

        public JokerBuilder setAdaptationTracker ( final AdaptationTracker adaptationTracker )
        {
            checkArgument( adaptationTracker != null );
            checkState( !built, "Joker is already built!" );
            this.adaptationTracker = adaptationTracker;
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
            return new Joker( jokerId, jokerConfig, regionExecPlanFactory, adaptationTracker );
        }

    }

}
