package cs.bilkent.joker;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;

import static com.google.inject.name.Names.named;
import cs.bilkent.joker.engine.adaptation.AdaptationManager;
import cs.bilkent.joker.engine.adaptation.AdaptationTracker;
import cs.bilkent.joker.engine.adaptation.impl.JointAdaptationManager;
import cs.bilkent.joker.engine.adaptation.impl.LatencyOptimizingAdaptationManager;
import cs.bilkent.joker.engine.adaptation.impl.ThroughputOptimizingAdaptationManager;
import cs.bilkent.joker.engine.adaptation.impl.adaptationtracker.DefaultAdaptationTracker;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_ID;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.kvstore.OperatorKVStoreManager;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.metric.MetricManager;
import cs.bilkent.joker.engine.metric.impl.MetricManagerImpl;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImpl;
import cs.bilkent.joker.engine.region.FlowDefOptimizer;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionExecPlanFactory;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.region.impl.DefaultRegionExecPlanFactory;
import cs.bilkent.joker.engine.region.impl.FlowDefOptimizerImpl;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImpl;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.engine.region.impl.RegionManagerImpl;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueueManager;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorQueueManagerImpl;

public class JokerModule extends AbstractModule
{

    public static final String DOWNSTREAM_FAILURE_FLAG_NAME = "downstreamFailureFlag";

    public static final String THROUGHPUT_OPTIMIZING_ADAPTATION_MANAGER_NAME = "throughputOptimizer";

    public static final String LATENCY_OPTIMIZING_ADAPTATION_MANAGER_NAME = "latencyOptimizer";

    public static final String JOINT_ADAPTATION_MANAGER_NAME = "jointOptimizer";


    private Object jokerId;

    private final JokerConfig config;

    private final RegionExecPlanFactory regionExecPlanFactory;

    private final AdaptationTracker adaptationTracker;

    public JokerModule ( final JokerConfig config )
    {
        this( UUID.randomUUID().toString(), config, null, null );
    }

    JokerModule ( final Object jokerId, final JokerConfig config, final RegionExecPlanFactory regionExecPlanFactory,
                  final AdaptationTracker adaptationTracker )
    {
        this.jokerId = jokerId;
        this.config = config;
        this.regionExecPlanFactory = regionExecPlanFactory;
        this.adaptationTracker = adaptationTracker;
    }

    public JokerConfig getConfig ()
    {
        return config;
    }

    @Override
    protected void configure ()
    {
        bind( PartitionService.class ).to( PartitionServiceImpl.class );
        bind( OperatorKVStoreManager.class ).to( OperatorKVStoreManagerImpl.class );
        bind( OperatorQueueManager.class ).to( OperatorQueueManagerImpl.class );
        bind( RegionManager.class ).to( RegionManagerImpl.class );
        bind( RegionDefFormer.class ).to( RegionDefFormerImpl.class );
        bind( Supervisor.class ).to( SupervisorImpl.class );
        bind( PartitionKeyExtractorFactory.class ).to( PartitionKeyExtractorFactoryImpl.class );
        bind( PipelineManager.class ).to( PipelineManagerImpl.class );
        bind( MetricManager.class ).to( MetricManagerImpl.class );
        bind( FlowDefOptimizer.class ).to( FlowDefOptimizerImpl.class );
        bind( PipelineTransformer.class ).to( PipelineTransformerImpl.class );
        bind( AdaptationManager.class ).to( ThroughputOptimizingAdaptationManager.class );
        if ( regionExecPlanFactory != null )
        {
            bind( RegionExecPlanFactory.class ).toInstance( regionExecPlanFactory );
        }
        else
        {
            bind( RegionExecPlanFactory.class ).to( DefaultRegionExecPlanFactory.class );
        }
        if ( adaptationTracker != null )
        {
            bind( AdaptationTracker.class ).toInstance( adaptationTracker );
        }
        else
        {
            bind( AdaptationTracker.class ).to( DefaultAdaptationTracker.class );
        }
        bind( JokerConfig.class ).toInstance( config );
        bind( ThreadGroup.class ).annotatedWith( named( JOKER_THREAD_GROUP_NAME ) ).toInstance( new ThreadGroup( "Joker" ) );
        bind( ThreadMXBean.class ).toInstance( ManagementFactory.getThreadMXBean() );
        bind( RuntimeMXBean.class ).toInstance( ManagementFactory.getRuntimeMXBean() );
        bind( OperatingSystemMXBean.class ).toInstance( ManagementFactory.getOperatingSystemMXBean() );
        bind( IdGenerator.class ).toInstance( new IdGenerator() );
        bind( AtomicBoolean.class ).annotatedWith( named( DOWNSTREAM_FAILURE_FLAG_NAME ) ).toInstance( new AtomicBoolean() );
        bind( Object.class ).annotatedWith( named( JOKER_ID ) ).toInstance( jokerId );
        bind( MetricRegistry.class ).toInstance( new MetricRegistry() );
        bind( AdaptationManager.class ).annotatedWith( named( THROUGHPUT_OPTIMIZING_ADAPTATION_MANAGER_NAME ) )
                                       .to( ThroughputOptimizingAdaptationManager.class );
        bind( AdaptationManager.class ).annotatedWith( named( LATENCY_OPTIMIZING_ADAPTATION_MANAGER_NAME ) )
                                       .to( LatencyOptimizingAdaptationManager.class );
        bind( AdaptationManager.class ).annotatedWith( named( JOINT_ADAPTATION_MANAGER_NAME ) ).to( JointAdaptationManager.class );
    }

}
