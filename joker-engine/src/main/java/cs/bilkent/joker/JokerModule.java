package cs.bilkent.joker;

import java.util.UUID;

import com.google.inject.AbstractModule;

import static com.google.inject.name.Names.named;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_ID;
import static cs.bilkent.joker.engine.config.JokerConfig.JOKER_THREAD_GROUP_NAME;
import cs.bilkent.joker.engine.kvstore.KVStoreContextManager;
import cs.bilkent.joker.engine.kvstore.impl.KVStoreContextManagerImpl;
import cs.bilkent.joker.engine.partition.PartitionKeyExtractorFactory;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.PartitionServiceImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.pipeline.PipelineManager;
import cs.bilkent.joker.engine.pipeline.impl.PipelineManagerImpl;
import cs.bilkent.joker.engine.region.FlowDeploymentDefFormer;
import cs.bilkent.joker.engine.region.PipelineTransformer;
import cs.bilkent.joker.engine.region.RegionConfigFactory;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionManager;
import cs.bilkent.joker.engine.region.impl.FlowDeploymentDefFormerImpl;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.InteractiveRegionConfigFactory;
import cs.bilkent.joker.engine.region.impl.PipelineTransformerImpl;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.joker.engine.region.impl.RegionManagerImpl;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.joker.engine.tuplequeue.impl.TupleQueueContextManagerImpl;

public class JokerModule extends AbstractModule
{

    private Object jokerId;

    private final JokerConfig config;

    private final RegionConfigFactory regionConfigFactory;

    public JokerModule ( final JokerConfig config )
    {
        this( UUID.randomUUID().toString(), config, null );
    }

    public JokerModule ( final Object jokerId, final JokerConfig config, final RegionConfigFactory regionConfigFactory )
    {
        this.jokerId = jokerId;
        this.config = config;
        this.regionConfigFactory = regionConfigFactory;
    }

    public Object getJokerId ()
    {
        return jokerId;
    }

    public JokerConfig getConfig ()
    {
        return config;
    }

    public RegionConfigFactory getRegionConfigFactory ()
    {
        return regionConfigFactory;
    }

    @Override
    protected void configure ()
    {
        bind( PartitionService.class ).to( PartitionServiceImpl.class );
        bind( KVStoreContextManager.class ).to( KVStoreContextManagerImpl.class );
        bind( TupleQueueContextManager.class ).to( TupleQueueContextManagerImpl.class );
        bind( RegionManager.class ).to( RegionManagerImpl.class );
        bind( RegionDefFormer.class ).to( RegionDefFormerImpl.class );
        bind( Supervisor.class ).to( SupervisorImpl.class );
        bind( PartitionKeyExtractorFactory.class ).to( PartitionKeyExtractorFactoryImpl.class );
        bind( PipelineManager.class ).to( PipelineManagerImpl.class );
        bind( FlowDeploymentDefFormer.class ).to( FlowDeploymentDefFormerImpl.class );
        bind( PipelineTransformer.class ).to( PipelineTransformerImpl.class );
        if ( regionConfigFactory != null )
        {
            bind( RegionConfigFactory.class ).toInstance( regionConfigFactory );
        }
        else
        {
            bind( RegionConfigFactory.class ).to( InteractiveRegionConfigFactory.class );
        }
        bind( JokerConfig.class ).toInstance( config );
        bind( ThreadGroup.class ).annotatedWith( named( JOKER_THREAD_GROUP_NAME ) ).toInstance( new ThreadGroup( "Joker" ) );
        bind( IdGenerator.class ).toInstance( new IdGenerator() );
        bind( Object.class ).annotatedWith( named( JOKER_ID ) ).toInstance( jokerId );
    }

}
