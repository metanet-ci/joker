package cs.bilkent.zanza;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.kvstore.KVStoreContextManager;
import cs.bilkent.zanza.engine.kvstore.impl.KVStoreContextManagerImpl;
import cs.bilkent.zanza.engine.partition.PartitionKeyFunctionFactory;
import cs.bilkent.zanza.engine.partition.PartitionService;
import cs.bilkent.zanza.engine.partition.PartitionServiceImpl;
import cs.bilkent.zanza.engine.partition.impl.PartitionKeyFunctionFactoryImpl;
import cs.bilkent.zanza.engine.pipeline.PipelineRuntimeManager;
import cs.bilkent.zanza.engine.pipeline.impl.PipelineRuntimeManagerImpl;
import cs.bilkent.zanza.engine.region.RegionDefFormer;
import cs.bilkent.zanza.engine.region.RegionManager;
import cs.bilkent.zanza.engine.region.impl.RegionDefFormerImpl;
import cs.bilkent.zanza.engine.region.impl.RegionManagerImpl;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.engine.supervisor.impl.SupervisorImpl;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContextManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueContextManagerImpl;

public class ZanzaModule extends AbstractModule
{

    private final ZanzaConfig config;

    public ZanzaModule ( final ZanzaConfig config )
    {
        this.config = config;
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
        bind( PartitionKeyFunctionFactory.class ).to( PartitionKeyFunctionFactoryImpl.class );
        bind( PipelineRuntimeManager.class ).to( PipelineRuntimeManagerImpl.class );
        bind( ZanzaConfig.class ).toInstance( config );
        bind( ThreadGroup.class ).annotatedWith( Names.named( "ZanzaThreadGroup" ) ).toInstance( new ThreadGroup( "Zanza" ) );
    }

}
