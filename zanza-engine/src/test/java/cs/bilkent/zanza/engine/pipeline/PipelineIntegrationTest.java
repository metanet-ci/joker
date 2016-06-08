package cs.bilkent.zanza.engine.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.zanza.engine.TestUtils.assertTrueEventually;
import static cs.bilkent.zanza.engine.TestUtils.spawnThread;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.SINGLE_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.kvstore.KVStoreContext;
import cs.bilkent.zanza.engine.kvstore.KVStoreManager;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import cs.bilkent.zanza.engine.kvstore.impl.KVStoreManagerImpl;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.zanza.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.zanza.engine.supervisor.Supervisor;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueManagerImpl;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.PartitionedTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.context.TuplePartitionerTupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
import cs.bilkent.zanza.operator.InitializationContext;
import cs.bilkent.zanza.operator.InvocationContext;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.Tuples;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operator.kvstore.KVStore;
import cs.bilkent.zanza.operator.kvstore.impl.KeyDecoratedKVStore;
import cs.bilkent.zanza.operator.scheduling.ScheduleNever;
import cs.bilkent.zanza.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import cs.bilkent.zanza.operators.FilterOperator;
import cs.bilkent.zanza.operators.MapperOperator;
import cs.bilkent.zanza.utils.Pair;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// TODO clean up duplicate code
public class PipelineIntegrationTest
{

    private final ZanzaConfig zanzaConfig = new ZanzaConfig();

    private final TupleQueueManager tupleQueueManager = new TupleQueueManagerImpl();

    private final KVStoreProvider kvStoreProvider = key -> null;

    private final PipelineInstanceId pipelineInstanceId1 = new PipelineInstanceId( 0, 0, 0 );

    private final PipelineInstanceId pipelineInstanceId2 = new PipelineInstanceId( 0, 1, 0 );

    private final PipelineInstanceId pipelineInstanceId3 = new PipelineInstanceId( 0, 2, 0 );

    @Before
    public void init ()
    {
        tupleQueueManager.init( zanzaConfig );
    }

    @Test
    public void testPipelineWithSingleOperator () throws ExecutionException, InterruptedException
    {

        final OperatorConfig mapperOperatorConfig = new OperatorConfig();
        final Function<Tuple, Tuple> multiplyBy2 = tuple -> new Tuple( "val", 2 * tuple.getIntegerValueOrDefault( "val", 0 ) );
        mapperOperatorConfig.set( MapperOperator.MAPPER_CONFIG_PARAMETER, multiplyBy2 );
        final OperatorDefinition mapperOperatorDef = OperatorDefinitionBuilder.newInstance( "map", MapperOperator.class )
                                                                              .setConfig( mapperOperatorConfig )
                                                                              .build();

        final TupleQueueContext tupleQueueContext = tupleQueueManager.createTupleQueueContext( mapperOperatorDef, MULTI_THREADED, 0 );
        final TupleQueueDrainerPool drainerPool = new BlockingTupleQueueDrainerPool( mapperOperatorDef );
        final Supplier<TuplesImpl> tuplesImplSupplier = new NonCachedTuplesImplSupplier( mapperOperatorDef.outputPortCount() );

        final OperatorInstance operator = new OperatorInstance( pipelineInstanceId1,
                                                                mapperOperatorDef,
                                                                tupleQueueContext,
                                                                kvStoreProvider,
                                                                drainerPool,
                                                                tuplesImplSupplier );
        final PipelineInstance pipeline = new PipelineInstance( pipelineInstanceId1, new OperatorInstance[] { operator } );
        final PipelineInstanceRunner runner = new PipelineInstanceRunner( pipeline );

        final Supervisor supervisor = mock( Supervisor.class );
        final UpstreamContext initialUpstreamContext = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );
        when( supervisor.getUpstreamContext( pipelineInstanceId1 ) ).thenReturn( initialUpstreamContext );
        runner.setSupervisor( supervisor );

        final TupleCollectorDownstreamTupleSender tupleCollector = new TupleCollectorDownstreamTupleSender( mapperOperatorDef
                                                                                                                    .outputPortCount() );
        runner.setDownstreamTupleSender( tupleCollector );

        runner.init( zanzaConfig );

        final Thread runnerThread = spawnThread( runner );

        final int initialVal = 1 + new Random().nextInt( 98 );
        final int tupleCount = 200;
        for ( int i = 0; i < tupleCount; i++ )
        {
            tupleQueueContext.offer( 0, singletonList( new Tuple( i + 1, "val", initialVal + i ) ) );
        }

        assertTrueEventually( () -> assertEquals( tupleCount, tupleCollector.tupleQueues[ 0 ].size() ) );
        final List<Tuple> tuples = tupleCollector.tupleQueues[ 0 ].pollTuplesAtLeast( 1 );
        for ( int i = 0; i < tupleCount; i++ )
        {
            final Tuple expected = multiplyBy2.apply( new Tuple( "val", initialVal + i ) );
            expected.setSequenceNumber( i + 1 );
            assertEquals( expected, tuples.get( i ) );
        }

        final UpstreamContext updatedUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } );
        reset( supervisor );
        when( supervisor.getUpstreamContext( pipelineInstanceId1 ) ).thenReturn( updatedUpstreamContext );
        runner.updatePipelineUpstreamContext();
        runnerThread.join();
    }

    @Test
    public void testPipelineWithMultipleOperators_operatorSetsScheduleNever () throws ExecutionException, InterruptedException
    {
        final OperatorConfig mapperOperatorConfig = new OperatorConfig();
        final Function<Tuple, Tuple> add1 = tuple -> new Tuple( "val", 1 + tuple.getIntegerValueOrDefault( "val", -1 ) );
        mapperOperatorConfig.set( MapperOperator.MAPPER_CONFIG_PARAMETER, add1 );
        final OperatorDefinition mapperOperatorDef = OperatorDefinitionBuilder.newInstance( "map", MapperOperator.class )
                                                                              .setConfig( mapperOperatorConfig )
                                                                              .build();

        final TupleQueueContext mapperTupleQueueContext = tupleQueueManager.createTupleQueueContext( mapperOperatorDef, MULTI_THREADED, 0 );

        final TupleQueueDrainerPool mapperDrainerPool = new BlockingTupleQueueDrainerPool( mapperOperatorDef );
        final Supplier<TuplesImpl> mapperTuplesImplSupplier = new CachedTuplesImplSupplier( mapperOperatorDef.outputPortCount() );

        final OperatorInstance mapperOperator = new OperatorInstance( pipelineInstanceId1,
                                                                      mapperOperatorDef,
                                                                      mapperTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      mapperDrainerPool,
                                                                      mapperTuplesImplSupplier );

        final OperatorConfig filterOperatorConfig = new OperatorConfig();
        final Predicate<Tuple> filterEvenVals = tuple -> tuple.getInteger( "val" ) % 2 == 0;
        filterOperatorConfig.set( FilterOperator.PREDICATE_CONFIG_PARAMETER, filterEvenVals );
        final OperatorDefinition filterOperatorDef = OperatorDefinitionBuilder.newInstance( "filter", FilterOperator.class )
                                                                              .setConfig( filterOperatorConfig )
                                                                              .build();

        final TupleQueueContext filterTupleQueueContext = tupleQueueManager.createTupleQueueContext( filterOperatorDef,
                                                                                                     SINGLE_THREADED,
                                                                                                     0 );

        final TupleQueueDrainerPool filterDrainerPool = new NonBlockingTupleQueueDrainerPool( filterOperatorDef );
        final Supplier<TuplesImpl> filterTuplesImplSupplier = new NonCachedTuplesImplSupplier( filterOperatorDef.inputPortCount() );

        final OperatorInstance filterOperator = new OperatorInstance( pipelineInstanceId1,
                                                                      filterOperatorDef,
                                                                      filterTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      filterDrainerPool,
                                                                      filterTuplesImplSupplier );

        final PipelineInstance pipeline = new PipelineInstance( pipelineInstanceId1,
                                                                new OperatorInstance[] { mapperOperator, filterOperator } );
        final PipelineInstanceRunner runner = new PipelineInstanceRunner( pipeline );

        final Supervisor supervisor = mock( Supervisor.class );
        final UpstreamContext initialUpstreamContext = new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } );
        when( supervisor.getUpstreamContext( pipelineInstanceId1 ) ).thenReturn( initialUpstreamContext );
        runner.setSupervisor( supervisor );

        final TupleCollectorDownstreamTupleSender tupleCollector = new TupleCollectorDownstreamTupleSender( filterOperatorDef
                                                                                                                    .outputPortCount() );
        runner.setDownstreamTupleSender( tupleCollector );

        runner.init( zanzaConfig );

        final Thread runnerThread = spawnThread( runner );

        final int initialVal = 2 + 2 * new Random().nextInt( 98 );
        final int tupleCount = 200;

        for ( int i = 0; i < tupleCount; i++ )
        {
            final int value = initialVal + i;
            final Tuple tuple = new Tuple( i + 1, "val", value );
            mapperTupleQueueContext.offer( 0, singletonList( tuple ) );
        }

        final int evenValCount = tupleCount / 2;
        assertTrueEventually( () -> assertEquals( evenValCount, tupleCollector.tupleQueues[ 0 ].size() ) );
        final List<Tuple> tuples = tupleCollector.tupleQueues[ 0 ].pollTuplesAtLeast( 1 );
        for ( int i = 0; i < evenValCount; i++ )
        {
            final Tuple expected = add1.apply( new Tuple( "val", initialVal + ( i * 2 ) ) );
            if ( filterEvenVals.test( expected ) )
            {
                expected.setSequenceNumber( ( i + 1 ) * 2 );
                assertEquals( expected, tuples.get( i ) );
            }
        }

        final UpstreamContext updatedUpstreamContext = new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } );
        reset( supervisor );
        when( supervisor.getUpstreamContext( pipelineInstanceId1 ) ).thenReturn( updatedUpstreamContext );
        runner.updatePipelineUpstreamContext();
        runnerThread.join();
    }

    @Test
    public void testPipelineWithMultipleOperators_pipelineUpstreamClosed () throws InterruptedException
    {
        final int batchCount = 4;

        final OperatorConfig generatorOperatorConfig = new OperatorConfig();
        generatorOperatorConfig.set( "batchCount", batchCount );

        final OperatorDefinition generatorOperatorDef = OperatorDefinitionBuilder.newInstance( "generator", ValueGeneratorOperator.class )
                                                                                 .setConfig( generatorOperatorConfig )
                                                                                 .build();

        final TupleQueueContext generatorTupleQueueContext = tupleQueueManager.createEmptyTupleQueueContext( generatorOperatorDef, 0 );

        final TupleQueueDrainerPool generatorDrainerPool = new NonBlockingTupleQueueDrainerPool( generatorOperatorDef );
        final Supplier<TuplesImpl> generatorTuplesImplSupplier = new CachedTuplesImplSupplier( generatorOperatorDef.outputPortCount() );

        final OperatorInstance generatorOperator = new OperatorInstance( pipelineInstanceId1,
                                                                         generatorOperatorDef,
                                                                         generatorTupleQueueContext,
                                                                         kvStoreProvider,
                                                                         generatorDrainerPool,
                                                                         generatorTuplesImplSupplier );

        final OperatorConfig passerOperatorConfig = new OperatorConfig();
        passerOperatorConfig.set( "batchCount", batchCount / 2 );

        final OperatorDefinition passerOperatorDef = OperatorDefinitionBuilder.newInstance( "passer", ValuePasserOperator.class )
                                                                              .setConfig( passerOperatorConfig )
                                                                              .build();

        final TupleQueueContext passerTupleQueueContext = tupleQueueManager.createTupleQueueContext( passerOperatorDef,
                                                                                                     SINGLE_THREADED,
                                                                                                     0 );

        final TupleQueueDrainerPool passerDrainerPool = new NonBlockingTupleQueueDrainerPool( passerOperatorDef );
        final Supplier<TuplesImpl> passerTuplesImplSupplier = new CachedTuplesImplSupplier( passerOperatorDef.outputPortCount() );

        final OperatorInstance passerOperator = new OperatorInstance( pipelineInstanceId1,
                                                                      passerOperatorDef,
                                                                      passerTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      passerDrainerPool,
                                                                      passerTuplesImplSupplier );

        final KVStoreManager kvStoreManager = new KVStoreManagerImpl();
        final KVStoreContext kvStoreContext = kvStoreManager.createKVStoreContext( "state", 1 );
        final KVStore kvStore = kvStoreContext.getKVStore( 0 );
        final KVStoreProvider kvStoreProvider = key -> new KeyDecoratedKVStore( key, kvStore );

        final OperatorDefinition stateOperatorDef = OperatorDefinitionBuilder.newInstance( "state", ValueStateOperator.class )
                                                                             .setPartitionFieldNames( singletonList( "val" ) )
                                                                             .build();

        final PartitionedTupleQueueContext stateTupleQueueContext = (PartitionedTupleQueueContext) tupleQueueManager
                                                                                                           .createTupleQueueContext(
                stateOperatorDef,
                SINGLE_THREADED,
                0 );
        final TupleQueueContext partitionerTupleQueueContext = new TuplePartitionerTupleQueueContext( stateTupleQueueContext );

        final TupleQueueDrainerPool stateDrainerPool = new NonBlockingTupleQueueDrainerPool( stateOperatorDef );
        final Supplier<TuplesImpl> stateTuplesImplSupplier = new CachedTuplesImplSupplier( stateOperatorDef.outputPortCount() );

        final OperatorInstance stateOperator = new OperatorInstance( pipelineInstanceId1,
                                                                     stateOperatorDef,
                                                                     partitionerTupleQueueContext,
                                                                     kvStoreProvider,
                                                                     stateDrainerPool,
                                                                     stateTuplesImplSupplier );

        final PipelineInstance pipeline = new PipelineInstance( pipelineInstanceId1,
                                                                new OperatorInstance[] { generatorOperator,
                                                                                         passerOperator,
                                                                                         stateOperator } );
        final PipelineInstanceRunner runner = new PipelineInstanceRunner( pipeline );

        final Supervisor supervisor = mock( Supervisor.class );
        final UpstreamContext initialUpstreamContext = new UpstreamContext( 0, new UpstreamConnectionStatus[] {} );
        when( supervisor.getUpstreamContext( pipelineInstanceId1 ) ).thenReturn( initialUpstreamContext );
        runner.setSupervisor( supervisor );

        runner.setDownstreamTupleSender( mock( DownstreamTupleSender.class ) );

        runner.init( zanzaConfig );

        final Thread runnerThread = spawnThread( runner );

        final ValueGeneratorOperator generatorOp = (ValueGeneratorOperator) generatorOperator.getOperator();
        generatorOp.start = true;

        assertTrueEventually( () -> assertTrue( generatorOp.count > 1000 ) );

        generatorOp.stop = true;

        assertTrueEventually( () -> verify( supervisor ).notifyPipelineCompletedRunning( pipelineInstanceId1 ) );

        runnerThread.join();

        final ValuePasserOperator passerOp = (ValuePasserOperator) passerOperator.getOperator();
        final ValueStateOperator stateOp = (ValueStateOperator) stateOperator.getOperator();

        assertEquals( generatorOp.count, passerOp.count );
        assertEquals( generatorOp.count, stateOp.count );
        assertEquals( generatorOp.count, kvStore.size() );
    }

    @Test
    public void testMultiplePipelines_singleInputPort () throws ExecutionException, InterruptedException
    {
        final OperatorConfig mapperOperatorConfig = new OperatorConfig();
        final Function<Tuple, Tuple> add1 = tuple -> new Tuple( "val", 1 + tuple.getIntegerValueOrDefault( "val", -1 ) );
        mapperOperatorConfig.set( MapperOperator.MAPPER_CONFIG_PARAMETER, add1 );
        final OperatorDefinition mapperOperatorDef = OperatorDefinitionBuilder.newInstance( "map", MapperOperator.class )
                                                                              .setConfig( mapperOperatorConfig )
                                                                              .build();

        final TupleQueueContext mapperTupleQueueContext = tupleQueueManager.createTupleQueueContext( mapperOperatorDef, MULTI_THREADED, 0 );

        final TupleQueueDrainerPool mapperDrainerPool = new BlockingTupleQueueDrainerPool( mapperOperatorDef );
        final Supplier<TuplesImpl> mapperTuplesImplSupplier = new NonCachedTuplesImplSupplier( mapperOperatorDef.outputPortCount() );

        final OperatorInstance mapperOperator = new OperatorInstance( pipelineInstanceId1,
                                                                      mapperOperatorDef,
                                                                      mapperTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      mapperDrainerPool,
                                                                      mapperTuplesImplSupplier );

        final PipelineInstance pipeline1 = new PipelineInstance( pipelineInstanceId1, new OperatorInstance[] { mapperOperator } );
        final PipelineInstanceRunner runner1 = new PipelineInstanceRunner( pipeline1 );

        final OperatorConfig filterOperatorConfig = new OperatorConfig();
        final Predicate<Tuple> filterEvenVals = tuple -> tuple.getInteger( "val" ) % 2 == 0;
        filterOperatorConfig.set( FilterOperator.PREDICATE_CONFIG_PARAMETER, filterEvenVals );
        final OperatorDefinition filterOperatorDef = OperatorDefinitionBuilder.newInstance( "filter", FilterOperator.class )
                                                                              .setConfig( filterOperatorConfig )
                                                                              .build();

        final TupleQueueContext filterTupleQueueContext = tupleQueueManager.createTupleQueueContext( filterOperatorDef, MULTI_THREADED, 0 );

        final TupleQueueDrainerPool filterDrainerPool = new BlockingTupleQueueDrainerPool( filterOperatorDef );
        final Supplier<TuplesImpl> filterTuplesImplSupplier = new NonCachedTuplesImplSupplier( filterOperatorDef.inputPortCount() );

        final OperatorInstance filterOperator = new OperatorInstance( pipelineInstanceId1,
                                                                      filterOperatorDef,
                                                                      filterTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      filterDrainerPool,
                                                                      filterTuplesImplSupplier );

        final PipelineInstance pipeline2 = new PipelineInstance( pipelineInstanceId2, new OperatorInstance[] { filterOperator } );
        final PipelineInstanceRunner runner2 = new PipelineInstanceRunner( pipeline2 );

        final SupervisorImpl supervisor = new SupervisorImpl();
        supervisor.upstreamContexts.put( pipelineInstanceId1, new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ) );
        supervisor.upstreamContexts.put( pipelineInstanceId2, new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE } ) );
        supervisor.inputPortIndices.put( pipelineInstanceId1, 0 );
        supervisor.inputPortIndices.put( pipelineInstanceId2, 0 );

        runner1.setSupervisor( supervisor );
        runner2.setSupervisor( supervisor );

        supervisor.targetPipelineInstanceId = pipelineInstanceId2;
        supervisor.runner = runner2;

        final DownstreamTupleSenderImpl tupleSender = new DownstreamTupleSenderImpl( filterTupleQueueContext,
                                                                                     new Pair[] { Pair.of( 0, 0 ) } );
        runner1.setDownstreamTupleSender( tupleSender );

        final TupleCollectorDownstreamTupleSender tupleCollector2 = new TupleCollectorDownstreamTupleSender( filterOperatorDef
                                                                                                                     .outputPortCount() );
        runner2.setDownstreamTupleSender( tupleCollector2 );

        runner1.init( zanzaConfig );
        runner2.init( zanzaConfig );

        final Thread runnerThread1 = spawnThread( runner1 );
        final Thread runnerThread2 = spawnThread( runner2 );

        final int initialVal = 2 + 2 * new Random().nextInt( 98 );
        final int tupleCount = 200;

        for ( int i = 0; i < tupleCount; i++ )
        {
            final int value = initialVal + i;
            final Tuple tuple = new Tuple( i + 1, "val", value );
            mapperTupleQueueContext.offer( 0, singletonList( tuple ) );
        }

        final int evenValCount = tupleCount / 2;
        assertTrueEventually( () -> assertEquals( evenValCount, tupleCollector2.tupleQueues[ 0 ].size() ) );
        final List<Tuple> tuples = tupleCollector2.tupleQueues[ 0 ].pollTuplesAtLeast( 1 );
        for ( int i = 0; i < evenValCount; i++ )
        {
            final Tuple expected = add1.apply( new Tuple( "val", initialVal + ( i * 2 ) ) );
            if ( filterEvenVals.test( expected ) )
            {
                expected.setSequenceNumber( ( i + 1 ) * 2 );
                assertEquals( expected, tuples.get( i ) );
            }
        }

        supervisor.upstreamContexts.put( pipelineInstanceId1, new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } ) );
        runner1.updatePipelineUpstreamContext();
        runnerThread1.join();
        runnerThread2.join();
        assertTrue( supervisor.completedPipelines.contains( pipelineInstanceId1 ) );
        assertTrue( supervisor.completedPipelines.contains( pipelineInstanceId2 ) );
    }

    @Test
    public void testMultiplePipelines_multipleInputPorts () throws InterruptedException
    {
        final int batchCount = 4;

        final OperatorConfig generatorOperatorConfig1 = new OperatorConfig();
        generatorOperatorConfig1.set( "batchCount", batchCount );

        final OperatorDefinition generatorOperatorDef1 = OperatorDefinitionBuilder.newInstance( "generator1", ValueGeneratorOperator.class )
                                                                                  .setConfig( generatorOperatorConfig1 )
                                                                                  .build();

        final TupleQueueContext generatorTupleQueueContext1 = tupleQueueManager.createEmptyTupleQueueContext( generatorOperatorDef1, 0 );

        final TupleQueueDrainerPool generatorDrainerPool1 = new NonBlockingTupleQueueDrainerPool( generatorOperatorDef1 );
        final Supplier<TuplesImpl> generatorTuplesImplSupplier1 = new NonCachedTuplesImplSupplier( generatorOperatorDef1.outputPortCount
                                                                                                                                 () );

        final OperatorInstance generatorOperator1 = new OperatorInstance( pipelineInstanceId1,
                                                                          generatorOperatorDef1,
                                                                          generatorTupleQueueContext1,
                                                                          kvStoreProvider,
                                                                          generatorDrainerPool1,
                                                                          generatorTuplesImplSupplier1 );

        final PipelineInstance pipeline1 = new PipelineInstance( pipelineInstanceId1, new OperatorInstance[] { generatorOperator1 } );
        final PipelineInstanceRunner runner1 = new PipelineInstanceRunner( pipeline1 );

        final OperatorConfig generatorOperatorConfig2 = new OperatorConfig();
        generatorOperatorConfig2.set( "batchCount", batchCount );
        generatorOperatorConfig2.set( "increment", false );

        final OperatorDefinition generatorOperatorDef2 = OperatorDefinitionBuilder.newInstance( "generator2", ValueGeneratorOperator.class )
                                                                                  .setConfig( generatorOperatorConfig2 )
                                                                                  .build();

        final TupleQueueContext generatorTupleQueueContext2 = tupleQueueManager.createEmptyTupleQueueContext( generatorOperatorDef2, 0 );

        final TupleQueueDrainerPool generatorDrainerPool2 = new NonBlockingTupleQueueDrainerPool( generatorOperatorDef2 );
        final Supplier<TuplesImpl> generatorTuplesImplSupplier2 = new NonCachedTuplesImplSupplier( generatorOperatorDef2.outputPortCount
                                                                                                                                 () );

        final OperatorInstance generatorOperator2 = new OperatorInstance( pipelineInstanceId2,
                                                                          generatorOperatorDef2,
                                                                          generatorTupleQueueContext2,
                                                                          kvStoreProvider,
                                                                          generatorDrainerPool2,
                                                                          generatorTuplesImplSupplier2 );

        final PipelineInstance pipeline2 = new PipelineInstance( pipelineInstanceId2, new OperatorInstance[] { generatorOperator2 } );
        final PipelineInstanceRunner runner2 = new PipelineInstanceRunner( pipeline2 );

        final OperatorConfig sinkOperatorConfig = new OperatorConfig();
        final OperatorDefinition sinkOperatorDef = OperatorDefinitionBuilder.newInstance( "sink", ValueSinkOperator.class )
                                                                            .setConfig( sinkOperatorConfig )
                                                                            .build();

        final TupleQueueContext sinkTupleQueueContext = tupleQueueManager.createTupleQueueContext( sinkOperatorDef, MULTI_THREADED, 0 );

        final TupleQueueDrainerPool sinkDrainerPool = new BlockingTupleQueueDrainerPool( sinkOperatorDef );
        final Supplier<TuplesImpl> sinkTuplesImplSupplier = new CachedTuplesImplSupplier( sinkOperatorDef.outputPortCount() );

        final OperatorInstance sinkOperator = new OperatorInstance( pipelineInstanceId3,
                                                                    sinkOperatorDef,
                                                                    sinkTupleQueueContext,
                                                                    kvStoreProvider,
                                                                    sinkDrainerPool,
                                                                    sinkTuplesImplSupplier );

        final OperatorConfig passerOperatorConfig = new OperatorConfig();
        passerOperatorConfig.set( "batchCount", batchCount / 2 );

        final OperatorDefinition passerOperatorDef = OperatorDefinitionBuilder.newInstance( "passer", ValuePasserOperator.class )
                                                                              .setConfig( passerOperatorConfig )
                                                                              .build();

        final TupleQueueContext passerTupleQueueContext = tupleQueueManager.createTupleQueueContext( passerOperatorDef,
                                                                                                     SINGLE_THREADED,
                                                                                                     0 );

        final TupleQueueDrainerPool passerDrainerPool = new NonBlockingTupleQueueDrainerPool( passerOperatorDef );
        final Supplier<TuplesImpl> passerTuplesImplSupplier = new CachedTuplesImplSupplier( passerOperatorDef.outputPortCount() );

        final OperatorInstance passerOperator = new OperatorInstance( pipelineInstanceId3,
                                                                      passerOperatorDef,
                                                                      passerTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      passerDrainerPool,
                                                                      passerTuplesImplSupplier );

        final KVStoreManager kvStoreManager = new KVStoreManagerImpl();
        final KVStoreContext kvStoreContext = kvStoreManager.createKVStoreContext( "state", 1 );
        final KVStore kvStore = kvStoreContext.getKVStore( 0 );
        final KVStoreProvider kvStoreProvider = key -> new KeyDecoratedKVStore( key, kvStore );

        final OperatorDefinition stateOperatorDef = OperatorDefinitionBuilder.newInstance( "state", ValueStateOperator.class )
                                                                             .setPartitionFieldNames( singletonList( "val" ) )
                                                                             .build();

        final PartitionedTupleQueueContext stateTupleQueueContext = (PartitionedTupleQueueContext) tupleQueueManager
                                                                                                           .createTupleQueueContext(
                stateOperatorDef,
                SINGLE_THREADED,
                0 );
        final TupleQueueContext partitionerTupleQueueContext = new TuplePartitionerTupleQueueContext( stateTupleQueueContext );

        final TupleQueueDrainerPool stateDrainerPool = new NonBlockingTupleQueueDrainerPool( stateOperatorDef );
        final Supplier<TuplesImpl> stateTuplesImplSupplier = new CachedTuplesImplSupplier( stateOperatorDef.outputPortCount() );

        final OperatorInstance stateOperator = new OperatorInstance( pipelineInstanceId3,
                                                                     stateOperatorDef,
                                                                     partitionerTupleQueueContext,
                                                                     kvStoreProvider,
                                                                     stateDrainerPool,
                                                                     stateTuplesImplSupplier );

        final PipelineInstance pipeline3 = new PipelineInstance( pipelineInstanceId3,
                                                                 new OperatorInstance[] { sinkOperator, passerOperator, stateOperator } );
        final PipelineInstanceRunner runner3 = new PipelineInstanceRunner( pipeline3 );

        final SupervisorImpl supervisor = new SupervisorImpl();
        supervisor.upstreamContexts.put( pipelineInstanceId1, new UpstreamContext( 0, new UpstreamConnectionStatus[] {} ) );
        supervisor.upstreamContexts.put( pipelineInstanceId2, new UpstreamContext( 0, new UpstreamConnectionStatus[] {} ) );
        supervisor.upstreamContexts.put( pipelineInstanceId3, new UpstreamContext( 0, new UpstreamConnectionStatus[] { ACTIVE, ACTIVE } ) );
        supervisor.inputPortIndices.put( pipelineInstanceId1, 0 );
        supervisor.inputPortIndices.put( pipelineInstanceId2, 1 );
        supervisor.targetPipelineInstanceId = pipelineInstanceId3;
        supervisor.runner = runner3;

        runner1.setSupervisor( supervisor );
        runner2.setSupervisor( supervisor );
        runner3.setSupervisor( supervisor );

        runner1.setDownstreamTupleSender( new DownstreamTupleSenderImpl( sinkTupleQueueContext, new Pair[] { Pair.of( 0, 0 ) } ) );
        runner2.setDownstreamTupleSender( new DownstreamTupleSenderImpl( sinkTupleQueueContext, new Pair[] { Pair.of( 0, 1 ) } ) );
        runner3.setDownstreamTupleSender( new DownstreamTupleSenderImpl( null, new Pair[] {} ) );

        runner1.init( zanzaConfig );
        runner2.init( zanzaConfig );
        runner3.init( zanzaConfig );

        final Thread runnerThread1 = spawnThread( runner1 );
        final Thread runnerThread2 = spawnThread( runner2 );
        final Thread runnerThread3 = spawnThread( runner3 );

        final ValueGeneratorOperator generatorOp1 = (ValueGeneratorOperator) generatorOperator1.getOperator();
        generatorOp1.start = true;

        final ValueGeneratorOperator generatorOp2 = (ValueGeneratorOperator) generatorOperator2.getOperator();

        assertTrueEventually( () -> assertTrue( generatorOp1.count > 5000 ) );
        generatorOp1.stop = true;

        generatorOp2.start = true;
        assertTrueEventually( () -> assertTrue( generatorOp2.count < -5000 ) );
        generatorOp2.stop = true;

        assertTrueEventually( () -> supervisor.completedPipelines.contains( pipelineInstanceId1 ) );
        assertTrueEventually( () -> supervisor.completedPipelines.contains( pipelineInstanceId2 ) );
        assertTrueEventually( () -> supervisor.completedPipelines.contains( pipelineInstanceId3 ) );

        runnerThread1.join();
        runnerThread2.join();
        runnerThread3.join();

        final ValuePasserOperator passerOp = (ValuePasserOperator) passerOperator.getOperator();
        final ValueStateOperator stateOp = (ValueStateOperator) stateOperator.getOperator();

        final int totalCount = generatorOp1.count + Math.abs( generatorOp2.count );
        assertEquals( totalCount, passerOp.count );
        assertEquals( totalCount, stateOp.count );
        assertEquals( totalCount, kvStore.size() );
        ;
    }

    private static class TupleCollectorDownstreamTupleSender implements DownstreamTupleSender
    {

        private final TupleQueue[] tupleQueues;

        TupleCollectorDownstreamTupleSender ( final int portCount )
        {
            tupleQueues = new TupleQueue[ portCount ];
            for ( int i = 0; i < portCount; i++ )
            {
                tupleQueues[ i ] = new MultiThreadedTupleQueue( 1000 );
            }
        }

        @Override
        public Future<Void> send ( final PipelineInstanceId id, final TuplesImpl tuples )
        {
            for ( int i = 0; i < tuples.getPortCount(); i++ )
            {
                tupleQueues[ i ].offerTuples( tuples.getTuples( i ) );
            }

            return null;
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ),
                                @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) },
            outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class
            ) } ) } )
    public static class ValueSinkOperator implements Operator
    {

        private int seqNo = 0;

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnAny( 2, 1, 0, 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            final Tuples input = invocationContext.getInput();
            for ( Tuple tuple : input.getTuples( 0 ) )
            {
                tuple.setSequenceNumber( ++seqNo );
            }
            for ( Tuple tuple : input.getTuples( 1 ) )
            {
                tuple.setSequenceNumber( ++seqNo );
            }

            final Tuples output = invocationContext.getOutput();
            output.addAll( input.getTuples( 0 ) );
            output.addAll( input.getTuples( 1 ) );

            if ( invocationContext.isErroneousInvocation() )
            {
                final List<Integer> ports = new ArrayList<>();
                if ( invocationContext.isInputPortOpen( 0 ) )
                {
                    ports.add( 0 );
                }
                if ( invocationContext.isInputPortOpen( 1 ) )
                {
                    ports.add( 1 );
                }
                if ( ports.isEmpty() )
                {
                    invocationContext.setNewSchedulingStrategy( ScheduleNever.INSTANCE );
                }
                else
                {
                    invocationContext.setNewSchedulingStrategy( scheduleWhenTuplesAvailableOnAny( 2, 1, ports ) );
                }
            }
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val",
            type = Integer.class ) } ) } )
    public static class ValueGeneratorOperator implements Operator
    {

        private volatile boolean start;

        private volatile boolean stop;

        private int batchCount;

        private volatile int count;

        private boolean increment;

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            final OperatorConfig config = context.getConfig();
            batchCount = config.getInteger( "batchCount" );
            count = config.getIntegerOrDefault( "initial", 0 );
            increment = config.getBooleanOrDefault( "increment", true );
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            if ( start )
            {
                final Tuples output = invocationContext.getOutput();
                for ( int i = 0; i < batchCount; i++ )
                {
                    final int val = increment ? ++count : --count;
                    output.add( new Tuple( Math.abs( val ), "val", val ) );
                }
            }

            if ( stop )
            {
                invocationContext.setNewSchedulingStrategy( ScheduleNever.INSTANCE );
            }
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) },
            outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class
            ) } ) } )
    public static class ValuePasserOperator implements Operator
    {

        private volatile int count;

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            final int batchCount = context.getConfig().getInteger( "batchCount" );
            return scheduleWhenTuplesAvailableOnDefaultPort( EXACT, batchCount );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            final Tuples input = invocationContext.getInput();
            final Tuples output = invocationContext.getOutput();
            output.addAll( input.getTuplesByDefaultPort() );
            count += input.getTupleCount( 0 );
        }

    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) }, outputs = {} )
    public static class ValueStateOperator implements Operator
    {

        private volatile int count;

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 1 );
        }

        @Override
        public void invoke ( final InvocationContext invocationContext )
        {
            final Tuples input = invocationContext.getInput();
            final KVStore kvStore = invocationContext.getKVStore();
            for ( Tuple tuple : input.getTuplesByDefaultPort() )
            {
                kvStore.set( "tuple", tuple );
            }

            count += input.getTupleCount( 0 );
        }

    }


    public static class DownstreamTupleSenderImpl implements DownstreamTupleSender
    {

        private final TupleQueueContext tupleQueueContext;

        private final Pair<Integer, Integer>[] ports;

        public DownstreamTupleSenderImpl ( final TupleQueueContext tupleQueueContext, final Pair<Integer, Integer>[] ports )
        {
            this.tupleQueueContext = tupleQueueContext;
            this.ports = ports;
        }

        @Override
        public Future<Void> send ( final PipelineInstanceId id, final TuplesImpl tuples )
        {
            for ( Pair<Integer, Integer> p : ports )
            {
                final int outputPort = p._1;
                final int inputPort = p._2;
                if ( tuples.getTupleCount( outputPort ) > 0 )
                {
                    tupleQueueContext.offer( inputPort, tuples.getTuples( outputPort ) );
                }
            }

            return null;
        }

    }


    public static class SupervisorImpl implements Supervisor
    {

        private final Map<PipelineInstanceId, UpstreamContext> upstreamContexts = new ConcurrentHashMap<>();

        private final Map<PipelineInstanceId, Integer> inputPortIndices = new HashMap<>();

        private final Set<PipelineInstanceId> completedPipelines = Collections.newSetFromMap( new ConcurrentHashMap<>() );

        private PipelineInstanceId targetPipelineInstanceId;

        private PipelineInstanceRunner runner;

        @Override
        public UpstreamContext getUpstreamContext ( final PipelineInstanceId id )
        {
            return upstreamContexts.get( id );
        }

        @Override
        public void notifyPipelineStoppedSendingDownstreamTuples ( final PipelineInstanceId id )
        {
            fail( id.toString() );
        }

        @Override
        public synchronized void notifyPipelineCompletedRunning ( final PipelineInstanceId id )
        {
            assertTrue( completedPipelines.add( id ) );
            if ( !id.equals( targetPipelineInstanceId ) )
            {
                final UpstreamContext currentUpstreamContext = upstreamContexts.get( targetPipelineInstanceId );
                final UpstreamContext newUpstreamContext = currentUpstreamContext.withUpstreamConnectionStatus( inputPortIndices.get( id ),
                                                                                                                CLOSED );
                upstreamContexts.put( targetPipelineInstanceId, newUpstreamContext );
                runner.updatePipelineUpstreamContext();
            }
        }

    }

}
