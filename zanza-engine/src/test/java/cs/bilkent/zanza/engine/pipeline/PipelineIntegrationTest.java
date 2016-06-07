package cs.bilkent.zanza.engine.pipeline;

import java.util.Collections;
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
import static cs.bilkent.zanza.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;
import cs.bilkent.zanza.operator.schema.annotation.OperatorSchema;
import cs.bilkent.zanza.operator.schema.annotation.PortSchema;
import static cs.bilkent.zanza.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.zanza.operator.schema.annotation.SchemaField;
import cs.bilkent.zanza.operator.spec.OperatorSpec;
import static cs.bilkent.zanza.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.zanza.operator.spec.OperatorType.STATELESS;
import cs.bilkent.zanza.operators.FilterOperator;
import cs.bilkent.zanza.operators.MapperOperator;
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
    public void testMultiPipeline () throws ExecutionException, InterruptedException
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

        runner1.setSupervisor( supervisor );
        runner2.setSupervisor( supervisor );

        supervisor.expectedId = pipelineInstanceId1;
        supervisor.otherId = pipelineInstanceId2;
        supervisor.runner = runner2;

        final DownstreamTupleSenderImpl tupleSender = new DownstreamTupleSenderImpl( filterTupleQueueContext );
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


    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( inputs = {}, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val",
            type = Integer.class ) } ) } )
    public static class ValueGeneratorOperator implements Operator
    {

        private volatile boolean start;

        private volatile boolean stop;

        private int batchCount;

        private volatile int count;

        @Override
        public SchedulingStrategy init ( final InitializationContext context )
        {
            batchCount = context.getConfig().getInteger( "batchCount" );
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
                    final int val = ++count;
                    output.add( new Tuple( val, "val", val ) );
                }
            }

            if ( stop )
            {
                invocationContext.setNextSchedulingStrategy( ScheduleNever.INSTANCE );
            }
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) }, outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) } )
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

        public DownstreamTupleSenderImpl ( final TupleQueueContext tupleQueueContext )
        {
            this.tupleQueueContext = tupleQueueContext;
        }

        @Override
        public Future<Void> send ( final PipelineInstanceId id, final TuplesImpl tuples )
        {
            for ( int portIndex = 0; portIndex < tuples.getPortCount(); portIndex++ )
            {
                if ( tuples.getTupleCount( portIndex ) > 0 )
                {
                    tupleQueueContext.offer( portIndex, tuples.getTuples( portIndex ) );
                }
            }

            return null;
        }

    }


    public static class SupervisorImpl implements Supervisor
    {

        private final Map<PipelineInstanceId, UpstreamContext> upstreamContexts = new ConcurrentHashMap<>();

        private final Set<PipelineInstanceId> completedPipelines = Collections.newSetFromMap( new ConcurrentHashMap<>() );

        private PipelineInstanceId expectedId;

        private PipelineInstanceId otherId;

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
        public void notifyPipelineCompletedRunning ( final PipelineInstanceId id )
        {
            assertTrue( completedPipelines.add( id ) );
            if ( id.equals( expectedId ) )
            {
                upstreamContexts.put( otherId, new UpstreamContext( 1, new UpstreamConnectionStatus[] { CLOSED } ) );
                runner.updatePipelineUpstreamContext();
            }
        }

    }

}
