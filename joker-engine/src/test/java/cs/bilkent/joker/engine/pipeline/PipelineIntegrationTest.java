package cs.bilkent.joker.engine.pipeline;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.junit.Test;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.engine.config.ThreadingPref.MULTI_THREADED;
import static cs.bilkent.joker.engine.config.ThreadingPref.SINGLE_THREADED;
import cs.bilkent.joker.engine.kvstore.OperatorKVStore;
import cs.bilkent.joker.engine.kvstore.impl.KVStoreContainer;
import cs.bilkent.joker.engine.kvstore.impl.OperatorKVStoreManagerImpl;
import cs.bilkent.joker.engine.metric.PipelineReplicaMeter;
import cs.bilkent.joker.engine.partition.PartitionDistribution;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractorFactoryImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionServiceImpl;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createInitialClosedUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createSourceOperatorInitialUpstreamCtx;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.createSourceOperatorShutdownUpstreamCtx;
import cs.bilkent.joker.engine.supervisor.Supervisor;
import cs.bilkent.joker.engine.tuplequeue.OperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueue;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.OperatorQueueManagerImpl;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.joker.engine.tuplequeue.impl.operator.EmptyOperatorQueue;
import cs.bilkent.joker.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.joker.engine.util.concurrent.BackoffIdleStrategy;
import cs.bilkent.joker.engine.util.concurrent.IdleStrategy;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.DefaultInvocationCtx;
import cs.bilkent.joker.operator.impl.DefaultOutputCollector;
import cs.bilkent.joker.operator.impl.InternalInvocationCtx;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.operator.kvstore.KVStore;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.AT_LEAST;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByCount.EXACT;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnAny;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.scheduleWhenTuplesAvailableOnDefaultPort;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.schema.annotation.OperatorSchema;
import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static cs.bilkent.joker.operator.schema.annotation.PortSchemaScope.EXACT_FIELD_SET;
import cs.bilkent.joker.operator.schema.annotation.SchemaField;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.operators.FilterOperator;
import static cs.bilkent.joker.operators.FilterOperator.PREDICATE_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.test.AbstractJokerTest;
import cs.bilkent.joker.utils.Pair;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// TODO clean up duplicate code
public class PipelineIntegrationTest extends AbstractJokerTest
{

    private static final int REGION_ID = 1;

    private static final int REPLICA_INDEX = 1;


    private final JokerConfig jokerConfig = new JokerConfig();

    private final PartitionService partitionService = new PartitionServiceImpl( jokerConfig );

    private final OperatorQueueManagerImpl operatorQueueManager = new OperatorQueueManagerImpl( jokerConfig,
                                                                                                new PartitionKeyExtractorFactoryImpl() );

    private final OperatorKVStoreManagerImpl operatorKVStoreManager = new OperatorKVStoreManagerImpl();

    private final PipelineReplicaId pipelineReplicaId1 = new PipelineReplicaId( 0, 0, 0 );

    private final PipelineReplicaId pipelineReplicaId2 = new PipelineReplicaId( 1, 0, 0 );

    private final PipelineReplicaId pipelineReplicaId3 = new PipelineReplicaId( 2, 0, 0 );


    @Test
    public void testPipelineWithSingleOperator () throws InterruptedException
    {
        final BiConsumer<Tuple, Tuple> multiplyBy2 = ( input, output ) -> output.set( "val",
                                                                                      2 * input.getIntegerValueOrDefault( "val", 0 ) );
        final OperatorConfig mapperOperatorConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER, multiplyBy2 );
        final OperatorDef mapperOperatorDef = OperatorDefBuilder.newInstance( "map", MapperOperator.class )
                                                                .setConfig( mapperOperatorConfig )
                                                                .build();

        final PipelineReplicaMeter pipelineReplicaMeter = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                    pipelineReplicaId1,
                                                                                    mapperOperatorDef );

        final OperatorQueue operatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                     mapperOperatorDef,
                                                                                     REPLICA_INDEX,
                                                                                     MULTI_THREADED );
        final TupleQueueDrainerPool drainerPool = new BlockingTupleQueueDrainerPool( jokerConfig, mapperOperatorDef );

        final DefaultInvocationCtx mapperInvocationCtx = new DefaultInvocationCtx( mapperOperatorDef.getInputPortCount(),
                                                                                   key -> null,
                                                                                   new DefaultOutputCollector( mapperOperatorDef
                                                                                                                       .getOutputPortCount() ) );
        final OperatorReplica mapperOperator = new OperatorReplica( pipelineReplicaId1,
                                                                    operatorQueue,
                                                                    drainerPool,
                                                                    pipelineReplicaMeter,
                                                                    mapperInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { mapperOperatorDef },
                                                                    new InternalInvocationCtx[] { mapperInvocationCtx } );
        final PipelineReplica pipeline = new PipelineReplica( pipelineReplicaId1,
                                                              new OperatorReplica[] { mapperOperator },
                                                              new EmptyOperatorQueue( "map", mapperOperatorDef.getInputPortCount() ),
                                                              pipelineReplicaMeter );
        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );

        pipeline.init( new SchedulingStrategy[][] { { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) } },
                       new UpstreamCtx[][] { { createInitialClosedUpstreamCtx( 1 ) } } );

        final TupleQueueDownstreamCollector tupleCollector = new TupleQueueDownstreamCollector( mapperOperatorDef.getOutputPortCount() );
        final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig, pipeline, supervisor, tupleCollector );

        final Thread runnerThread = spawnThread( runner );

        final int initialVal = 1 + new Random().nextInt( 98 );
        final int tupleCount = 200;
        for ( int i = 0; i < tupleCount; i++ )
        {
            final Tuple tuple = Tuple.of( "val", initialVal + i );
            operatorQueue.offer( 0, singletonList( tuple ) );
        }

        assertTrueEventually( () -> assertEquals( tupleCount, tupleCollector.tupleQueues[ 0 ].size() ) );
        final List<Tuple> tuples = tupleCollector.tupleQueues[ 0 ].poll( Integer.MAX_VALUE );
        for ( int i = 0; i < tupleCount; i++ )
        {
            final Tuple expected = new Tuple();
            final Tuple t = Tuple.of( "val", initialVal + i );
            multiplyBy2.accept( t, expected );
            assertEquals( expected, tuples.get( i ) );
        }

        final UpstreamCtx updatedUpstreamCtx = createInitialClosedUpstreamCtx( 1 ).withConnectionClosed( 0 );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( updatedUpstreamCtx );
        runner.updatePipelineUpstreamCtx();
        runnerThread.join();
    }

    @Test
    public void testPipelineWithMultipleOperators_pipelineUpstreamClosed () throws InterruptedException
    {
        final BiConsumer<Tuple, Tuple> add1 = ( input, output ) -> output.set( "val", 1 + input.getIntegerValueOrDefault( "val", -1 ) );
        final OperatorConfig mapperOperatorConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER, add1 );
        final OperatorDef mapperOperatorDef = OperatorDefBuilder.newInstance( "map", MapperOperator.class )
                                                                .setConfig( mapperOperatorConfig )
                                                                .build();

        final Predicate<Tuple> filterEvenVals = tuple -> tuple.getInteger( "val" ) % 2 == 0;
        final OperatorConfig filterOperatorConfig = new OperatorConfig().set( PREDICATE_CONFIG_PARAMETER, filterEvenVals );
        final OperatorDef filterOperatorDef = OperatorDefBuilder.newInstance( "filter", FilterOperator.class )
                                                                .setConfig( filterOperatorConfig )
                                                                .build();

        final PipelineReplicaMeter pipelineReplicaMeter = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                    pipelineReplicaId1,
                                                                                    mapperOperatorDef );

        final OperatorQueue mapperOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           mapperOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           MULTI_THREADED );

        final TupleQueueDrainerPool mapperDrainerPool = new BlockingTupleQueueDrainerPool( jokerConfig, mapperOperatorDef );
        final DefaultInvocationCtx mapperInvocationCtx = new DefaultInvocationCtx( mapperOperatorDef.getInputPortCount(),
                                                                                   key -> null,
                                                                                   new DefaultOutputCollector( mapperOperatorDef
                                                                                                                       .getOutputPortCount() ) );
        final OperatorReplica mapperOperator = new OperatorReplica( pipelineReplicaId1,
                                                                    mapperOperatorQueue,
                                                                    mapperDrainerPool,
                                                                    pipelineReplicaMeter,
                                                                    mapperInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { mapperOperatorDef },
                                                                    new InternalInvocationCtx[] { mapperInvocationCtx } );

        final OperatorQueue filterOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           filterOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           SINGLE_THREADED );
        final TupleQueueDrainerPool filterDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, filterOperatorDef );
        final DefaultInvocationCtx filterInvocationCtx = new DefaultInvocationCtx( filterOperatorDef.getInputPortCount(),
                                                                                   key -> null,
                                                                                   new DefaultOutputCollector( filterOperatorDef
                                                                                                                       .getOutputPortCount() ) );
        final OperatorReplica filterOperator = new OperatorReplica( pipelineReplicaId1,
                                                                    filterOperatorQueue,
                                                                    filterDrainerPool,
                                                                    pipelineReplicaMeter,
                                                                    filterInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { filterOperatorDef },
                                                                    new InternalInvocationCtx[] { filterInvocationCtx } );

        final PipelineReplica pipeline = new PipelineReplica( pipelineReplicaId1,
                                                              new OperatorReplica[] { mapperOperator, filterOperator },
                                                              new EmptyOperatorQueue( "map", mapperOperatorDef.getInputPortCount() ),
                                                              pipelineReplicaMeter );

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );

        pipeline.init( new SchedulingStrategy[][] { { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) },
                                                    { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) } },
                       new UpstreamCtx[][] { { createInitialClosedUpstreamCtx( 1 ) }, { createInitialClosedUpstreamCtx( 1 ) } } );

        final TupleQueueDownstreamCollector tupleCollector = new TupleQueueDownstreamCollector( filterOperatorDef.getOutputPortCount() );

        final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig, pipeline, supervisor, tupleCollector );

        final Thread runnerThread = spawnThread( runner );

        final int initialVal = 2 + 2 * new Random().nextInt( 98 );
        final int tupleCount = 200;

        for ( int i = 0; i < tupleCount; i++ )
        {
            final int value = initialVal + i;
            final Tuple tuple = Tuple.of( "val", value );
            mapperOperatorQueue.offer( 0, singletonList( tuple ) );
        }

        final int evenValCount = tupleCount / 2;
        assertTrueEventually( () -> assertEquals( evenValCount, tupleCollector.tupleQueues[ 0 ].size() ) );
        final List<Tuple> tuples = tupleCollector.tupleQueues[ 0 ].poll( Integer.MAX_VALUE );
        for ( int i = 0; i < evenValCount; i++ )
        {
            final Tuple expected = new Tuple();
            final Tuple t = Tuple.of( "val", initialVal + ( i * 2 ) );
            add1.accept( t, expected );
            if ( filterEvenVals.test( expected ) )
            {
                assertEquals( expected, tuples.get( i ) );
            }
        }

        final UpstreamCtx updatedUpstreamCtx = createInitialClosedUpstreamCtx( 1 ).withConnectionClosed( 0 );
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( updatedUpstreamCtx );
        runner.updatePipelineUpstreamCtx();
        runnerThread.join();
    }

    @Test
    public void testPipelineWithMultipleOperators_pipelineUpstreamClosed_0inputOperator () throws InterruptedException
    {
        final int batchCount = 4;

        final OperatorConfig generatorOperatorConfig = new OperatorConfig().set( "batchCount", batchCount );
        final OperatorDef generatorOperatorDef = OperatorDefBuilder.newInstance( "generator", ValueGeneratorOperator.class )
                                                                   .setConfig( generatorOperatorConfig )
                                                                   .build();

        final int passerBatchCount = batchCount / 2;
        final OperatorConfig passerOperatorConfig = new OperatorConfig().set( "batchCount", passerBatchCount );
        final OperatorDef passerOperatorDef = OperatorDefBuilder.newInstance( "passer", ValuePasserOperator.class )
                                                                .setConfig( passerOperatorConfig )
                                                                .build();

        final OperatorDef stateOperatorDef = OperatorDefBuilder.newInstance( "state", ValueStateOperator.class )
                                                               .setPartitionFieldNames( singletonList( "val" ) )
                                                               .build();

        final PipelineReplicaMeter pipelineReplicaMeter = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                    pipelineReplicaId1,
                                                                                    generatorOperatorDef );

        final OperatorQueue generatorOperatorQueue = new EmptyOperatorQueue( generatorOperatorDef.getId(),
                                                                             generatorOperatorDef.getInputPortCount() );
        final TupleQueueDrainerPool generatorDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, generatorOperatorDef );
        final DefaultInvocationCtx generatorInvocationCtx = new DefaultInvocationCtx( generatorOperatorDef.getInputPortCount(),
                                                                                      k -> null,
                                                                                      new DefaultOutputCollector( generatorOperatorDef
                                                                                                                          .getOutputPortCount() ) );
        final OperatorReplica generatorOperator = new OperatorReplica( pipelineReplicaId1,
                                                                       generatorOperatorQueue,
                                                                       generatorDrainerPool,
                                                                       pipelineReplicaMeter,
                                                                       generatorInvocationCtx::createInputTuples,
                                                                       new OperatorDef[] { generatorOperatorDef },
                                                                       new InternalInvocationCtx[] { generatorInvocationCtx } );

        final OperatorQueue passerOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           passerOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           SINGLE_THREADED );
        final TupleQueueDrainerPool passerDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, passerOperatorDef );
        final DefaultInvocationCtx passerInvocationCtx = new DefaultInvocationCtx( passerOperatorDef.getInputPortCount(),
                                                                                   k -> null,
                                                                                   new DefaultOutputCollector( passerOperatorDef
                                                                                                                       .getOutputPortCount() ) );
        final OperatorReplica passerOperator = new OperatorReplica( pipelineReplicaId1,
                                                                    passerOperatorQueue,
                                                                    passerDrainerPool,
                                                                    pipelineReplicaMeter,
                                                                    passerInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { passerOperatorDef },
                                                                    new InternalInvocationCtx[] { passerInvocationCtx } );

        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 1 );
        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.createPartitionedKVStores( REGION_ID,
                                                                                                     "state",
                                                                                                     partitionDistribution );

        final OperatorQueue[] stateOperatorQueues = operatorQueueManager.createPartitionedQueues( REGION_ID,
                                                                                                  stateOperatorDef,
                                                                                                  partitionDistribution );
        final OperatorQueue stateOperatorQueue = stateOperatorQueues[ 0 ];
        final TupleQueueDrainerPool stateDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, stateOperatorDef );
        final DefaultInvocationCtx stateInvocationCtx = new DefaultInvocationCtx( stateOperatorDef.getInputPortCount(),
                                                                                  operatorKvStores[ 0 ]::getKVStore,
                                                                                  new DefaultOutputCollector( stateOperatorDef
                                                                                                                      .getOutputPortCount
                                                                                                                               () ) );
        final OperatorReplica stateOperator = new OperatorReplica( pipelineReplicaId1,
                                                                   stateOperatorQueue,
                                                                   stateDrainerPool,
                                                                   pipelineReplicaMeter,
                                                                   stateInvocationCtx::createInputTuples,
                                                                   new OperatorDef[] { stateOperatorDef },
                                                                   new InternalInvocationCtx[] { stateInvocationCtx } );

        final PipelineReplica pipeline = new PipelineReplica( pipelineReplicaId1,
                                                              new OperatorReplica[] { generatorOperator, passerOperator, stateOperator },
                                                              new EmptyOperatorQueue( "generator",
                                                                                      generatorOperatorDef.getInputPortCount() ),
                                                              pipelineReplicaMeter );

        final Supervisor supervisor = mock( Supervisor.class );
        when( supervisor.getDownstreamCollector( pipelineReplicaId1 ) ).thenReturn( mock( DownstreamCollector.class ) );

        pipeline.init( new SchedulingStrategy[][] { { ScheduleWhenAvailable.INSTANCE },
                                                    { scheduleWhenTuplesAvailableOnDefaultPort( EXACT, passerBatchCount ) },
                                                    { scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 1 ) } },
                       new UpstreamCtx[][] { { createSourceOperatorInitialUpstreamCtx() },
                                             { createInitialClosedUpstreamCtx( 1 ) },
                                             { createInitialClosedUpstreamCtx( 1 ) } } );

        final PipelineReplicaRunner runner = new PipelineReplicaRunner( jokerConfig, pipeline, supervisor,
                                                                        mock( DownstreamCollector.class ) );

        final Thread runnerThread = spawnThread( runner );

        final ValueGeneratorOperator generatorOp = (ValueGeneratorOperator) generatorOperator.getOperator( 0 );
        generatorOp.start = true;

        assertTrueEventually( () -> assertTrue( generatorOp.count > 1000 ) );

        final UpstreamCtx updatedUpstreamCtx = createSourceOperatorShutdownUpstreamCtx();
        when( supervisor.getUpstreamCtx( pipelineReplicaId1 ) ).thenReturn( updatedUpstreamCtx );
        runner.updatePipelineUpstreamCtx();

        assertTrueEventually( () -> verify( supervisor ).notifyPipelineReplicaCompleted( pipelineReplicaId1 ) );

        runnerThread.join();

        final ValuePasserOperator passerOp = (ValuePasserOperator) passerOperator.getOperator( 0 );
        final ValueStateOperator stateOp = (ValueStateOperator) stateOperator.getOperator( 0 );

        assertEquals( generatorOp.count, passerOp.count );
        assertEquals( generatorOp.count, stateOp.count );
        assertEquals( generatorOp.count, getKVStoreTotalItemCount( REGION_ID, "state" ) );
    }

    @Test
    public void testMultiplePipelines_singleInputPort () throws InterruptedException
    {
        final SupervisorImpl supervisor = new SupervisorImpl();
        supervisor.upstreamCtxes.put( pipelineReplicaId1, createInitialClosedUpstreamCtx( 1 ) );
        supervisor.upstreamCtxes.put( pipelineReplicaId2, createInitialClosedUpstreamCtx( 1 ) );
        supervisor.inputPortIndices.put( pipelineReplicaId1, 0 );
        supervisor.inputPortIndices.put( pipelineReplicaId2, 0 );

        final BiConsumer<Tuple, Tuple> add1 = ( input, output ) -> output.set( "val", 1 + input.getIntegerValueOrDefault( "val", -1 ) );
        final OperatorConfig mapperOperatorConfig = new OperatorConfig().set( MAPPER_CONFIG_PARAMETER, add1 );
        final OperatorDef mapperOperatorDef = OperatorDefBuilder.newInstance( "map", MapperOperator.class )
                                                                .setConfig( mapperOperatorConfig )
                                                                .build();

        final OperatorQueue mapperOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           mapperOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           MULTI_THREADED );

        final TupleQueueDrainerPool mapperDrainerPool = new BlockingTupleQueueDrainerPool( jokerConfig, mapperOperatorDef );
        final DefaultInvocationCtx mapperInvocationCtx = new DefaultInvocationCtx( mapperOperatorDef.getInputPortCount(),
                                                                                   key -> null,
                                                                                   new DefaultOutputCollector( mapperOperatorDef
                                                                                                                       .getOutputPortCount() ) );
        final PipelineReplicaMeter pipelineReplicaMeter1 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId1,
                                                                                     mapperOperatorDef );
        final OperatorReplica mapperOperator = new OperatorReplica( pipelineReplicaId1,
                                                                    mapperOperatorQueue,
                                                                    mapperDrainerPool,
                                                                    pipelineReplicaMeter1,
                                                                    mapperInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { mapperOperatorDef },
                                                                    new InternalInvocationCtx[] { mapperInvocationCtx } );

        final PipelineReplica pipeline1 = new PipelineReplica( pipelineReplicaId1,
                                                               new OperatorReplica[] { mapperOperator },
                                                               new EmptyOperatorQueue( "map", mapperOperatorDef.getInputPortCount() ),
                                                               pipelineReplicaMeter1 );

        pipeline1.init( new SchedulingStrategy[][] { { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId1 ) } } );

        final Predicate<Tuple> filterEvenVals = tuple -> tuple.getInteger( "val" ) % 2 == 0;
        final OperatorConfig filterOperatorConfig = new OperatorConfig().set( PREDICATE_CONFIG_PARAMETER, filterEvenVals );
        final OperatorDef filterOperatorDef = OperatorDefBuilder.newInstance( "filter", FilterOperator.class )
                                                                .setConfig( filterOperatorConfig )
                                                                .build();

        final OperatorQueue filterOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           filterOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           MULTI_THREADED );

        final DownstreamCollectorImpl tupleCollector1 = new DownstreamCollectorImpl( filterOperatorQueue, new Pair[] { Pair.of( 0, 0 ) } );

        final TupleQueueDrainerPool filterDrainerPool = new BlockingTupleQueueDrainerPool( jokerConfig, filterOperatorDef );
        final DefaultInvocationCtx filterInvocationCtx = new DefaultInvocationCtx( filterOperatorDef.getInputPortCount(),
                                                                                   key -> null,
                                                                                   new DefaultOutputCollector( filterOperatorDef
                                                                                                                       .getOutputPortCount() ) );

        final PipelineReplicaMeter pipelineReplicaMeter2 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId2,
                                                                                     filterOperatorDef );
        final OperatorReplica filterOperator = new OperatorReplica( pipelineReplicaId2,
                                                                    filterOperatorQueue,
                                                                    filterDrainerPool,
                                                                    pipelineReplicaMeter2,
                                                                    filterInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { filterOperatorDef },
                                                                    new InternalInvocationCtx[] { filterInvocationCtx } );

        final PipelineReplica pipeline2 = new PipelineReplica( pipelineReplicaId2,
                                                               new OperatorReplica[] { filterOperator },
                                                               new EmptyOperatorQueue( "filter", filterOperatorDef.getInputPortCount() ),
                                                               pipelineReplicaMeter2 );

        pipeline2.init( new SchedulingStrategy[][] { { scheduleWhenTuplesAvailableOnDefaultPort( 1 ) } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId2 ) } } );

        final PipelineReplicaRunner runner1 = new PipelineReplicaRunner( jokerConfig, pipeline1, supervisor, tupleCollector1 );
        supervisor.downstreamCollectors.put( pipeline1.id(), tupleCollector1 );

        final TupleQueueDownstreamCollector tupleCollector2 = new TupleQueueDownstreamCollector( filterOperatorDef.getOutputPortCount() );

        final PipelineReplicaRunner runner2 = new PipelineReplicaRunner( jokerConfig, pipeline2, supervisor, tupleCollector2 );
        supervisor.downstreamCollectors.put( pipeline2.id(), tupleCollector2 );

        supervisor.targetPipelineReplicaId = pipelineReplicaId2;
        supervisor.runner = runner2;

        final Thread runnerThread1 = spawnThread( runner1 );
        final Thread runnerThread2 = spawnThread( runner2 );

        final int initialVal = 2 + 2 * new Random().nextInt( 98 );
        final int tupleCount = 200;

        for ( int i = 0; i < tupleCount; i++ )
        {
            final int value = initialVal + i;
            final Tuple tuple = Tuple.of( "val", value );
            mapperOperatorQueue.offer( 0, singletonList( tuple ) );
        }

        final int evenValCount = tupleCount / 2;
        assertTrueEventually( () -> assertEquals( evenValCount, tupleCollector2.tupleQueues[ 0 ].size() ) );
        final List<Tuple> tuples = tupleCollector2.tupleQueues[ 0 ].poll( Integer.MAX_VALUE );
        for ( int i = 0; i < evenValCount; i++ )
        {
            final Tuple expected = new Tuple();
            final Tuple t = Tuple.of( "val", initialVal + ( i * 2 ) );
            add1.accept( t, expected );
            if ( filterEvenVals.test( expected ) )
            {
                assertEquals( expected, tuples.get( i ) );
            }
        }

        supervisor.upstreamCtxes.put( pipelineReplicaId1, createInitialClosedUpstreamCtx( 1 ).withConnectionClosed( 0 ) );
        runner1.updatePipelineUpstreamCtx();
        runnerThread1.join();
        runnerThread2.join();
        assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId1 ) );
        assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId2 ) );
    }

    @Test
    public void testMultiplePipelines_multipleInputPorts () throws InterruptedException
    {
        final SupervisorImpl supervisor = new SupervisorImpl();
        supervisor.upstreamCtxes.put( pipelineReplicaId1, createSourceOperatorInitialUpstreamCtx() );
        supervisor.upstreamCtxes.put( pipelineReplicaId2, createSourceOperatorInitialUpstreamCtx() );
        supervisor.upstreamCtxes.put( pipelineReplicaId3, createInitialClosedUpstreamCtx( 2 ) );
        supervisor.inputPortIndices.put( pipelineReplicaId1, 0 );
        supervisor.inputPortIndices.put( pipelineReplicaId2, 1 );

        final int batchCount = 4;

        final OperatorConfig generatorOperatorConfig1 = new OperatorConfig().set( "batchCount", batchCount );
        final OperatorDef generatorOperatorDef1 = OperatorDefBuilder.newInstance( "generator1", ValueGeneratorOperator.class )
                                                                    .setConfig( generatorOperatorConfig1 )
                                                                    .build();

        final OperatorQueue generatorOperatorQueue1 = new EmptyOperatorQueue( generatorOperatorDef1.getId(),

                                                                              generatorOperatorDef1.getInputPortCount() );
        final TupleQueueDrainerPool generatorDrainerPool1 = new NonBlockingTupleQueueDrainerPool( jokerConfig, generatorOperatorDef1 );

        final PipelineReplicaMeter pipelineReplicaMeter1 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId1,
                                                                                     generatorOperatorDef1 );
        final DefaultInvocationCtx generatorInvocationCtx1 = new DefaultInvocationCtx( generatorOperatorDef1.getInputPortCount(),
                                                                                       k -> null,
                                                                                       new DefaultOutputCollector(
                                                                                               generatorOperatorDef1.getOutputPortCount()
                                                                                       ) );
        final OperatorReplica generatorOperator1 = new OperatorReplica( pipelineReplicaId1,
                                                                        generatorOperatorQueue1,
                                                                        generatorDrainerPool1,
                                                                        pipelineReplicaMeter1,
                                                                        generatorInvocationCtx1::createInputTuples,
                                                                        new OperatorDef[] { generatorOperatorDef1 },
                                                                        new InternalInvocationCtx[] { generatorInvocationCtx1 } );

        final PipelineReplica pipeline1 = new PipelineReplica( pipelineReplicaId1,
                                                               new OperatorReplica[] { generatorOperator1 },
                                                               new EmptyOperatorQueue( "generator1",
                                                                                       generatorOperatorDef1.getInputPortCount() ),
                                                               pipelineReplicaMeter1 );

        pipeline1.init( new SchedulingStrategy[][] { { ScheduleWhenAvailable.INSTANCE } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId1 ) } } );

        final OperatorConfig generatorOperatorConfig2 = new OperatorConfig().set( "batchCount", batchCount ).set( "increment", false );
        final OperatorDef generatorOperatorDef2 = OperatorDefBuilder.newInstance( "generator2", ValueGeneratorOperator.class )
                                                                    .setConfig( generatorOperatorConfig2 )
                                                                    .build();

        final OperatorQueue generatorOperatorQueue2 = new EmptyOperatorQueue( generatorOperatorDef2.getId(),

                                                                              generatorOperatorDef2.getInputPortCount() );
        final TupleQueueDrainerPool generatorDrainerPool2 = new NonBlockingTupleQueueDrainerPool( jokerConfig, generatorOperatorDef2 );
        final DefaultInvocationCtx generatorInvocationCtx2 = new DefaultInvocationCtx( generatorOperatorDef2.getInputPortCount(),
                                                                                       k -> null,
                                                                                       new DefaultOutputCollector(
                                                                                               generatorOperatorDef2.getOutputPortCount()
                                                                                       ) );

        final PipelineReplicaMeter pipelineReplicaMeter2 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId2,
                                                                                     generatorOperatorDef2 );
        final OperatorReplica generatorOperator2 = new OperatorReplica( pipelineReplicaId2,
                                                                        generatorOperatorQueue2,
                                                                        generatorDrainerPool2,
                                                                        pipelineReplicaMeter2,
                                                                        generatorInvocationCtx2::createInputTuples,
                                                                        new OperatorDef[] { generatorOperatorDef2 },
                                                                        new InternalInvocationCtx[] { generatorInvocationCtx2 } );

        final PipelineReplica pipeline2 = new PipelineReplica( pipelineReplicaId2,
                                                               new OperatorReplica[] { generatorOperator2 },
                                                               new EmptyOperatorQueue( "generator2",
                                                                                       generatorOperatorDef2.getInputPortCount() ),
                                                               pipelineReplicaMeter2 );

        pipeline2.init( new SchedulingStrategy[][] { { ScheduleWhenAvailable.INSTANCE } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId2 ) } } );

        final OperatorConfig sinkOperatorConfig = new OperatorConfig();
        final OperatorDef sinkOperatorDef = OperatorDefBuilder.newInstance( "sink", ValueSinkOperator.class )
                                                              .setConfig( sinkOperatorConfig )
                                                              .build();

        final int passerBatchCount = batchCount / 2;
        final OperatorConfig passerOperatorConfig = new OperatorConfig().set( "batchCount", passerBatchCount );
        final OperatorDef passerOperatorDef = OperatorDefBuilder.newInstance( "passer", ValuePasserOperator.class )
                                                                .setConfig( passerOperatorConfig )
                                                                .build();

        final OperatorDef stateOperatorDef = OperatorDefBuilder.newInstance( "state", ValueStateOperator.class )
                                                               .setPartitionFieldNames( singletonList( "val" ) )
                                                               .build();

        final PipelineReplicaMeter pipelineReplicaMeter3 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId3,
                                                                                     sinkOperatorDef );

        final OperatorQueue sinkOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                         sinkOperatorDef,
                                                                                         REPLICA_INDEX,
                                                                                         MULTI_THREADED );
        final TupleQueueDrainerPool sinkDrainerPool = new BlockingTupleQueueDrainerPool( jokerConfig, sinkOperatorDef );
        final OperatorKVStore sinkOperatorKVStore = operatorKVStoreManager.createDefaultKVStore( REGION_ID, "sink" );
        final DefaultInvocationCtx sinkInvocationCtx = new DefaultInvocationCtx( sinkOperatorDef.getInputPortCount(),
                                                                                 sinkOperatorKVStore::getKVStore,
                                                                                 new DefaultOutputCollector( sinkOperatorDef
                                                                                                                     .getOutputPortCount
                                                                                                                              () ) );
        final OperatorReplica sinkOperator = new OperatorReplica( pipelineReplicaId3,
                                                                  sinkOperatorQueue,
                                                                  sinkDrainerPool,
                                                                  pipelineReplicaMeter3,
                                                                  sinkInvocationCtx::createInputTuples,
                                                                  new OperatorDef[] { sinkOperatorDef },
                                                                  new InternalInvocationCtx[] { sinkInvocationCtx } );

        final OperatorQueue passerOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           passerOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           SINGLE_THREADED );
        final TupleQueueDrainerPool passerDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, passerOperatorDef );
        final DefaultInvocationCtx passerInvocationCtx = new DefaultInvocationCtx( passerOperatorDef.getInputPortCount(),
                                                                                   k -> null,
                                                                                   new DefaultOutputCollector( passerOperatorDef
                                                                                                                       .getOutputPortCount() ) );
        final OperatorReplica passerOperator = new OperatorReplica( pipelineReplicaId3,
                                                                    passerOperatorQueue,
                                                                    passerDrainerPool,
                                                                    pipelineReplicaMeter3,
                                                                    passerInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { passerOperatorDef },
                                                                    new InternalInvocationCtx[] { passerInvocationCtx } );
        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 1 );
        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.createPartitionedKVStores( REGION_ID, "state",

                                                                                                     partitionDistribution );
        final OperatorQueue[] stateOperatorQueues = operatorQueueManager.createPartitionedQueues( REGION_ID,
                                                                                                  stateOperatorDef,
                                                                                                  partitionDistribution );
        final OperatorQueue stateOperatorQueue = stateOperatorQueues[ 0 ];
        final TupleQueueDrainerPool stateDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, stateOperatorDef );
        final DefaultInvocationCtx stateInvocationCtx = new DefaultInvocationCtx( stateOperatorDef.getInputPortCount(),
                                                                                  operatorKvStores[ 0 ]::getKVStore,
                                                                                  new DefaultOutputCollector( stateOperatorDef
                                                                                                                      .getOutputPortCount
                                                                                                                               () ) );
        final OperatorReplica stateOperator = new OperatorReplica( pipelineReplicaId3,
                                                                   stateOperatorQueue,
                                                                   stateDrainerPool,
                                                                   pipelineReplicaMeter3,
                                                                   stateInvocationCtx::createInputTuples,
                                                                   new OperatorDef[] { stateOperatorDef },
                                                                   new InternalInvocationCtx[] { stateInvocationCtx } );

        final PipelineReplica pipeline3 = new PipelineReplica( pipelineReplicaId3,
                                                               new OperatorReplica[] { sinkOperator, passerOperator, stateOperator },
                                                               new EmptyOperatorQueue( "sink", sinkOperatorDef.getInputPortCount() ),
                                                               pipelineReplicaMeter3 );

        pipeline3.init( new SchedulingStrategy[][] { { scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 ) },
                                                     { scheduleWhenTuplesAvailableOnDefaultPort( EXACT, passerBatchCount ) },
                                                     { scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 1 ) } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId3 ) },
                                              { createInitialClosedUpstreamCtx( 1 ) },
                                              { createInitialClosedUpstreamCtx( 1 ) } } );

        final DownstreamCollectorImpl collector1 = new DownstreamCollectorImpl( sinkOperatorQueue, new Pair[] { Pair.of( 0, 0 ) } );
        final PipelineReplicaRunner runner1 = new PipelineReplicaRunner( jokerConfig, pipeline1, supervisor, collector1 );
        supervisor.downstreamCollectors.put( pipeline1.id(), collector1 );

        final DownstreamCollectorImpl collector2 = new DownstreamCollectorImpl( sinkOperatorQueue, new Pair[] { Pair.of( 0, 1 ) } );
        final PipelineReplicaRunner runner2 = new PipelineReplicaRunner( jokerConfig, pipeline2, supervisor, collector2 );
        supervisor.downstreamCollectors.put( pipeline2.id(), collector2 );

        final DownstreamCollectorImpl collector3 = new DownstreamCollectorImpl( null, new Pair[] {} );
        final PipelineReplicaRunner runner3 = new PipelineReplicaRunner( jokerConfig, pipeline3, supervisor, collector3 );
        supervisor.downstreamCollectors.put( pipeline3.id(), collector3 );

        supervisor.targetPipelineReplicaId = pipelineReplicaId3;
        supervisor.runner = runner3;

        final Thread runnerThread1 = spawnThread( runner1 );
        final Thread runnerThread2 = spawnThread( runner2 );
        final Thread runnerThread3 = spawnThread( runner3 );

        final ValueGeneratorOperator generatorOp1 = (ValueGeneratorOperator) generatorOperator1.getOperator( 0 );
        generatorOp1.start = true;

        final ValueGeneratorOperator generatorOp2 = (ValueGeneratorOperator) generatorOperator2.getOperator( 0 );

        assertTrueEventually( () -> assertTrue( generatorOp1.count > 5000 ) );
        supervisor.upstreamCtxes.put( pipelineReplicaId1, createSourceOperatorShutdownUpstreamCtx() );
        runner1.updatePipelineUpstreamCtx();

        generatorOp2.start = true;
        assertTrueEventually( () -> assertTrue( generatorOp2.count < -5000 ) );
        supervisor.upstreamCtxes.put( pipelineReplicaId2, createSourceOperatorShutdownUpstreamCtx() );
        runner2.updatePipelineUpstreamCtx();

        assertTrueEventually( () -> assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId1 ) ) );
        assertTrueEventually( () -> assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId2 ) ) );
        assertTrueEventually( () -> assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId3 ) ) );

        runnerThread1.join();
        runnerThread2.join();
        runnerThread3.join();

        final ValuePasserOperator passerOp = (ValuePasserOperator) passerOperator.getOperator( 0 );
        final ValueStateOperator stateOp = (ValueStateOperator) stateOperator.getOperator( 0 );

        final int totalCount = generatorOp1.count + Math.abs( generatorOp2.count );
        assertEquals( totalCount, passerOp.count );
        assertEquals( totalCount, stateOp.count );
        assertEquals( totalCount, getKVStoreTotalItemCount( REGION_ID, "state" ) );
    }

    @Test
    public void testMultiplePipelines_partitionedStatefulDownstreamPipeline () throws InterruptedException
    {
        final SupervisorImpl supervisor = new SupervisorImpl();
        supervisor.upstreamCtxes.put( pipelineReplicaId1, createSourceOperatorInitialUpstreamCtx() );
        supervisor.upstreamCtxes.put( pipelineReplicaId2, createInitialClosedUpstreamCtx( 1 ) );
        supervisor.inputPortIndices.put( pipelineReplicaId1, 0 );
        supervisor.inputPortIndices.put( pipelineReplicaId2, 0 );

        final int batchCount = 4;
        final OperatorConfig generatorOperatorConfig = new OperatorConfig().set( "batchCount", batchCount );
        final OperatorDef generatorOperatorDef = OperatorDefBuilder.newInstance( "generator", ValueGeneratorOperator.class )
                                                                   .setConfig( generatorOperatorConfig )
                                                                   .build();

        final int passerBatchCount = batchCount / 2;
        final OperatorConfig passerOperatorConfig = new OperatorConfig().set( "batchCount", passerBatchCount );
        final OperatorDef passerOperatorDef = OperatorDefBuilder.newInstance( "passer", ValuePasserOperator.class )
                                                                .setConfig( passerOperatorConfig )
                                                                .build();

        final PipelineReplicaMeter pipelineReplicaMeter1 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId1,
                                                                                     generatorOperatorDef );

        final OperatorQueue generatorOperatorQueue = new EmptyOperatorQueue( generatorOperatorDef.getId(),
                                                                             generatorOperatorDef.getInputPortCount() );
        final DefaultInvocationCtx generatorInvocationCtx = new DefaultInvocationCtx( generatorOperatorDef.getInputPortCount(),
                                                                                      k -> null,
                                                                                      new DefaultOutputCollector( generatorOperatorDef
                                                                                                                          .getOutputPortCount() ) );
        final TupleQueueDrainerPool generatorDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, generatorOperatorDef );

        final OperatorReplica generatorOperator = new OperatorReplica( pipelineReplicaId1,
                                                                       generatorOperatorQueue,
                                                                       generatorDrainerPool,
                                                                       pipelineReplicaMeter1,
                                                                       generatorInvocationCtx::createInputTuples,
                                                                       new OperatorDef[] { generatorOperatorDef },
                                                                       new InternalInvocationCtx[] { generatorInvocationCtx } );

        final OperatorQueue passerOperatorQueue = operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                           passerOperatorDef,
                                                                                           REPLICA_INDEX,
                                                                                           SINGLE_THREADED );

        final TupleQueueDrainerPool passerDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, passerOperatorDef );
        final DefaultInvocationCtx passerInvocationCtx = new DefaultInvocationCtx( passerOperatorDef.getInputPortCount(),
                                                                                   k -> null,
                                                                                   new DefaultOutputCollector( passerOperatorDef
                                                                                                                       .getOutputPortCount() ) );

        final OperatorReplica passerOperator = new OperatorReplica( pipelineReplicaId1,
                                                                    passerOperatorQueue,
                                                                    passerDrainerPool,
                                                                    pipelineReplicaMeter1,
                                                                    passerInvocationCtx::createInputTuples,
                                                                    new OperatorDef[] { passerOperatorDef },
                                                                    new InternalInvocationCtx[] { passerInvocationCtx } );

        final PartitionDistribution partitionDistribution = partitionService.createPartitionDistribution( REGION_ID, 1 );
        final OperatorKVStore[] operatorKvStores = operatorKVStoreManager.createPartitionedKVStores( REGION_ID,
                                                                                                     "state",
                                                                                                     partitionDistribution );

        final OperatorDef stateOperatorDef = OperatorDefBuilder.newInstance( "state", ValueStateOperator.class )
                                                               .setPartitionFieldNames( singletonList( "val" ) )
                                                               .build();

        final OperatorQueue[] stateOperatorQueues = operatorQueueManager.createPartitionedQueues( REGION_ID,
                                                                                                  stateOperatorDef,
                                                                                                  partitionDistribution );
        final OperatorQueue stateOperatorQueue = stateOperatorQueues[ 0 ];

        final TupleQueueDrainerPool stateDrainerPool = new NonBlockingTupleQueueDrainerPool( jokerConfig, stateOperatorDef );

        final PipelineReplicaMeter pipelineReplicaMeter2 = new PipelineReplicaMeter( jokerConfig.getMetricManagerConfig().getTickMask(),
                                                                                     pipelineReplicaId2,
                                                                                     stateOperatorDef );

        final DefaultInvocationCtx stateInvocationCtx = new DefaultInvocationCtx( stateOperatorDef.getInputPortCount(),
                                                                                  operatorKvStores[ 0 ]::getKVStore,
                                                                                  new DefaultOutputCollector( stateOperatorDef
                                                                                                                      .getOutputPortCount
                                                                                                                               () ) );
        final OperatorReplica stateOperator = new OperatorReplica( pipelineReplicaId2,
                                                                   stateOperatorQueue,
                                                                   stateDrainerPool,
                                                                   pipelineReplicaMeter2,
                                                                   stateInvocationCtx::createInputTuples,
                                                                   new OperatorDef[] { stateOperatorDef },
                                                                   new InternalInvocationCtx[] { stateInvocationCtx } );

        final PipelineReplica pipeline1 = new PipelineReplica( pipelineReplicaId1,
                                                               new OperatorReplica[] { generatorOperator, passerOperator },
                                                               new EmptyOperatorQueue( "generator",
                                                                                       generatorOperatorDef.getInputPortCount() ),
                                                               pipelineReplicaMeter1 );

        pipeline1.init( new SchedulingStrategy[][] { { ScheduleWhenAvailable.INSTANCE },
                                                     { scheduleWhenTuplesAvailableOnDefaultPort( EXACT, passerBatchCount ) } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId1 ) },
                                              { createInitialClosedUpstreamCtx( 1 ) } } );

        final PipelineReplica pipeline2 = new PipelineReplica( pipelineReplicaId2,
                                                               new OperatorReplica[] { stateOperator },
                                                               operatorQueueManager.createDefaultQueue( REGION_ID,
                                                                                                        stateOperatorDef,
                                                                                                        REPLICA_INDEX,
                                                                                                        MULTI_THREADED ),
                                                               pipelineReplicaMeter2 );

        pipeline2.init( new SchedulingStrategy[][] { { scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 1 ) } },
                        new UpstreamCtx[][] { { supervisor.upstreamCtxes.get( pipelineReplicaId2 ) } } );

        final DownstreamCollectorImpl collector1 = new DownstreamCollectorImpl( pipeline2.getEffectiveQueue(),
                                                                                new Pair[] { Pair.of( 0, 0 ) } );
        supervisor.downstreamCollectors.put( pipeline1.id(), collector1 );
        final PipelineReplicaRunner runner1 = new PipelineReplicaRunner( jokerConfig, pipeline1, supervisor, collector1 );

        final DownstreamCollector collector2 = mock( DownstreamCollector.class );
        final PipelineReplicaRunner runner2 = new PipelineReplicaRunner( jokerConfig, pipeline2, supervisor, collector2 );
        supervisor.downstreamCollectors.put( pipeline2.id(), collector2 );

        supervisor.targetPipelineReplicaId = pipelineReplicaId2;
        supervisor.runner = runner2;

        final Thread runnerThread1 = spawnThread( runner1 );
        final Thread runnerThread2 = spawnThread( runner2 );

        final ValueGeneratorOperator generatorOp = (ValueGeneratorOperator) generatorOperator.getOperator( 0 );
        generatorOp.start = true;

        assertTrueEventually( () -> assertTrue( generatorOp.count > 1000 ) );

        supervisor.upstreamCtxes.put( pipelineReplicaId1, createSourceOperatorShutdownUpstreamCtx() );
        runner1.updatePipelineUpstreamCtx();

        assertTrueEventually( () -> assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId1 ) ) );
        assertTrueEventually( () -> assertTrue( supervisor.completedPipelines.contains( pipelineReplicaId2 ) ) );

        runnerThread1.join();
        runnerThread2.join();

        final ValuePasserOperator passerOp = (ValuePasserOperator) passerOperator.getOperator( 0 );
        final ValueStateOperator stateOp = (ValueStateOperator) stateOperator.getOperator( 0 );

        assertEquals( generatorOp.count, passerOp.count );
        assertEquals( generatorOp.count, stateOp.count );
        assertEquals( generatorOp.count, getKVStoreTotalItemCount( REGION_ID, "state" ) );
    }

    private int getKVStoreTotalItemCount ( final int regionId, final String operatorId )
    {
        int count = 0;
        for ( KVStoreContainer container : operatorKVStoreManager.getKVStoreContainers( regionId, operatorId ) )
        {
            count += container.getKeyCount();
        }

        return count;
    }

    private static class TupleQueueDownstreamCollector implements DownstreamCollector
    {

        private final TupleQueue[] tupleQueues;

        TupleQueueDownstreamCollector ( final int portCount )
        {
            tupleQueues = new TupleQueue[ portCount ];
            for ( int i = 0; i < portCount; i++ )
            {
                tupleQueues[ i ] = new MultiThreadedTupleQueue( 1000 );
            }
        }

        @Override
        public void accept ( final TuplesImpl tuples )
        {
            for ( int i = 0; i < tuples.getPortCount(); i++ )
            {
                tupleQueues[ i ].offer( tuples.getTuples( i ) );
            }
        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 2, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ),

                                @PortSchema( portIndex = 1, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) } )
    public static class ValueSinkOperator implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return scheduleWhenTuplesAvailableOnAny( AT_LEAST, 2, 1, 0, 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            final List<Tuple> tuples0 = ctx.getInputTuples( 0 );
            final List<Tuple> tuples1 = ctx.getInputTuples( 1 );
            final int c = Math.min( tuples0.size(), tuples1.size() );

            for ( int i = 0; i < c; i++ )
            {
                final Tuple t0 = tuples0.get( i ).copyForAttachment();
                t0.attach( tuples1.get( i ) );
                ctx.output( t0 );
                final Tuple t1 = tuples1.get( i ).copyForAttachment();
                t1.attach( tuples0.get( i ) );
                ctx.output( t1 );
            }

            for ( int i = c; i < tuples0.size(); i++ )
            {
                ctx.output( tuples0.get( i ) );
            }

            for ( int i = c; i < tuples1.size(); i++ )
            {
                ctx.output( tuples1.get( i ) );
            }
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 0, outputPortCount = 1 )
    @OperatorSchema( outputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) } )
    public static class ValueGeneratorOperator implements Operator
    {

        private volatile boolean start;

        private int batchCount;

        private volatile int count;

        private boolean increment;

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            final OperatorConfig config = ctx.getConfig();
            batchCount = config.getInteger( "batchCount" );
            count = config.getIntegerOrDefault( "initial", 0 );
            increment = config.getBooleanOrDefault( "increment", true );
            return ScheduleWhenAvailable.INSTANCE;
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            if ( start )
            {
                for ( int i = 0; i < batchCount; i++ )
                {
                    final int val = increment ? ++count : --count;
                    ctx.output( Tuple.of( "val", val ) );
                }
            }
            else
            {
                sleepUninterruptibly( 1, TimeUnit.MICROSECONDS );
            }
        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) }, outputs = {
            @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) } )
    public static class ValuePasserOperator implements Operator
    {

        private volatile int count;

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            final int batchCount = ctx.getConfig().getInteger( "batchCount" );
            return scheduleWhenTuplesAvailableOnDefaultPort( EXACT, batchCount );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            final List<Tuple> tuples = ctx.getInputTuples( 0 );
            ctx.output( tuples );
            count += tuples.size();
        }

    }


    @OperatorSpec( type = PARTITIONED_STATEFUL, inputPortCount = 1, outputPortCount = 0 )
    @OperatorSchema( inputs = { @PortSchema( portIndex = 0, scope = EXACT_FIELD_SET, fields = { @SchemaField( name = "val", type = Integer.class ) } ) } )
    public static class ValueStateOperator implements Operator
    {

        private volatile int count;

        @Override
        public SchedulingStrategy init ( final InitCtx context )
        {
            return scheduleWhenTuplesAvailableOnDefaultPort( EXACT, 1 );
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {
            final KVStore kvStore = ctx.getKVStore();
            for ( Tuple tuple : ctx.getInputTuplesByDefaultPort() )
            {
                kvStore.set( "tuple", tuple );
            }

            count += ctx.getInputTupleCount( 0 );
        }

    }


    public static class DownstreamCollectorImpl implements DownstreamCollector
    {

        private final IdleStrategy idleStrategy = BackoffIdleStrategy.newDefaultInstance();

        private final OperatorQueue operatorQueue;

        private final Pair<Integer, Integer>[] ports;

        public DownstreamCollectorImpl ( final OperatorQueue operatorQueue, final Pair<Integer, Integer>[] ports )
        {
            this.operatorQueue = operatorQueue;
            this.ports = ports;
        }

        @Override
        public void accept ( final TuplesImpl input )
        {
            for ( Pair<Integer, Integer> p : ports )
            {
                final int outputPort = p._1;
                final int inputPort = p._2;
                if ( input.getTupleCount( outputPort ) > 0 )
                {
                    idleStrategy.reset();

                    final List<Tuple> tuples = input.getTuples( outputPort );
                    final int size = tuples.size();
                    int fromIndex = 0;
                    while ( true )
                    {
                        final int offered = operatorQueue.offer( inputPort, tuples, fromIndex );
                        fromIndex += offered;
                        if ( fromIndex == size )
                        {
                            break;
                        }
                        else if ( offered == 0 )
                        {
                            idleStrategy.idle();
                        }
                    }
                }
            }
        }

    }


    public static class SupervisorImpl implements Supervisor
    {

        private final Map<PipelineReplicaId, UpstreamCtx> upstreamCtxes = new ConcurrentHashMap<>();

        private final Map<PipelineReplicaId, DownstreamCollector> downstreamCollectors = new ConcurrentHashMap<>();

        private final Map<PipelineReplicaId, Integer> inputPortIndices = new HashMap<>();

        private final Set<PipelineReplicaId> completedPipelines = Collections.newSetFromMap( new ConcurrentHashMap<>() );

        private PipelineReplicaId targetPipelineReplicaId;

        private PipelineReplicaRunner runner;

        @Override
        public UpstreamCtx getUpstreamCtx ( final PipelineReplicaId id )
        {
            return upstreamCtxes.get( id );
        }

        @Override
        public DownstreamCollector getDownstreamCollector ( final PipelineReplicaId id )
        {
            return downstreamCollectors.get( id );
        }

        @Override
        public synchronized void notifyPipelineReplicaCompleted ( final PipelineReplicaId id )
        {
            assertTrue( completedPipelines.add( id ) );
            if ( !id.equals( targetPipelineReplicaId ) )
            {
                final UpstreamCtx currentUpstreamCtx = upstreamCtxes.get( targetPipelineReplicaId );
                final UpstreamCtx newUpstreamCtx = currentUpstreamCtx.withConnectionClosed( inputPortIndices.get( id ) );
                upstreamCtxes.put( targetPipelineReplicaId, newUpstreamCtx );
                runner.updatePipelineUpstreamCtx();
            }
        }

        @Override
        public void notifyPipelineReplicaFailed ( final PipelineReplicaId id, final Throwable failure )
        {
            System.err.println( "Pipeline Replica " + id + " failed with " + failure );
            failure.printStackTrace();
        }

    }

}
