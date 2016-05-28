package cs.bilkent.zanza.engine.pipeline.examples;

import java.util.List;
import java.util.Random;
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
import cs.bilkent.zanza.engine.coordinator.CoordinatorHandle;
import cs.bilkent.zanza.engine.kvstore.KVStoreProvider;
import cs.bilkent.zanza.engine.pipeline.CachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.pipeline.DownstreamTupleSender;
import cs.bilkent.zanza.engine.pipeline.NonCachedTuplesImplSupplier;
import cs.bilkent.zanza.engine.pipeline.OperatorInstance;
import cs.bilkent.zanza.engine.pipeline.PipelineInstance;
import cs.bilkent.zanza.engine.pipeline.PipelineInstanceId;
import cs.bilkent.zanza.engine.pipeline.PipelineInstanceRunner;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueue;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueManager;
import cs.bilkent.zanza.engine.tuplequeue.impl.TupleQueueManagerImpl;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.BlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.drainer.pool.NonBlockingTupleQueueDrainerPool;
import cs.bilkent.zanza.engine.tuplequeue.impl.queue.MultiThreadedTupleQueue;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.impl.TuplesImpl;
import cs.bilkent.zanza.operators.FilterOperator;
import cs.bilkent.zanza.operators.MapperOperator;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class OperatorPipelineInstanceRunnerTest
{

    private final ZanzaConfig zanzaConfig = new ZanzaConfig();

    private final TupleQueueManager tupleQueueManager = new TupleQueueManagerImpl();

    private final KVStoreProvider kvStoreProvider = key -> null;

    private final PipelineInstanceId pipelineInstanceId = new PipelineInstanceId( 0, 0, 0 );

    @Before
    public void init ()
    {
        tupleQueueManager.init( zanzaConfig );
    }

    @Test
    public void testPipelineExecutionWithSingleOperator () throws ExecutionException, InterruptedException
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

        final OperatorInstance operator = new OperatorInstance( pipelineInstanceId,
                                                                mapperOperatorDef,
                                                                tupleQueueContext,
                                                                kvStoreProvider,
                                                                drainerPool,
                                                                tuplesImplSupplier );
        final PipelineInstance pipeline = new PipelineInstance( pipelineInstanceId, new OperatorInstance[] { operator } );
        final PipelineInstanceRunner runner = new PipelineInstanceRunner( pipeline );

        runner.setCoordinator( mock( CoordinatorHandle.class ) );

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

        runner.stop();
        runnerThread.join();
    }

    @Test
    public void testPipelineExecutionWithMultipleOperators () throws ExecutionException, InterruptedException
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

        final OperatorInstance mapperOperator = new OperatorInstance( pipelineInstanceId,
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
        final Supplier<TuplesImpl> filterTuplesImplSupplier = new CachedTuplesImplSupplier( filterOperatorDef.inputPortCount() );

        final OperatorInstance filterOperator = new OperatorInstance( pipelineInstanceId,
                                                                      filterOperatorDef,
                                                                      filterTupleQueueContext,
                                                                      kvStoreProvider,
                                                                      filterDrainerPool,
                                                                      filterTuplesImplSupplier );

        final PipelineInstance pipeline = new PipelineInstance( pipelineInstanceId,
                                                                new OperatorInstance[] { mapperOperator, filterOperator } );
        final PipelineInstanceRunner runner = new PipelineInstanceRunner( pipeline );

        runner.setCoordinator( mock( CoordinatorHandle.class ) );

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
        System.out.println( "---" );
        tuples.stream().forEach( System.out::println );
        for ( int i = 0; i < evenValCount; i++ )
        {
            final Tuple expected = add1.apply( new Tuple( "val", initialVal + ( i * 2 ) ) );
            if ( filterEvenVals.test( expected ) )
            {
                expected.setSequenceNumber( ( i + 1 ) * 2 );
                assertEquals( expected, tuples.get( i ) );
            }
        }

        runner.stop();
        runnerThread.join();
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

}
