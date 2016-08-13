package cs.bilkent.joker.engine.tuplequeue.impl;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.ThreadingPreference;
import static cs.bilkent.joker.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.joker.engine.partition.PartitionService;
import cs.bilkent.joker.engine.partition.PartitionServiceImpl;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyFunctionFactoryImpl;
import cs.bilkent.joker.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.joker.flow.OperatorDef;
import cs.bilkent.joker.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.spec.OperatorType;
import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.junit.Assert.assertTrue;


public class TupleQueueContextManagerImplTest extends AbstractJokerTest
{

    private TupleQueueContextManagerImpl tupleQueueManager;

    @Before
    public void init ()
    {
        final JokerConfig jokerConfig = new JokerConfig();
        final PartitionService partitionService = new PartitionServiceImpl( jokerConfig );
        tupleQueueManager = new TupleQueueContextManagerImpl( jokerConfig, partitionService, new PartitionKeyFunctionFactoryImpl() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContexteWithoutOperatorDef ()
    {
        tupleQueueManager.createDefaultTupleQueueContext( 1, 1, null, ThreadingPreference.SINGLE_THREADED );
    }

    @Test
    public void shouldCreateTupleQueueContextOnlyOnceForMultipleInvocations ()
    {
        final OperatorDef operatorDef = new OperatorDef( "op1",
                                                         Operator.class,
                                                         OperatorType.STATELESS,
                                                         1,
                                                         1,
                                                         new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                         new OperatorConfig(),
                                                         Collections.emptyList() );

        final TupleQueueContext tupleQueueContext1 = tupleQueueManager.createDefaultTupleQueueContext( 1, 1, operatorDef, MULTI_THREADED );
        final TupleQueueContext tupleQueueContext2 = tupleQueueManager.createDefaultTupleQueueContext( 1, 1, operatorDef, MULTI_THREADED );

        assertTrue( tupleQueueContext1 == tupleQueueContext2 );
    }

    @Test
    public void shouldCleanTupleQueueContextOnRelease ()
    {
        final OperatorDef operatorDef = new OperatorDef( "op1",
                                                         Operator.class,
                                                         OperatorType.STATELESS,
                                                         1,
                                                         1,
                                                         new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                         new OperatorConfig(),
                                                         Collections.emptyList() );
        final TupleQueueContext tupleQueueContext = tupleQueueManager.createDefaultTupleQueueContext( 1, 1, operatorDef, MULTI_THREADED );
        tupleQueueContext.offer( 0, Collections.singletonList( new Tuple() ) );
        assertTrue( tupleQueueManager.releaseDefaultTupleQueueContext( 1, 1, "op1" ) );
    }

}
