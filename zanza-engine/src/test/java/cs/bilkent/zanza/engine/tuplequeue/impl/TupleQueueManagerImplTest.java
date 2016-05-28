package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static org.junit.Assert.assertTrue;


public class TupleQueueManagerImplTest
{

    private final TupleQueueManagerImpl tupleQueueManager = new TupleQueueManagerImpl();

    @Before
    public void init ()
    {
        tupleQueueManager.init( new ZanzaConfig() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContexteWithoutOperatorDefinition ()
    {
        tupleQueueManager.createTupleQueueContext( null, ThreadingPreference.SINGLE_THREADED, 1 );
    }

    @Test
    public void shouldCreateTupleQueueContextOnlyOnceForMultipleInvocations ()
    {
        final OperatorDefinition operatorDefinition = new OperatorDefinition( "op1",
                                                                              Operator.class,
                                                                              OperatorType.STATELESS,
                                                                              1,
                                                                              1,
                                                                              new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                                              new OperatorConfig(),
                                                                              Collections.emptyList() );

        final TupleQueueContext tupleQueueContext1 = tupleQueueManager.createTupleQueueContext( operatorDefinition, MULTI_THREADED, 0 );
        final TupleQueueContext tupleQueueContext2 = tupleQueueManager.createTupleQueueContext( operatorDefinition, MULTI_THREADED, 0 );

        assertTrue( tupleQueueContext1 == tupleQueueContext2 );
    }

    @Test
    public void shouldCleanTupleQueueContextOnRelease ()
    {
        final OperatorDefinition operatorDefinition = new OperatorDefinition( "op1",
                                                                              Operator.class,
                                                                              OperatorType.STATELESS,
                                                                              1,
                                                                              1,
                                                                              new OperatorRuntimeSchemaBuilder( 1, 1 ).build(),
                                                                              new OperatorConfig(),
                                                                              Collections.emptyList() );
        final TupleQueueContext tupleQueueContext = tupleQueueManager.createTupleQueueContext( operatorDefinition, MULTI_THREADED, 0 );
        tupleQueueContext.offer( 0, Collections.singletonList( new Tuple() ) );
        assertTrue( tupleQueueManager.releaseTupleQueueContext( "op1", 0 ) );
    }

}
