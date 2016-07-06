package cs.bilkent.zanza.engine.tuplequeue.impl;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.zanza.engine.config.ThreadingPreference;
import static cs.bilkent.zanza.engine.config.ThreadingPreference.MULTI_THREADED;
import cs.bilkent.zanza.engine.config.ZanzaConfig;
import cs.bilkent.zanza.engine.partition.PartitionService;
import cs.bilkent.zanza.engine.partition.PartitionServiceImpl;
import cs.bilkent.zanza.engine.partition.impl.PartitionKeyFunctionFactoryImpl;
import cs.bilkent.zanza.engine.tuplequeue.TupleQueueContext;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorRuntimeSchemaBuilder;
import cs.bilkent.zanza.operator.Operator;
import cs.bilkent.zanza.operator.OperatorConfig;
import cs.bilkent.zanza.operator.Tuple;
import cs.bilkent.zanza.operator.spec.OperatorType;
import static org.junit.Assert.assertTrue;


public class TupleQueueContextManagerImplTest
{

    private TupleQueueContextManagerImpl tupleQueueManager;

    @Before
    public void init ()
    {
        final ZanzaConfig zanzaConfig = new ZanzaConfig();
        final PartitionService partitionService = new PartitionServiceImpl( zanzaConfig );
        tupleQueueManager = new TupleQueueContextManagerImpl( zanzaConfig, partitionService, new PartitionKeyFunctionFactoryImpl() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotCreateTupleQueueContexteWithoutOperatorDefinition ()
    {
        tupleQueueManager.createDefaultTupleQueueContext( 1, 1, null, ThreadingPreference.SINGLE_THREADED );
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

        final TupleQueueContext tupleQueueContext1 = tupleQueueManager.createDefaultTupleQueueContext( 1,
                                                                                                       1,
                                                                                                       operatorDefinition,
                                                                                                       MULTI_THREADED );
        final TupleQueueContext tupleQueueContext2 = tupleQueueManager.createDefaultTupleQueueContext( 1,
                                                                                                       1,
                                                                                                       operatorDefinition,
                                                                                                       MULTI_THREADED );

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
        final TupleQueueContext tupleQueueContext = tupleQueueManager.createDefaultTupleQueueContext( 1,
                                                                                                      1,
                                                                                                      operatorDefinition,
                                                                                                      MULTI_THREADED );
        tupleQueueContext.offer( 0, Collections.singletonList( new Tuple() ) );
        assertTrue( tupleQueueManager.releaseDefaultTupleQueueContext( 1, 1, "op1" ) );
    }

}
