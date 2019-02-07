package cs.bilkent.joker.engine.metric;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.pipeline.PipelineReplicaId;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;
import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineReplicaMeterTest extends AbstractJokerTest
{

    private static final PipelineReplicaId PIPELINE_REPLICA_ID = new PipelineReplicaId( 0, 0, 0 );

    private static final int INPUT_PORT_COUNT = 2;


    private final String headOperatorId = "head", tailOperatorId = "tail";

    private PipelineReplicaMeter pipelineReplicaMeter;

    @Before
    public void init ()
    {
        final OperatorDef headOperatorDef = mock( OperatorDef.class );
        when( headOperatorDef.getId() ).thenReturn( headOperatorId );
        when( headOperatorDef.getInputPortCount() ).thenReturn( INPUT_PORT_COUNT );
        pipelineReplicaMeter = new PipelineReplicaMeter( 1, PIPELINE_REPLICA_ID, headOperatorDef );
    }

    @Test
    public void shouldNotGetCurrentlyExecutingComponentInitially ()
    {
        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldGetCurrentlyExecutingComponentOnTick ()
    {
        pipelineReplicaMeter.tryTick();

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.tryTick();

        assertEquals( PIPELINE_REPLICA_ID, pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.tryTick();

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldNotSetExecutingOperatorOnNonTick ()
    {
        pipelineReplicaMeter.onInvocationStart( "id" );

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.tryTick();

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldSetExecutingOperatorOnTick ()
    {
        pipelineReplicaMeter.tryTick();
        pipelineReplicaMeter.tryTick();

        pipelineReplicaMeter.onInvocationStart( "id" );

        assertEquals( "id", pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldCompleteExecutingOperatorOnTick ()
    {
        pipelineReplicaMeter.tryTick();
        pipelineReplicaMeter.tryTick();

        pipelineReplicaMeter.onInvocationStart( "id" );

        assertEquals( "id", pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.onInvocationComplete( "id", Collections.emptyList(), 0, false );

        assertEquals( PIPELINE_REPLICA_ID, pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldCountInboundThroughputOnHeadOperator ()
    {
        final TuplesImpl tuples = new TuplesImpl( 2 );
        tuples.add( 0, new Tuple(), new Tuple() );
        tuples.add( 1, new Tuple() );

        pipelineReplicaMeter.onInvocationComplete( headOperatorId, Collections.singletonList( tuples ), 1, true );

        final long[] buffer = new long[] { 0, 0 };
        pipelineReplicaMeter.readThroughput( buffer );
        assertArrayEquals( new long[] { 2, 1 }, buffer );
    }

    @Test
    public void shouldNotCountInboundThroughputOnAnotherOperator ()
    {
        final TuplesImpl tuples = new TuplesImpl( 1 );
        tuples.add( 0, new Tuple(), new Tuple() );

        pipelineReplicaMeter.onInvocationComplete( tailOperatorId, Collections.singletonList( tuples ), 1, true );

        final long[] buffer = new long[] { 0, 0 };
        pipelineReplicaMeter.readThroughput( buffer );
        assertArrayEquals( new long[] { 0, 0 }, buffer );
    }

}
