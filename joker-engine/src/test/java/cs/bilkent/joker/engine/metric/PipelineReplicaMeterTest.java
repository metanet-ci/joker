package cs.bilkent.joker.engine.metric;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import cs.bilkent.joker.engine.flow.PipelineId;
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

@Ignore
public class PipelineReplicaMeterTest extends AbstractJokerTest
{

    private static final PipelineReplicaId PIPELINE_REPLICA_ID = new PipelineReplicaId( new PipelineId( 0, 0 ), 0 );


    private final String headOperatorId = "head", tailOperatorId = "tail";

    private PipelineReplicaMeter pipelineReplicaMeter;

    @Before
    public void init ()
    {
        final OperatorDef headOperatorDef = mock( OperatorDef.class );
        when( headOperatorDef.getId() ).thenReturn( headOperatorId );
        when( headOperatorDef.getInputPortCount() ).thenReturn( 2 );
        final OperatorDef tailOperatorDef = mock( OperatorDef.class );
        when( tailOperatorDef.getId() ).thenReturn( tailOperatorId );
        when( tailOperatorDef.getOutputPortCount() ).thenReturn( 1 );
        pipelineReplicaMeter = new PipelineReplicaMeter( 1, PIPELINE_REPLICA_ID, headOperatorDef, tailOperatorDef );
    }

    @Test
    public void shouldNotGetCurrentlyExecutingComponentInitially ()
    {
        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldGetCurrentlyExecutingComponentOnTick ()
    {
        pipelineReplicaMeter.tick();

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.tick();

        assertEquals( PIPELINE_REPLICA_ID, pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.tick();

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldNotSetExecutingOperatorNotOnTick ()
    {
        pipelineReplicaMeter.startOperatorInvocation( "id" );

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.tick();

        assertNull( pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldSetExecutingOperatorOnTick ()
    {
        pipelineReplicaMeter.tick();
        pipelineReplicaMeter.tick();
        pipelineReplicaMeter.startOperatorInvocation( "id" );

        assertEquals( "id", pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldCompleteExecutingOperatorOnTick ()
    {
        pipelineReplicaMeter.tick();
        pipelineReplicaMeter.tick();
        pipelineReplicaMeter.startOperatorInvocation( "id" );

        assertEquals( "id", pipelineReplicaMeter.getCurrentlyExecutingComponent() );

        pipelineReplicaMeter.completeOperatorInvocation( "id" );

        assertEquals( PIPELINE_REPLICA_ID, pipelineReplicaMeter.getCurrentlyExecutingComponent() );
    }

    @Test
    public void shouldCountConsumedThroughputOnHeadOperator ()
    {
        final TuplesImpl tuples = new TuplesImpl( 2 );
        tuples.add( 0, new Tuple() );
        tuples.add( 0, new Tuple() );
        tuples.add( 1, new Tuple() );

        pipelineReplicaMeter.addConsumedTuples( headOperatorId, tuples );

        final long[] buffer = new long[] { 0, 0 };
        pipelineReplicaMeter.getConsumedTupleCounts( buffer );
        assertArrayEquals( new long[] { 2, 1 }, buffer );
    }

    @Test
    public void shouldNotCountConsumedThroughputOnAnotherOperator ()
    {
        final TuplesImpl tuples = new TuplesImpl( 1 );
        tuples.add( 0, new Tuple() );
        tuples.add( 0, new Tuple() );

        pipelineReplicaMeter.addConsumedTuples( tailOperatorId, tuples );

        final long[] buffer = new long[] { 0, 0 };
        pipelineReplicaMeter.getConsumedTupleCounts( buffer );
        assertArrayEquals( new long[] { 0, 0 }, buffer );
    }

    @Test
    public void shouldCountProducedThroughputOnTailOperator ()
    {
        final TuplesImpl tuples = new TuplesImpl( 1 );
        tuples.add( 0, new Tuple() );
        tuples.add( 0, new Tuple() );

        pipelineReplicaMeter.addProducedTuples( tailOperatorId, tuples );

        final long[] buffer = new long[] { 0 };
        pipelineReplicaMeter.getProducedTupleCounts( buffer );
        assertArrayEquals( new long[] { 2 }, buffer );
    }

    @Test
    public void shouldNotCountProducedThroughputOnAnotherOperator ()
    {
        final TuplesImpl tuples = new TuplesImpl( 2 );
        tuples.add( 0, new Tuple() );
        tuples.add( 1, new Tuple() );

        pipelineReplicaMeter.addProducedTuples( headOperatorId, tuples );

        final long[] buffer = new long[] { 0 };
        pipelineReplicaMeter.getProducedTupleCounts( buffer );
        assertArrayEquals( new long[] { 0 }, buffer );
    }

}
