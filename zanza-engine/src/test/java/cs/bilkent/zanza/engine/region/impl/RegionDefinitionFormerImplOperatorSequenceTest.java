package cs.bilkent.zanza.engine.region.impl;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import cs.bilkent.zanza.flow.FlowDefinition;
import cs.bilkent.zanza.flow.FlowDefinitionBuilder;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.flow.OperatorDefinitionBuilder;
import cs.bilkent.zanza.operators.MapperOperator;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;

public class RegionDefinitionFormerImplOperatorSequenceTest
{

    private final RegionDefinitionFormerImpl regionFormer = new RegionDefinitionFormerImpl();

    private final FlowDefinitionBuilder flowBuilder = new FlowDefinitionBuilder();


    @Test
    public void testFlow1 ()
    {
        /**
         * O1 --> O2
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 1 ) );

        final List<OperatorDefinition> operators = operatorSequences.iterator().next();
        assertThat( operators, hasSize( 2 ) );
        assertThat( operators.get( 0 ).id(), equalTo( "o1" ) );
        assertThat( operators.get( 1 ).id(), equalTo( "o2" ) );
    }

    @Test
    public void testFlow2 ()
    {
        /**
         * O1 --> O2 --> O3
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o2", "o3" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 1 ) );

        final List<OperatorDefinition> operators = operatorSequences.iterator().next();
        assertThat( operators, hasSize( 3 ) );
        assertThat( operators.get( 0 ).id(), equalTo( "o1" ) );
        assertThat( operators.get( 1 ).id(), equalTo( "o2" ) );
        assertThat( operators.get( 2 ).id(), equalTo( "o3" ) );
    }

    @Test
    public void testFlow3 ()
    {
        /**
         *
         *                   /--> O4
         *                  /
         * O1 --> O2 --> O3
         *                  \
         *                   \--> O5
         *
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o5", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o2", "o3" );
        flowBuilder.connect( "o3", "o4" );
        flowBuilder.connect( "o3", "o5" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 3 ) );
        assertOperatorSequence( asList( "o1", "o2", "o3" ), operatorSequences );
        assertOperatorSequence( singletonList( "o4" ), operatorSequences );
        assertOperatorSequence( singletonList( "o5" ), operatorSequences );
    }

    @Test
    public void testFlow4 ()
    {
        /**
         *
         * O1 --> O2 --> O3 --> O5
         *             /
         *       O4 --/
         *
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o5", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o2", "o3" );
        flowBuilder.connect( "o4", "o3" );
        flowBuilder.connect( "o3", "o5" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 3 ) );
        assertOperatorSequence( asList( "o1", "o2" ), operatorSequences );
        assertOperatorSequence( asList( "o3", "o5" ), operatorSequences );
        assertOperatorSequence( singletonList( "o4" ), operatorSequences );
    }

    @Test
    public void testFlow5 ()
    {
        /**
         *
         * O1 --> O2 --> O3
         *             /
         *       O4 --/
         *
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o2", "o3" );
        flowBuilder.connect( "o4", "o3" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 3 ) );
        assertOperatorSequence( asList( "o1", "o2" ), operatorSequences );
        assertOperatorSequence( singletonList( "o3" ), operatorSequences );
        assertOperatorSequence( singletonList( "o4" ), operatorSequences );
    }

    @Test
    public void testFlow6 ()
    {
        /**
         *
         *     /--> O2
         *    /        \
         * O1           --> O4 --> 05
         *    \        /
         *     \--> O3
         *
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o5", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o1", "o3" );
        flowBuilder.connect( "o2", "o4" );
        flowBuilder.connect( "o3", "o4" );
        flowBuilder.connect( "o4", "o5" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 4 ) );
        assertOperatorSequence( singletonList( "o1" ), operatorSequences );
        assertOperatorSequence( singletonList( "o2" ), operatorSequences );
        assertOperatorSequence( singletonList( "o3" ), operatorSequences );
        assertOperatorSequence( asList( "o4", "o5" ), operatorSequences );
    }

    @Test
    public void testFlow7 ()
    {
        /**
         *
         *              /--> O4
         *             /
         *     /--> O2
         *    /        \
         * O1           \--> O5
         *    \
         *     \--> O3
         *
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o5", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o1", "o3" );
        flowBuilder.connect( "o2", "o4" );
        flowBuilder.connect( "o2", "o5" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 5 ) );
        assertOperatorSequence( singletonList( "o1" ), operatorSequences );
        assertOperatorSequence( singletonList( "o2" ), operatorSequences );
        assertOperatorSequence( singletonList( "o3" ), operatorSequences );
        assertOperatorSequence( singletonList( "o4" ), operatorSequences );
        assertOperatorSequence( singletonList( "o5" ), operatorSequences );
    }

    @Test
    public void testFlow8 ()
    {
        /**
         *
         *         O5 --\
         *               \
         *     /--> O2 --> O4
         *    /
         * O1
         *    \
         *     \--> O3
         *
         */

        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o1", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o2", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o3", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o4", MapperOperator.class ) );
        flowBuilder.add( OperatorDefinitionBuilder.newInstance( "o5", MapperOperator.class ) );
        flowBuilder.connect( "o1", "o2" );
        flowBuilder.connect( "o1", "o3" );
        flowBuilder.connect( "o2", "o4" );
        flowBuilder.connect( "o5", "o4" );
        final FlowDefinition flow = flowBuilder.build();

        final Collection<List<OperatorDefinition>> operatorSequences = regionFormer.createOperatorSequences( flow );

        assertThat( operatorSequences, hasSize( 5 ) );
        assertOperatorSequence( singletonList( "o1" ), operatorSequences );
        assertOperatorSequence( singletonList( "o2" ), operatorSequences );
        assertOperatorSequence( singletonList( "o3" ), operatorSequences );
        assertOperatorSequence( singletonList( "o4" ), operatorSequences );
        assertOperatorSequence( singletonList( "o5" ), operatorSequences );
    }

    private void assertOperatorSequence ( final List<String> expectedOperatorIds, Collection<List<OperatorDefinition>> operatorSequences )
    {
        final boolean sequenceExists = operatorSequences.stream().anyMatch( operatorSequence -> {
            final List<String> sequenceOperatorIds = operatorSequence.stream().map( op -> op.id() ).collect( Collectors.toList() );
            return sequenceOperatorIds.equals( expectedOperatorIds );
        } );

        assertTrue( sequenceExists );
    }

}
