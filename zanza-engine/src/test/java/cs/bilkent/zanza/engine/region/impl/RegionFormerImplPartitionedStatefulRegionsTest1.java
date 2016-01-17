package cs.bilkent.zanza.engine.region.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import cs.bilkent.zanza.engine.region.impl.RegionFormerImplOperatorSequenceRegionsTest.PartitionedStatefulOperator;
import cs.bilkent.zanza.engine.region.impl.RegionFormerImplOperatorSequenceRegionsTest.StatelessOperator;
import static cs.bilkent.zanza.engine.region.impl.RegionFormerImplOperatorSequenceRegionsTest.assertPartitionedStatefulRegion;
import static cs.bilkent.zanza.engine.region.impl.RegionFormerImplOperatorSequenceRegionsTest.createOperator;
import cs.bilkent.zanza.flow.OperatorDefinition;
import cs.bilkent.zanza.region.RegionDefinition;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@RunWith( Parameterized.class )
public class RegionFormerImplPartitionedStatefulRegionsTest1
{
    @Parameters
    public static Collection<Object[]> parameters ()
    {
        return asList( new Object[][] { { singletonList( asList( "A", "B" ) ), asList( "A", "B" ), asList( "A", "B" ) },

                                        { asList( asList( "A", "B" ), asList( "A", "B" ) ), asList( "A", "B" ), asList( "A", "B" ) },

                                        { asList( singletonList( "A" ), asList( "A", "B" ) ), asList( "A", "B" ), singletonList( "A" ) },

                                        { asList( asList( "A", "B", "C" ), asList( "A", "B" ) ), asList( "A", "B" ), asList( "A", "B" ) },

                                        { asList( asList( "A", "C" ), asList( "A", "B" ) ), asList( "A", "B" ), singletonList( "A" ) },

                                        { singletonList( asList( "A", "B" ) ), asList( "A", "B" ), asList( "A", "B" ) },

                                        { asList( asList( "A", "B" ), asList( "A", "B" ) ), asList( "A", "B", "C" ), asList( "A", "B" ) },

                                        { asList( singletonList( "A" ), asList( "A", "B" ) ),
                                          asList( "A", "B", "C" ),
                                          singletonList( "A" ) },

                                        { asList( asList( "A", "B", "C" ), asList( "A", "B" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B" ) },

                                        { asList( asList( "A", "C" ), asList( "A", "B" ) ), asList( "A", "B", "C" ), singletonList( "A" ) },

                                        { singletonList( asList( "A", "B", "D" ) ), asList( "A", "B", "C" ), asList( "A", "B" ) },

                                        { asList( asList( "A", "B", "D" ), asList( "A", "B", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B" ) },

                                        { asList( singletonList( "A" ), asList( "A", "B", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          singletonList( "A" ) },

                                        { asList( asList( "A", "B", "D", "E" ), asList( "A", "B", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B" ) },

                                        { asList( asList( "A", "B", "E" ), asList( "A", "B", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B" ) },

                                        { singletonList( asList( "A", "B", "C", "D" ) ), asList( "A", "B", "C" ), asList( "A", "B", "C" ) },

                                        { asList( asList( "A", "B", "C", "D" ), asList( "A", "B", "C", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B", "C" ) },

                                        { asList( asList( "A", "B", "C" ), asList( "A", "B", "C" ) ),
                                          asList( "A", "B", "C", "D" ),
                                          asList( "A", "B", "C" ) },

                                        { asList( asList( "A", "B", "C", "D", "E" ), asList( "A", "B", "C", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B", "C" ) },

                                        { asList( asList( "A", "B", "C", "E" ), asList( "A", "B", "C", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B", "C" ) },

                                        { asList( asList( "A", "B", "D", "E" ), asList( "A", "B", "C", "D" ) ),
                                          asList( "A", "B", "C" ),
                                          asList( "A", "B" ) } } );
    }


    private final RegionFormerImpl regionFormer = new RegionFormerImpl();

    private final List<List<String>> statelessOperatorSchemas;

    private final List<String> partitionFieldNames;

    private final List<String> expectedPartitionFieldNames;

    public RegionFormerImplPartitionedStatefulRegionsTest1 ( final List<List<String>> statelessOperatorSchemas,
                                                             final List<String> partitionFieldNames,
                                                             final List<String> expectedPartitionFieldNames )
    {
        this.statelessOperatorSchemas = statelessOperatorSchemas;
        this.partitionFieldNames = partitionFieldNames;
        this.expectedPartitionFieldNames = expectedPartitionFieldNames;
    }

    @Test
    public void testRegionWithStatefulOperatorsAtBeginningAndPartitionedStatefulOperatorAtTheEnd ()
    {
        final List<OperatorDefinition> operators = new ArrayList<>();
        int operatorCount = 0;
        for ( List<String> statelessOperatorFieldNames : statelessOperatorSchemas )
        {
            operators.add( createOperator( "o" + ( ++operatorCount ), StatelessOperator.class, statelessOperatorFieldNames, emptyList() ) );
        }

        operators.add( createOperator( "o" + ( ++operatorCount ),
                                       PartitionedStatefulOperator.class,
                                       partitionFieldNames,
                                       partitionFieldNames ) );

        final List<RegionDefinition> regions = regionFormer.createRegions( operators );

        assertThat( regions, hasSize( 1 ) );
        assertPartitionedStatefulRegion( regions.get( 0 ), expectedPartitionFieldNames, operators );
    }

}
