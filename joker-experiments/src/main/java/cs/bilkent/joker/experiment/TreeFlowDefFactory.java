package cs.bilkent.joker.experiment;

import java.util.Arrays;
import java.util.List;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.experiment.BaseMultiplierOperator.MULTIPLICATION_COUNT;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEYS_PER_INVOCATION_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEY_RANGE_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.TUPLES_PER_KEY_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.VALUE_RANGE_CONFIG_PARAMETER;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class TreeFlowDefFactory implements FlowDefFactory
{

    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int keyRange = config.getInt( "keyRange" );
        final int valueRange = config.getInt( "valueRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final int keysPerInvocation = config.getInt( "keysPerInvocation" );
        final List<Integer> operatorCostsDown1 = Arrays.stream( config.getString( "operatorCostsDown1" ).split( "_" ) )
                                                       .map( Integer::parseInt )
                                                       .collect( toList() );
        final List<Integer> operatorCostsDown2 = Arrays.stream( config.getString( "operatorCostsDown2" ).split( "_" ) )
                                                       .map( Integer::parseInt )
                                                       .collect( toList() );
        final List<Integer> operatorCostsUp = Arrays.stream( config.getString( "operatorCostsUp" ).split( "_" ) )
                                                    .map( Integer::parseInt )
                                                    .collect( toList() );

        final List<List<Integer>> downstreamOperatorCosts = asList( operatorCostsDown1, operatorCostsDown2 );

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( KEY_RANGE_CONFIG_PARAMETER, keyRange );
        beaconConfig.set( VALUE_RANGE_CONFIG_PARAMETER, valueRange );
        beaconConfig.set( TUPLES_PER_KEY_CONFIG_PARAMETER, tuplesPerKey );
        beaconConfig.set( KEYS_PER_INVOCATION_CONFIG_PARAMETER, keysPerInvocation );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", MemorizingBeaconOperator.class ).setConfig( beaconConfig ).build();

        flowDefBuilder.add( beacon );

        OperatorConfig ptioner1Config = new OperatorConfig();
        ptioner1Config.set( MULTIPLICATION_COUNT, operatorCostsUp.get( 0 ) );

        OperatorDef ptioner1 = OperatorDefBuilder.newInstance( "m10", PartitionedStatefulMultiplierOperator.class )
                                                 .setConfig( ptioner1Config )
                                                 .setPartitionFieldNames( singletonList( "key1" ) )
                                                 .build();

        flowDefBuilder.add( ptioner1 );

        flowDefBuilder.connect( beacon.getId(), ptioner1.getId() );

        for ( int i = 1; i < operatorCostsUp.size(); i++ )
        {
            OperatorConfig multiplierConfig = new OperatorConfig();
            multiplierConfig.set( MULTIPLICATION_COUNT, operatorCostsUp.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m1" + i, StatelessMultiplierOperator.class )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m1" + ( i - 1 ), multiplier.getId() );
        }

        for ( int i = 0; i < downstreamOperatorCosts.size(); i++ )
        {
            final List<Integer> operatorCosts = downstreamOperatorCosts.get( i );
            OperatorConfig ptioner3Config = new OperatorConfig();
            ptioner3Config.set( MULTIPLICATION_COUNT, operatorCosts.get( 0 ) );

            OperatorDef ptioner3 = OperatorDefBuilder.newInstance( "m2" + i + "0", PartitionedStatefulMultiplierOperator.class )
                                                     .setConfig( ptioner3Config )
                                                     .setPartitionFieldNames( asList( "key1", "key2" ) )
                                                     .build();

            flowDefBuilder.add( ptioner3 );
            flowDefBuilder.connect( "m1" + ( operatorCostsUp.size() - 1 ), ptioner3.getId() );

            for ( int j = 1; j < operatorCosts.size(); j++ )
            {
                OperatorConfig multiplierConfig = new OperatorConfig();
                multiplierConfig.set( MULTIPLICATION_COUNT, operatorCosts.get( j ) );

                OperatorDef multiplier = OperatorDefBuilder.newInstance( "m2" + i + "" + j, StatelessMultiplierOperator.class )
                                                           .setConfig( multiplierConfig )
                                                           .build();

                flowDefBuilder.add( multiplier );
                flowDefBuilder.connect( "m2" + i + "" + ( j - 1 ), multiplier.getId() );
            }
        }

        return flowDefBuilder.build();
    }

}
