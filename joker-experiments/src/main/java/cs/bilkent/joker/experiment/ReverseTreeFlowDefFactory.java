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

public class ReverseTreeFlowDefFactory implements FlowDefFactory
{

    @Override
    public FlowDef createFlow ( final Config config, final JokerConfig jokerConfig )
    {
        final int keyRange = config.getInt( "keyRange" );
        final int valueRange = config.getInt( "valueRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final int keysPerInvocation = config.getInt( "keysPerInvocation" );
        final List<Integer> operatorCostsUp1 = Arrays.stream( config.getString( "operatorCostsUp1" ).split( "_" ) )
                                                     .map( Integer::parseInt )
                                                     .collect( toList() );
        final List<Integer> operatorCostsUp2 = Arrays.stream( config.getString( "operatorCostsUp2" ).split( "_" ) )
                                                     .map( Integer::parseInt )
                                                     .collect( toList() );
        final List<Integer> operatorCostsDown = Arrays.stream( config.getString( "operatorCostsDown" ).split( "_" ) )
                                                      .map( Integer::parseInt )
                                                      .collect( toList() );

        final List<List<Integer>> upstreamOperatorCosts = asList( operatorCostsUp1, operatorCostsUp2 );

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( KEY_RANGE_CONFIG_PARAMETER, keyRange );
        beaconConfig.set( VALUE_RANGE_CONFIG_PARAMETER, valueRange );
        beaconConfig.set( TUPLES_PER_KEY_CONFIG_PARAMETER, tuplesPerKey );
        beaconConfig.set( KEYS_PER_INVOCATION_CONFIG_PARAMETER, keysPerInvocation );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", MemorizingBeaconOperator.class ).setConfig( beaconConfig ).build();

        flowDefBuilder.add( beacon );

        OperatorConfig ptioner2Config = new OperatorConfig();
        ptioner2Config.set( MULTIPLICATION_COUNT, operatorCostsDown.get( 0 ) );

        OperatorDef ptioner2 = OperatorDefBuilder.newInstance( "m20", PartitionedStatefulMultiplierOperator.class )
                                                 .setConfig( ptioner2Config )
                                                 .setPartitionFieldNames( singletonList( "key1" ) )
                                                 .build();

        flowDefBuilder.add( ptioner2 );

        for ( int i = 0; i < upstreamOperatorCosts.size(); i++ )
        {
            final List<Integer> operatorCosts = upstreamOperatorCosts.get( i );
            OperatorConfig ptioner1Config = new OperatorConfig();
            ptioner1Config.set( MULTIPLICATION_COUNT, operatorCosts.get( 0 ) );

            OperatorDef ptioner1 = OperatorDefBuilder.newInstance( "m1" + i + "0", PartitionedStatefulMultiplierOperator.class )
                                                     .setConfig( ptioner1Config )
                                                     .setPartitionFieldNames( asList( "key1", "key2" ) )
                                                     .build();

            flowDefBuilder.add( ptioner1 );
            flowDefBuilder.connect( beacon.getId(), ptioner1.getId() );

            for ( int j = 1; j < operatorCosts.size(); j++ )
            {
                OperatorConfig multiplierConfig = new OperatorConfig();
                multiplierConfig.set( MULTIPLICATION_COUNT, operatorCosts.get( j ) );

                OperatorDef multiplier = OperatorDefBuilder.newInstance( "m1" + i + "" + j, StatelessMultiplierOperator.class )
                                                           .setConfig( multiplierConfig )
                                                           .build();

                flowDefBuilder.add( multiplier );
                flowDefBuilder.connect( "m1" + i + "" + ( j - 1 ), multiplier.getId() );
            }

            flowDefBuilder.connect( "m1" + i + "" + ( operatorCosts.size() - 1 ), ptioner2.getId() );
        }

        for ( int i = 1; i < operatorCostsDown.size(); i++ )
        {
            OperatorConfig multiplierConfig = new OperatorConfig();
            multiplierConfig.set( MULTIPLICATION_COUNT, operatorCostsDown.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m2" + i, StatelessMultiplierOperator.class )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m2" + ( i - 1 ), multiplier.getId() );
        }

        return flowDefBuilder.build();
    }

}
