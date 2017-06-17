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

public class MultiRegionFlowDefFactory implements FlowDefFactory
{

    public MultiRegionFlowDefFactory ()
    {
    }

    @Override
    public FlowDef createFlow ( final Config config, final JokerConfig jokerConfig )
    {
        final int keyRange = config.getInt( "keyRange" );
        final int valueRange = config.getInt( "valueRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final int keysPerInvocation = config.getInt( "keysPerInvocation" );
        final List<Integer> operatorCosts1 = Arrays.stream( config.getString( "operatorCosts1" ).split( "_" ) )
                                                   .map( Integer::parseInt )
                                                   .collect( toList() );
        final List<Integer> operatorCosts2 = Arrays.stream( config.getString( "operatorCosts2" ).split( "_" ) )
                                                   .map( Integer::parseInt )
                                                   .collect( toList() );

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( KEY_RANGE_CONFIG_PARAMETER, keyRange );
        beaconConfig.set( VALUE_RANGE_CONFIG_PARAMETER, valueRange );
        beaconConfig.set( TUPLES_PER_KEY_CONFIG_PARAMETER, tuplesPerKey );
        beaconConfig.set( KEYS_PER_INVOCATION_CONFIG_PARAMETER, keysPerInvocation );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", MemorizingBeaconOperator.class ).setConfig( beaconConfig ).build();

        flowDefBuilder.add( beacon );

        OperatorConfig ptioner1Config = new OperatorConfig();
        ptioner1Config.set( MULTIPLICATION_COUNT, operatorCosts1.get( 0 ) );

        OperatorDef ptioner1 = OperatorDefBuilder.newInstance( "m10", PartitionedStatefulMultiplierOperator.class )
                                                 .setConfig( ptioner1Config )
                                                 .setPartitionFieldNames( asList( "key1", "key2" ) )
                                                 .build();

        flowDefBuilder.add( ptioner1 );
        flowDefBuilder.connect( beacon.getId(), ptioner1.getId() );

        for ( int i = 1; i < operatorCosts1.size(); i++ )
        {
            OperatorConfig multiplierConfig = new OperatorConfig();
            multiplierConfig.set( MULTIPLICATION_COUNT, operatorCosts1.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m1" + i, StatelessMultiplierOperator.class )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m1" + ( i - 1 ), multiplier.getId() );
        }

        OperatorConfig ptioner2Config = new OperatorConfig();
        ptioner2Config.set( MULTIPLICATION_COUNT, operatorCosts2.get( 0 ) );

        OperatorDef ptioner2 = OperatorDefBuilder.newInstance( "m20", PartitionedStatefulMultiplierOperator.class )
                                                 .setConfig( ptioner2Config )
                                                 .setPartitionFieldNames( singletonList( "key1" ) )
                                                 .build();

        flowDefBuilder.add( ptioner2 );

        flowDefBuilder.connect( "m1" + ( operatorCosts1.size() - 1 ), ptioner2.getId() );

        for ( int i = 1; i < operatorCosts2.size(); i++ )
        {
            OperatorConfig multiplierConfig = new OperatorConfig();
            multiplierConfig.set( MULTIPLICATION_COUNT, operatorCosts2.get( i ) );

            OperatorDef multiplier = OperatorDefBuilder.newInstance( "m2" + i, StatelessMultiplierOperator.class )
                                                       .setConfig( multiplierConfig )
                                                       .build();

            flowDefBuilder.add( multiplier );
            flowDefBuilder.connect( "m2" + ( i - 1 ), multiplier.getId() );
        }

        return flowDefBuilder.build();
    }

}
