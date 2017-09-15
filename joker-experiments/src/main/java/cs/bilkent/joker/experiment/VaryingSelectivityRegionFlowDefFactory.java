package cs.bilkent.joker.experiment;

import java.util.function.Predicate;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import static cs.bilkent.joker.experiment.BaseMultiplierOperator.MULTIPLICATION_COUNT;
import static cs.bilkent.joker.experiment.DuplicatorOperator.DUPLICATE_COUNT_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEYS_PER_INVOCATION_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.KEY_RANGE_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.TUPLES_PER_KEY_CONFIG_PARAMETER;
import static cs.bilkent.joker.experiment.MemorizingBeaconOperator.VALUE_RANGE_CONFIG_PARAMETER;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.FilterOperator;
import static cs.bilkent.joker.operators.FilterOperator.PREDICATE_CONFIG_PARAMETER;
import static java.util.Collections.singletonList;

public class VaryingSelectivityRegionFlowDefFactory implements FlowDefFactory
{

    @Override
    public FlowDef createFlow ( final JokerConfig jokerConfig )
    {
        final Config config = jokerConfig.getRootConfig();
        final int keyRange = config.getInt( "keyRange" );
        final int valueRange = config.getInt( "valueRange" );
        final int tuplesPerKey = config.getInt( "tuplesPerKey" );
        final int keysPerInvocation = config.getInt( "keysPerInvocation" );

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( KEY_RANGE_CONFIG_PARAMETER, keyRange );
        beaconConfig.set( VALUE_RANGE_CONFIG_PARAMETER, valueRange );
        beaconConfig.set( TUPLES_PER_KEY_CONFIG_PARAMETER, tuplesPerKey );
        beaconConfig.set( KEYS_PER_INVOCATION_CONFIG_PARAMETER, keysPerInvocation );

        OperatorDef beacon = OperatorDefBuilder.newInstance( "bc", MemorizingBeaconOperator.class ).setConfig( beaconConfig ).build();

        flowDefBuilder.add( beacon );

        OperatorConfig ptionerConfig = new OperatorConfig();
        ptionerConfig.set( MULTIPLICATION_COUNT, 1024 );

        OperatorDef ptioner = OperatorDefBuilder.newInstance( "m0", PartitionedStatefulMultiplierOperator.class )
                                                .setConfig( ptionerConfig )
                                                .setPartitionFieldNames( singletonList( "key1" ) )
                                                .build();

        flowDefBuilder.add( ptioner ).connect( beacon.getId(), ptioner.getId() );

        final OperatorRuntimeSchemaBuilder s1SchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        s1SchemaBuilder.addInputField( 0, "key1", Integer.class )
                       .addInputField( 0, "key2", Integer.class )
                       .addInputField( 0, "val1", Integer.class )
                       .addInputField( 0, "val2", Integer.class )
                       .addOutputField( 0, "key1", Integer.class )
                       .addOutputField( 0, "key2", Integer.class )
                       .addOutputField( 0, "val1", Integer.class )
                       .addOutputField( 0, "val2", Integer.class );

        final OperatorConfig s1Config = new OperatorConfig();
        s1Config.set( DUPLICATE_COUNT_PARAMETER, 4 );

        final OperatorDef s1 = OperatorDefBuilder.newInstance( "s1", DuplicatorOperator.class )
                                                 .setExtendingSchema( s1SchemaBuilder )
                                                 .setConfig( s1Config )
                                                 .build();

        flowDefBuilder.add( s1 ).connect( ptioner.getId(), s1.getId() );

        final OperatorConfig multiplier1Config = new OperatorConfig();
        multiplier1Config.set( MULTIPLICATION_COUNT, 256 );

        final OperatorDef multiplier1 = OperatorDefBuilder.newInstance( "m1", StatelessMultiplierOperator.class )
                                                          .setConfig( multiplier1Config )
                                                          .build();

        flowDefBuilder.add( multiplier1 ).connect( s1.getId(), multiplier1.getId() );

        final OperatorRuntimeSchemaBuilder s2SchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        s2SchemaBuilder.addInputField( 0, "key1", Integer.class )
                       .addInputField( 0, "key2", Integer.class )
                       .addInputField( 0, "val1", Integer.class )
                       .addInputField( 0, "val2", Integer.class )
                       .addOutputField( 0, "key1", Integer.class )
                       .addOutputField( 0, "key2", Integer.class )
                       .addOutputField( 0, "val1", Integer.class )
                       .addOutputField( 0, "val2", Integer.class );

        final OperatorConfig s2Config = new OperatorConfig();
        s2Config.set( PREDICATE_CONFIG_PARAMETER, new StaticSelectivityPredicate( 4 ) );

        final OperatorDef s2 = OperatorDefBuilder.newInstance( "s2", FilterOperator.class )
                                                 .setExtendingSchema( s2SchemaBuilder )
                                                 .setConfig( s2Config )
                                                 .build();

        flowDefBuilder.add( s2 ).connect( multiplier1.getId(), s2.getId() );

        final OperatorConfig multiplier2Config = new OperatorConfig();
        multiplier2Config.set( MULTIPLICATION_COUNT, 1024 );

        final OperatorDef multiplier2 = OperatorDefBuilder.newInstance( "m2", StatelessMultiplierOperator.class )
                                                          .setConfig( multiplier2Config )
                                                          .build();

        flowDefBuilder.add( multiplier2 ).connect( s2.getId(), multiplier2.getId() );

        final OperatorRuntimeSchemaBuilder s3SchemaBuilder = new OperatorRuntimeSchemaBuilder( 1, 1 );
        s3SchemaBuilder.addInputField( 0, "key1", Integer.class )
                       .addInputField( 0, "key2", Integer.class )
                       .addInputField( 0, "val1", Integer.class )
                       .addInputField( 0, "val2", Integer.class )
                       .addOutputField( 0, "key1", Integer.class )
                       .addOutputField( 0, "key2", Integer.class )
                       .addOutputField( 0, "val1", Integer.class )
                       .addOutputField( 0, "val2", Integer.class );

        final OperatorConfig s3Config = new OperatorConfig();
        s3Config.set( PREDICATE_CONFIG_PARAMETER, new StaticSelectivityPredicate( 2 ) );

        final OperatorDef s3 = OperatorDefBuilder.newInstance( "s3", FilterOperator.class )
                                                 .setExtendingSchema( s3SchemaBuilder )
                                                 .setConfig( s3Config )
                                                 .build();

        flowDefBuilder.add( s3 ).connect( multiplier2.getId(), s3.getId() );

        final OperatorConfig multiplier3Config = new OperatorConfig();
        multiplier3Config.set( MULTIPLICATION_COUNT, 2048 );

        final OperatorDef multiplier3 = OperatorDefBuilder.newInstance( "m3", StatelessMultiplierOperator.class )
                                                          .setConfig( multiplier3Config )
                                                          .build();

        flowDefBuilder.add( multiplier3 ).connect( s3.getId(), multiplier3.getId() );

        return flowDefBuilder.build();
    }


    private static class StaticSelectivityPredicate implements Predicate<Tuple>
    {

        private final int mod;

        private int i;

        public StaticSelectivityPredicate ( final int mod )
        {
            this.mod = mod;
        }

        @Override
        public boolean test ( final Tuple tuple )
        {
            return ( i++ % mod ) == 0;
        }

    }

}
