package cs.bilkent.joker.engine.region.impl;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.region.RegionDef;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.RegionExecutionPlanFactory;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.OperatorConfig;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.schema.runtime.OperatorRuntimeSchemaBuilder;
import cs.bilkent.joker.operators.BeaconOperator;
import static cs.bilkent.joker.operators.BeaconOperator.TUPLE_POPULATOR_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.MapperOperator;
import static cs.bilkent.joker.operators.MapperOperator.MAPPER_CONFIG_PARAMETER;
import cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER;
import static cs.bilkent.joker.operators.TupleCountBasedWindowReducerOperator.REDUCER_CONFIG_PARAMETER;
import static java.util.Collections.singletonList;

public class InteractiveRegionExecutionPlanFactoryMain
{

    public static void main ( String[] args )
    {
        final Random random = new Random();
        final IdGenerator idGenerator = new IdGenerator();
        final RegionDefFormer regionFormer = new RegionDefFormerImpl( idGenerator );
        final JokerConfig jokerConfig = new JokerConfig();
        final RegionExecutionPlanFactory regionExecutionPlanFactory = new InteractiveRegionExecutionPlanFactory( jokerConfig );

        final OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 10 );
        beaconConfig.set( TUPLE_POPULATOR_CONFIG_PARAMETER, (Consumer<Tuple>) tuple ->
        {
            sleepUninterruptibly( 1 + random.nextInt( 100 ), TimeUnit.MILLISECONDS );
            tuple.set( "field1", random.nextInt( 1000 ) );
        } );
        final OperatorRuntimeSchemaBuilder beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beaconSchema.getOutputPortSchemaBuilder( 0 ).addField( "field1", Integer.class );

        final OperatorDef beacon = OperatorDefBuilder.newInstance( "beacon", BeaconOperator.class )
                                                     .setConfig( beaconConfig )
                                                     .setExtendingSchema( beaconSchema )
                                                     .build();

        final OperatorConfig mapperConfig = new OperatorConfig();
        mapperConfig.set( MAPPER_CONFIG_PARAMETER,
                          (BiConsumer<Tuple, Tuple>) ( input, output ) -> output.set( "field1", input.get( "field1" ) ) );

        final OperatorRuntimeSchemaBuilder mapperSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        mapperSchema.addInputField( 0, "field1", Integer.class ).addOutputField( 0, "field1", Integer.class );

        final OperatorDef mapper = OperatorDefBuilder.newInstance( "mapper", MapperOperator.class )
                                                     .setConfig( mapperConfig )
                                                     .setExtendingSchema( mapperSchema )
                                                     .build();

        final OperatorConfig windowConfig = new OperatorConfig();
        windowConfig.set( ACCUMULATOR_INITIALIZER_CONFIG_PARAMETER, (Consumer<Tuple>) tuple -> tuple.set( "field1", 0 ) );
        windowConfig.set( TupleCountBasedWindowReducerOperator.TUPLE_COUNT_CONFIG_PARAMETER, 1 );
        windowConfig.set( REDUCER_CONFIG_PARAMETER,
                          (BiConsumer<Tuple, Tuple>) ( acc, val ) -> acc.set( "field1",
                                                                              acc.getInteger( "field1" ) + val.getInteger( "field1" ) ) );

        final OperatorRuntimeSchemaBuilder windowSchema = new OperatorRuntimeSchemaBuilder( 1, 1 );
        windowSchema.addInputField( 0, "field1", Integer.class );

        final OperatorDef window = OperatorDefBuilder.newInstance( "window", TupleCountBasedWindowReducerOperator.class )
                                                     .setConfig( windowConfig )
                                                     .setExtendingSchema( windowSchema )
                                                     .setPartitionFieldNames( singletonList( "field1" ) )
                                                     .build();

        final FlowDef flow = new FlowDefBuilder().add( beacon )
                                                 .add( mapper )
                                                 .add( window )
                                                 .connect( "beacon", "mapper" )
                                                 .connect( "mapper", "window" )
                                                 .build();

        final List<RegionDef> regions = regionFormer.createRegions( flow );

        regionExecutionPlanFactory.createRegionExecutionPlans( regions );
    }

}
