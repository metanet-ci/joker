package cs.bilkent.joker.pcj;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.pcj.PCJ;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.Joker.JokerBuilder;
import cs.bilkent.joker.engine.config.JokerConfig;
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

public class PCJJokerMain
{


    public static void main ( String[] args ) throws InterruptedException
    {
        final String[] nodes = new String[] { "localhost", "localhost" };

        final Random random = new Random();
        final OperatorConfig beaconConfig = new OperatorConfig();
        beaconConfig.set( BeaconOperator.TUPLE_COUNT_CONFIG_PARAMETER, 10 );
        beaconConfig.set( TUPLE_POPULATOR_CONFIG_PARAMETER, (Consumer<Tuple>) tuple ->
        {
            sleepUninterruptibly( 250 + random.nextInt( 100 ), TimeUnit.MILLISECONDS );
            tuple.set( "field1", random.nextInt( 10 ) );
        } );
        final OperatorRuntimeSchemaBuilder beaconSchema = new OperatorRuntimeSchemaBuilder( 0, 1 );
        beaconSchema.addOutputField( 0, "field1", Integer.class );

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

        final FlowDef flowDef = new FlowDefBuilder().add( beacon ).add( mapper ).connect( "beacon", "mapper" ).build();
        final PCJJokerInstanceFactory factory = ( jokerId, migrationService ) -> new JokerBuilder( new JokerConfig() ).setJokerId( jokerId )
                                                                                                                      .build();
        JokerRegistry.getInstance().start( factory );

        new Thread( () ->
                    {
                        Collection<Joker> jokerInstances;
                        while ( ( jokerInstances = JokerRegistry.getInstance().getJokerInstances() ).size() < nodes.length )
                        {
                            sleepUninterruptibly( 1, TimeUnit.MILLISECONDS );
                        }
                        jokerInstances.forEach( joker -> joker.run( flowDef ) );
                        sleepUninterruptibly( 10, TimeUnit.SECONDS );
                        jokerInstances.forEach( joker ->
                                                {
                                                    try
                                                    {
                                                        joker.shutdown().get( 30, TimeUnit.SECONDS );
                                                    }
                                                    catch ( InterruptedException e )
                                                    {
                                                        e.printStackTrace();
                                                        Thread.currentThread().interrupt();
                                                    }
                                                    catch ( ExecutionException | TimeoutException e )
                                                    {
                                                        e.printStackTrace();
                                                    }
                                                } );
                    } ).start();

        PCJ.deploy( PCJJokerWrapper.class, PCJJokerWrapper.class, nodes );

        System.out.println( "Completed." );
    }

}
