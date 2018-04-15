package cs.bilkent.joker.pcj;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.pcj.PCJ;
import org.pcj.StartPoint;
import org.pcj.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import cs.bilkent.joker.Joker;
import cs.bilkent.joker.engine.migration.MigrationService;
import cs.bilkent.joker.operator.utils.Pair;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PCJJokerWrapper extends Storage implements StartPoint, MigrationService
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PCJJokerWrapper.class );

    @Override
    public void main () throws Throwable
    {
        final Pair<Integer, Integer> jokerId = getJokerId();
        LOGGER.info( "Starting {} of joker: {}", PCJJokerWrapper.class.getSimpleName(), jokerId );

        final String jokerInstanceFactoryClassName = System.getProperty( PCJMain.PCJ_JOKER_FACTORY_SYS_PARAM );
        final Class<PCJJokerInstanceFactory> jokerFactoryClazz = (Class<PCJJokerInstanceFactory>) Class.forName(
                jokerInstanceFactoryClassName );
        final PCJJokerInstanceFactory jokerFactory = jokerFactoryClazz.newInstance();

        final Joker joker = jokerFactory.createJokerInstance( jokerId, this );

        sleepUninterruptibly( 30, SECONDS );

        try
        {
            joker.shutdown().get( 30, TimeUnit.SECONDS );
        }
        catch ( InterruptedException e )
        {
            LOGGER.error( "Shutdown failed", e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException | TimeoutException e )
        {
            LOGGER.error( "Shutdown failed", e );
        }

        LOGGER.info( "Joker {} is completed.", jokerId );
    }

    private Pair<Integer, Integer> getJokerId ()
    {
        return Pair.of( PCJ.getPhysicalNodeId(), PCJ.myId() );
    }

}
