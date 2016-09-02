package cs.bilkent.joker.pcj;

import org.pcj.PCJ;
import org.pcj.StartPoint;
import org.pcj.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.joker.Joker;
import cs.bilkent.joker.engine.FlowStatus;
import cs.bilkent.joker.engine.migration.MigrationService;
import cs.bilkent.joker.utils.Pair;
import static java.lang.Thread.sleep;

public class PCJJokerWrapper extends Storage implements StartPoint, MigrationService
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PCJJokerWrapper.class );

    @Override
    public void main () throws Throwable
    {
        final Pair<Integer, Integer> jokerId = getJokerId();
        LOGGER.info( "Starting {} of joker: {}", PCJJokerWrapper.class.getSimpleName(), jokerId );

        final Joker joker = JokerRegistry.getInstance().createJokerInstance( jokerId, this );
        while ( joker.getStatus() != FlowStatus.SHUT_DOWN && joker.getStatus() != FlowStatus.INITIALIZATION_FAILED )
        {
            sleep( 100 );
        }

        LOGGER.info( "Joker {} is completed.", jokerId );

        JokerRegistry.getInstance().destroyJokerInstance( jokerId );

        LOGGER.info( "Completing {} of joker: {}", PCJJokerWrapper.class.getSimpleName(), jokerId );
    }

    private Pair<Integer, Integer> getJokerId ()
    {
        return Pair.of( PCJ.getPhysicalNodeId(), PCJ.myId() );
    }

}
