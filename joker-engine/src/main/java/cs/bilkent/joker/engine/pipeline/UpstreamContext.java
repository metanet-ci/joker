package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.CLOSED;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.lang.Math.min;

public class UpstreamContext
{

    private static final Logger LOGGER = LoggerFactory.getLogger( UpstreamContext.class );


    private final int version;

    private final UpstreamConnectionStatus[] statuses;

    public UpstreamContext ( final int version, final UpstreamConnectionStatus[] statuses )
    {
        this.version = version;
        this.statuses = Arrays.copyOf( statuses, statuses.length );
    }

    public int getVersion ()
    {
        return version;
    }

    public UpstreamConnectionStatus getUpstreamConnectionStatus ( int index )
    {
        return index < statuses.length ? statuses[ index ] : CLOSED;
    }

    public boolean isActiveConnectionPresent ()
    {
        for ( UpstreamConnectionStatus status : statuses )
        {
            if ( status == ACTIVE )
            {
                return true;
            }
        }

        return false;
    }

    public boolean isActiveConnectionAbsent ()
    {
        return !isActiveConnectionPresent();
    }

    public boolean[] getUpstreamConnectionStatuses ( int portCount )
    {
        final boolean[] b = new boolean[ portCount ];
        for ( int portIndex = 0, j = min( portCount, statuses.length ); portIndex < j; portIndex++ )
        {
            b[ portIndex ] = statuses[ portIndex ] == ACTIVE;
        }

        return b;
    }

    public boolean isInvokable ( final OperatorDef operatorDef, final SchedulingStrategy schedulingStrategy )
    {
        try
        {
            verifyOrFail( operatorDef, schedulingStrategy );
            return true;
        }
        catch ( IllegalStateException e )
        {
            LOGGER.info( "{} not invokable anymore. scheduling strategy: {} upstream context: {} error: {}",
                         operatorDef.id(),
                         schedulingStrategy,
                         this,
                         e.getMessage() );
            return false;
        }
    }

    public void verifyOrFail ( final OperatorDef operatorDef, final SchedulingStrategy schedulingStrategy )
    {
        if ( schedulingStrategy instanceof ScheduleWhenAvailable )
        {
            checkState( operatorDef.inputPortCount() == 0,
                        "%s cannot be used by operator: %s with input port count: %s",
                        ScheduleWhenAvailable.class.getSimpleName(),
                        operatorDef.id(),
                        operatorDef.inputPortCount() );
            checkState( version == 0, "upstream context is closed for 0 input port operator: %s", operatorDef.id() );
        }
        else if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            checkState( operatorDef.inputPortCount() > 0,
                        "0 input port operator: %s cannot use %s",
                        operatorDef.id(),
                        ScheduleWhenTuplesAvailable.class.getSimpleName() );
            final ScheduleWhenTuplesAvailable s = (ScheduleWhenTuplesAvailable) schedulingStrategy;
            if ( s.getTupleAvailabilityByPort() == ANY_PORT )
            {
                for ( int i = 0; i < operatorDef.inputPortCount(); i++ )
                {
                    if ( s.getTupleCount( i ) > 0 && getUpstreamConnectionStatus( i ) == ACTIVE )
                    {
                        return;
                    }
                }

                throw new IllegalStateException( "SchedulingStrategy " + s + " is not invokable anymore since there is no open port" );
            }
            else if ( s.getTupleAvailabilityByPort() == ALL_PORTS )
            {
                for ( int i = 0; i < operatorDef.inputPortCount(); i++ )
                {
                    checkState( getUpstreamConnectionStatus( i ) == ACTIVE,
                                "SchedulingStrategy %s is not invokable anymore since there is closed port",
                                s );
                }
            }
            else
            {
                throw new IllegalStateException( s.toString() );
            }
        }
        else
        {
            throw new IllegalStateException( operatorDef.id() + " returns invalid initial scheduling strategy: " + schedulingStrategy );
        }
    }

    public UpstreamContext withClosedUpstreamConnection ( final int portIndex )
    {
        checkArgument( portIndex < statuses.length );
        final UpstreamConnectionStatus[] s = Arrays.copyOf( statuses, statuses.length );
        s[ portIndex ] = CLOSED;
        return new UpstreamContext( version + 1, s );
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final UpstreamContext that = (UpstreamContext) o;

        if ( version != that.version )
        {
            return false;
        }

        return Arrays.equals( statuses, that.statuses );
    }

    @Override
    public int hashCode ()
    {
        int result = version;
        result = 31 * result + Arrays.hashCode( statuses );
        return result;
    }

    @Override
    public String toString ()
    {
        return "UpstreamContext{" + "version=" + version + ", statuses=" + Arrays.toString( statuses ) + '}';
    }

}
