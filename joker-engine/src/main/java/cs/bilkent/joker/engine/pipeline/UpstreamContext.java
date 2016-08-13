package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.engine.pipeline.UpstreamConnectionStatus.ACTIVE;
import cs.bilkent.joker.flow.OperatorDef;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;

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

    public int getPortCount ()
    {
        return statuses.length;
    }

    public UpstreamConnectionStatus getUpstreamConnectionStatus ( int index )
    {
        return statuses[ index ];
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

    public boolean[] getUpstreamConnectionStatuses ()
    {
        final boolean[] b = new boolean[ statuses.length ];
        for ( int portIndex = 0; portIndex < statuses.length; portIndex++ )
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
        checkArgument( operatorDef.inputPortCount() == getPortCount(),
                       "different input port counts! operator=%s upstream context=%s",
                       operatorDef.inputPortCount(),
                       getPortCount() );

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
            checkState( operatorDef.inputPortCount() == s.getPortCount(),
                        "Operator %s and SchedulingStrategy %s has different input port counts: %s, %s respectively.",
                        operatorDef.id(),
                        s,
                        operatorDef.inputPortCount(),
                        s.getPortCount() );
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
