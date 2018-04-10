package cs.bilkent.joker.engine.pipeline;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.CLOSED;
import static cs.bilkent.joker.engine.pipeline.UpstreamCtx.ConnectionStatus.OPEN;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.Port;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenAvailable;
import cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ALL_PORTS;
import static cs.bilkent.joker.operator.scheduling.ScheduleWhenTuplesAvailable.TupleAvailabilityByPort.ANY_PORT;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import static java.util.Arrays.copyOf;
import static java.util.Arrays.fill;
import static java.util.Arrays.stream;

public class UpstreamCtx
{

    private static final Logger LOGGER = LoggerFactory.getLogger( UpstreamCtx.class );

    public static final int INITIAL_VERSION = 0;


    public enum ConnectionStatus
    {
        OPEN, CLOSED
    }


    public static UpstreamCtx createSourceOperatorInitialUpstreamCtx ()
    {
        return new UpstreamCtx( INITIAL_VERSION, new ConnectionStatus[ 0 ] );
    }

    public static UpstreamCtx createSourceOperatorShutdownUpstreamCtx ()
    {
        return new UpstreamCtx( INITIAL_VERSION + 1, new ConnectionStatus[ 0 ] );
    }

    public static UpstreamCtx createInitialUpstreamCtx ( final ConnectionStatus... statuses )
    {
        return new UpstreamCtx( INITIAL_VERSION, statuses );
    }

    public static UpstreamCtx createInitialClosedUpstreamCtx ( final int portCount )
    {
        final ConnectionStatus[] statuses = new ConnectionStatus[ portCount ];
        fill( statuses, OPEN );

        return new UpstreamCtx( INITIAL_VERSION, statuses );
    }

    public static UpstreamCtx createInitialUpstreamCtx ( final FlowDef flow, final String operatorId )
    {
        final OperatorDef operatorDef = flow.getOperator( operatorId );
        final int inputPortCount = operatorDef.getInputPortCount();
        final ConnectionStatus[] statuses = new ConnectionStatus[ inputPortCount ];
        fill( statuses, CLOSED );
        for ( Port inputPort : flow.getInboundConnections( operatorDef.getId() ).keySet() )
        {
            statuses[ inputPort.getPortIndex() ] = OPEN;
        }

        return createInitialUpstreamCtx( statuses );
    }


    private final int version;

    private final ConnectionStatus[] statuses;

    public UpstreamCtx ( final int version, final ConnectionStatus[] statuses )
    {
        checkArgument( version >= INITIAL_VERSION );
        this.version = version;
        this.statuses = copyOf( statuses, statuses.length );
    }

    public int getVersion ()
    {
        return version;
    }

    public boolean isInitial ()
    {
        return version == INITIAL_VERSION;
    }

    public boolean isNotInitial ()
    {
        return version > INITIAL_VERSION;
    }

    public int getPortCount ()
    {
        return statuses.length;
    }

    public ConnectionStatus getConnectionStatus ( int index )
    {
        checkArgument( index < statuses.length );
        return statuses[ index ];
    }

    boolean isOpenConnectionPresent ()
    {
        for ( ConnectionStatus status : statuses )
        {
            if ( status == OPEN )
            {
                return true;
            }
        }

        return false;
    }

    boolean isOpenConnectionAbsent ()
    {
        return !isOpenConnectionPresent();
    }

    public boolean[] getConnectionStatuses ()
    {
        final boolean[] b = new boolean[ statuses.length ];
        for ( int portIndex = 0, j = statuses.length; portIndex < j; portIndex++ )
        {
            b[ portIndex ] = ( statuses[ portIndex ] == OPEN );
        }

        return b;
    }

    public void verifyInitializable ( final OperatorDef operatorDef, final SchedulingStrategy schedulingStrategy )
    {
        checkArgument( statuses.length == operatorDef.getInputPortCount(),
                       "%s has different input port count than %s",
                       this,
                       operatorDef.getId() );

        if ( schedulingStrategy instanceof ScheduleWhenAvailable )
        {
            checkState( version == 0, "%s is closed for 0 input port operator: %s", this, operatorDef.getId() );
            checkArgument( operatorDef.getInputPortCount() == 0,
                           "%s cannot be used by operator: %s with input port count: %s",
                           ScheduleWhenAvailable.class.getSimpleName(),
                           operatorDef.getId(),
                           operatorDef.getInputPortCount() );
        }
        else if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            checkArgument( operatorDef.getInputPortCount() > 0,
                           "0 input port operator: %s cannot use %s",
                           operatorDef.getId(),
                           ScheduleWhenTuplesAvailable.class.getSimpleName() );

            final ScheduleWhenTuplesAvailable s = (ScheduleWhenTuplesAvailable) schedulingStrategy;

            checkArgument( s.getPortCount() == operatorDef.getInputPortCount(),
                           "%s has different input port count than %s",
                           s,
                           operatorDef );

            for ( int i = 0; i < operatorDef.getInputPortCount(); i++ )
            {
                final boolean validWhenClosed = ( getConnectionStatus( i ) == CLOSED && s.getTupleCount( i ) == 0 );
                final boolean validWhenOpen = ( getConnectionStatus( i ) == OPEN && s.getTupleCount( i ) > 0 );
                checkArgument( validWhenClosed || validWhenOpen, "Invalid %s for %s of %s", s, this, operatorDef );
            }
        }
        else
        {
            throw new IllegalStateException( "Invalid " + schedulingStrategy + " for operator: " + operatorDef.getId() );
        }
    }

    public boolean isInvokable ( final OperatorDef operatorDef, final SchedulingStrategy schedulingStrategy )
    {
        checkArgument( statuses.length == operatorDef.getInputPortCount(), "%s has different input port count than %s", this, operatorDef );

        if ( schedulingStrategy instanceof ScheduleWhenAvailable )
        {
            checkArgument( operatorDef.getInputPortCount() == 0,
                           "%s cannot be used by operator: %s with input port count: %s",
                           ScheduleWhenAvailable.class.getSimpleName(),
                           operatorDef.getId(),
                           operatorDef.getInputPortCount() );

            if ( version > 0 )
            {
                LOGGER.warn( "%s is closed for 0 input port operator: {}", this, operatorDef.getId() );

                return false;
            }

            return true;
        }
        else if ( schedulingStrategy instanceof ScheduleWhenTuplesAvailable )
        {
            checkArgument( operatorDef.getInputPortCount() > 0,
                           "0 input port operator: %s cannot use %s",
                           operatorDef.getId(),
                           ScheduleWhenTuplesAvailable.class.getSimpleName() );

            final ScheduleWhenTuplesAvailable s = (ScheduleWhenTuplesAvailable) schedulingStrategy;

            checkArgument( s.getPortCount() == operatorDef.getInputPortCount(),
                           "Operator: %s has different input port count than %s",
                           s,
                           operatorDef.getId() );

            if ( s.getTupleAvailabilityByPort() == ANY_PORT )
            {
                for ( int i = 0; i < operatorDef.getInputPortCount(); i++ )
                {
                    if ( s.getTupleCount( i ) > 0 && getConnectionStatus( i ) == OPEN )
                    {
                        return true;
                    }
                }

                LOGGER.warn( "Operator: {} with {} is not invokable anymore since there is no open port", operatorDef.getId(), s );

                return false;
            }
            else if ( s.getTupleAvailabilityByPort() == ALL_PORTS )
            {
                for ( int i = 0; i < operatorDef.getInputPortCount(); i++ )
                {
                    if ( s.getTupleCount( i ) > 0 && getConnectionStatus( i ) != OPEN )
                    {
                        LOGGER.warn( "Operator: {} with {} is not invokable anymore since input port: {} is closed",
                                     operatorDef.getId(),
                                     s,
                                     i );

                        return false;
                    }
                }

                return true;
            }
        }

        throw new IllegalStateException( "Invalid " + schedulingStrategy + " for operator: " + operatorDef.getId() );
    }

    public UpstreamCtx withConnectionClosed ( final int portIndex )
    {
        checkArgument( portIndex < statuses.length );

        if ( statuses[ portIndex ] == CLOSED )
        {
            return this;
        }

        final ConnectionStatus[] s = copyOf( statuses, statuses.length );
        s[ portIndex ] = CLOSED;

        return new UpstreamCtx( version + 1, s );
    }

    public UpstreamCtx withAllConnectionsClosed ()
    {
        if ( statuses.length == stream( statuses ).filter( s -> s == CLOSED ).count() )
        {
            return this;
        }

        final ConnectionStatus[] s = copyOf( statuses, statuses.length );
        fill( s, CLOSED );

        return new UpstreamCtx( version + 1, s );
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

        final UpstreamCtx that = (UpstreamCtx) o;

        return version == that.version && Arrays.equals( statuses, that.statuses );
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
