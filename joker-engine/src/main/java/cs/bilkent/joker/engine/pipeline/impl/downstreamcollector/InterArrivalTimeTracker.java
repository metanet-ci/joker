package cs.bilkent.joker.engine.pipeline.impl.downstreamcollector;

import java.util.List;
import java.util.function.LongSupplier;

import cs.bilkent.joker.engine.pipeline.DownstreamCollector;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.operator.impl.TuplesImpl;

public class InterArrivalTimeTracker implements DownstreamCollector
{

    private final String operatorId;

    private final int interArrivalTimeTrackingPeriod;

    private final int interArrivalTimeTrackingCount;

    private final LongSupplier timeGenerator;

    private final DownstreamCollector downstream;

    private int round;

    private long previousTime;

    public InterArrivalTimeTracker ( final String operatorId,
                                     final int interArrivalTimeTrackingPeriod,
                                     final int interArrivalTimeTrackingCount,
                                     final LongSupplier timeGenerator,
                                     final DownstreamCollector downstream )
    {
        this.operatorId = operatorId;
        this.interArrivalTimeTrackingPeriod = interArrivalTimeTrackingPeriod;
        this.interArrivalTimeTrackingCount = interArrivalTimeTrackingCount;
        this.round = interArrivalTimeTrackingPeriod;
        this.timeGenerator = timeGenerator;
        this.downstream = downstream;
    }

    @Override
    public void accept ( final TuplesImpl tuples )
    {
        trackInterArrivalTime( tuples );
        downstream.accept( tuples );
    }

    private void trackInterArrivalTime ( final TuplesImpl tuples )
    {
        if ( round == interArrivalTimeTrackingCount )
        {
            previousTime = timeGenerator.getAsLong();
        }
        else if ( round < interArrivalTimeTrackingCount )
        {
            final long time = timeGenerator.getAsLong();
            final long interArrivalTime = ( time - previousTime );
            final List<Tuple> t = tuples.getTuplesModifiable( 0 );
            if ( t.size() > 0 )
            {
                t.get( 0 ).recordInterArrivalTime( operatorId, interArrivalTime );
            }

            previousTime = time;

            if ( round == 0 )
            {
                round = interArrivalTimeTrackingPeriod;
                return;
            }
        }

        round--;
    }

}
