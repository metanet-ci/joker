package cs.bilkent.zanza.engine.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cs.bilkent.zanza.engine.config.ZanzaConfig;

public class PartitionServiceImpl implements PartitionService
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionServiceImpl.class );


    private final Map<Integer, int[]> regionReplicaDistributions = new HashMap<>();

    private int partitionCount;

    @Override
    public void init ( final ZanzaConfig config )
    {
        this.partitionCount = config.getPartitionServiceConfig().getPartitionCount();
    }

    @Override
    public int getPartitionCount ()
    {
        return partitionCount;
    }

    @Override
    public int[] getOrCreatePartitionDistribution ( final int regionId, final int replicaCount )
    {
        final int[] distribution = regionReplicaDistributions.computeIfAbsent( regionId, integer -> {
            final List<Integer> l = new ArrayList<>( partitionCount );
            for ( int i = 0; i < partitionCount; i++ )
            {
                l.add( i % replicaCount );
            }
            Collections.shuffle( l );

            final int[] d = new int[ partitionCount ];
            for ( int i = 0; i < partitionCount; i++ )
            {
                d[ i ] = l.get( i );
            }

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "partition distribution is created for regionId={} replicaCount={} partitions={}",
                              regionId,
                              replicaCount,
                              d );
            }
            else
            {
                LOGGER.info( "partition distribution is created for regionId={} replicaCount={}", regionId, replicaCount );
            }

            return d;
        } );

        return Arrays.copyOf( distribution, partitionCount );
    }

}
