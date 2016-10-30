package cs.bilkent.joker.engine.partition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.Singleton;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import cs.bilkent.joker.engine.config.JokerConfig;
import static java.lang.Integer.compare;
import static java.util.Collections.emptyList;

@Singleton
@NotThreadSafe
public class PartitionServiceImpl implements PartitionService
{

    private static final Logger LOGGER = LoggerFactory.getLogger( PartitionServiceImpl.class );


    private final Map<Integer, int[]> distributions = new HashMap<>();

    private final int partitionCount;

    @Inject
    public PartitionServiceImpl ( final JokerConfig jokerConfig )
    {
        this.partitionCount = jokerConfig.getPartitionServiceConfig().getPartitionCount();
    }

    @Override
    public int getPartitionCount ()
    {
        return partitionCount;
    }

    @Override
    public int[] getOrCreatePartitionDistribution ( final int regionId, final int replicaCount )
    {
        checkReplicaCount( replicaCount );

        final int[] distribution = distributions.computeIfAbsent( regionId, r ->
        {
            final List<Integer> replicaIndices = new ArrayList<>( partitionCount );
            for ( int partitionId = 0; partitionId < partitionCount; partitionId++ )
            {
                replicaIndices.add( partitionId % replicaCount );
            }
            Collections.shuffle( replicaIndices );

            final int[] partitionOwnership = new int[ partitionCount ];
            for ( int partitionId = 0; partitionId < partitionCount; partitionId++ )
            {
                partitionOwnership[ partitionId ] = replicaIndices.get( partitionId );
            }

            LOGGER.info( "partition distribution is created for regionId={} replicaCount={} distribution={}",
                         regionId,
                         replicaCount,
                         partitionOwnership );

            checkDistribution( regionId, partitionOwnership );

            return partitionOwnership;
        } );

        return Arrays.copyOf( distribution, partitionCount );
    }

    private void checkReplicaCount ( final int replicaCount )
    {
        checkArgument( replicaCount > 0, "replica count: %s must be positive", replicaCount );
    }

    private void checkDistribution ( final int regionId, final int[] distribution )
    {
        final Map<Integer, List<Integer>> newOwnerships = getOwnershipsMap( distribution );
        final int replicaCount = newOwnerships.size();
        int overCapacityCount = partitionCount % replicaCount;
        int normalCapacityCount = replicaCount - overCapacityCount;
        final int normalCapacity = partitionCount / replicaCount;
        for ( Entry<Integer, List<Integer>> e : newOwnerships.entrySet() )
        {
            final Integer replicaIndex = e.getKey();
            final List<Integer> partitions = e.getValue();

            if ( partitions.size() < normalCapacity )
            {
                final String err =
                        "regionId=" + regionId + " replicaIndex=" + replicaIndex + " is under capacity! distribution: " + Arrays.toString(
                                distribution );
                throw new IllegalStateException( err );
            }
            else if ( partitions.size() == normalCapacity )
            {
                final String err = "regionId=" + regionId + " replicaIndex=" + replicaIndex + " is over normal capacity! distribution: "
                                   + Arrays.toString( distribution );
                checkState( normalCapacityCount-- > 0, err );
            }
            else
            {
                final String err = "regionId=" + regionId + " replicaIndex=" + replicaIndex + " is over over capacity! distribution: "
                                   + Arrays.toString( distribution );
                checkState( overCapacityCount-- > 0, err );
            }
        }
    }

    @Override
    public int[] rebalancePartitionDistribution ( final int regionId, final int newReplicaCount )
    {
        checkReplicaCount( newReplicaCount );

        final int[] distribution = distributions.get( regionId );

        final Map<Integer, List<Integer>> currentOwnerships = getOwnershipsMap( distribution );

        if ( currentOwnerships.size() == newReplicaCount )
        {
            return Arrays.copyOf( distribution, partitionCount );
        }

        final Map<Integer, List<Integer>> destinations = changeOwnerships( regionId, currentOwnerships, newReplicaCount );

        updateDistribution( distribution, destinations );

        checkDistribution( regionId, distribution );

        LOGGER.info( "partition distribution is rebalanced for regionId={} newReplicaCount={} distribution={}",
                     regionId,
                     newReplicaCount,
                     distribution );

        return Arrays.copyOf( distribution, partitionCount );
    }

    private Map<Integer, List<Integer>> getOwnershipsMap ( final int[] distribution )
    {
        final Map<Integer, List<Integer>> currentOwnerships = new HashMap<>();
        for ( int partitionId = 0; partitionId < distribution.length; partitionId++ )
        {
            final int replicaIndex = distribution[ partitionId ];
            currentOwnerships.computeIfAbsent( replicaIndex, integer -> new ArrayList<>() ).add( partitionId );
        }

        currentOwnerships.values().forEach( Collections::shuffle );

        return currentOwnerships;
    }

    private void populate ( final int regionId,
                            final Map<Integer, List<Integer>> currentOwnerships,
                            final int newReplicaCount,
                            final Set<Integer> sourceReplicas,
                            final Map<Integer, List<Integer>> destinations )
    {
        final int currentReplicaCount = currentOwnerships.size();
        final Set<Integer> destinationReplicas = new HashSet<>();

        if ( currentReplicaCount > newReplicaCount )
        {
            for ( int replicaIndex = 0; replicaIndex < currentReplicaCount; replicaIndex++ )
            {
                final Set<Integer> replicas = ( replicaIndex < newReplicaCount ) ? destinationReplicas : sourceReplicas;
                replicas.add( replicaIndex );
            }
        }
        else
        {
            for ( int replicaIndex = 0; replicaIndex < newReplicaCount; replicaIndex++ )
            {
                final Set<Integer> replicas = ( replicaIndex < currentReplicaCount ) ? sourceReplicas : destinationReplicas;
                replicas.add( replicaIndex );
            }
        }

        final int sourceReplicaCount = sourceReplicas.size();
        sourceReplicas.removeAll( destinationReplicas );
        checkState( sourceReplicas.size() == sourceReplicaCount,
                    "could not determine source and destination replicas! regionId=%s current ownerships: %s",
                    regionId,
                    currentOwnerships );

        destinationReplicas.forEach( r -> destinations.put( r, new ArrayList<>() ) );
        destinations.forEach( ( replica, partitions ) -> partitions.addAll( currentOwnerships.getOrDefault( replica, emptyList() ) ) );
    }

    private Map<Integer, List<Integer>> changeOwnerships ( final int regionId,
                                                           final Map<Integer, List<Integer>> currentOwnerships,
                                                           final int newReplicaCount )
    {

        final int normalCapacity = partitionCount / newReplicaCount;

        final Set<Integer> sourceReplicas = new HashSet<>();
        final Map<Integer, List<Integer>> destinations = new HashMap<>();

        populate( regionId, currentOwnerships, newReplicaCount, sourceReplicas, destinations );

        while ( true )
        {
            final Entry<Integer, List<Integer>> source = getSource( currentOwnerships, sourceReplicas );

            final List<Integer> sourcePartitions = source.getValue();
            checkState( sourcePartitions.size() > 0,
                        "all rebalancing source replicas are drained! regionId=%s, current ownerships=%s, source replicas: %s, "
                        + "destinations: %s",
                        regionId,
                        currentOwnerships,
                        sourceReplicas,
                        destinations );

            final Integer partitionId = sourcePartitions.remove( sourcePartitions.size() - 1 );

            addToDestination( destinations, partitionId );

            if ( isNewDistributionBalanced( regionId, currentOwnerships, newReplicaCount, normalCapacity, sourceReplicas, destinations ) )
            {
                break;
            }
        }

        return destinations;
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    private Entry<Integer, List<Integer>> getSource ( final Map<Integer, List<Integer>> currentOwnerships,
                                                      final Set<Integer> sourceReplicas )
    {
        return currentOwnerships.entrySet()
                                .stream()
                                .filter( e -> sourceReplicas.contains( e.getKey() ) )
                                .max( ( e1, e2 ) -> compare( getPartitionCount( e1 ), getPartitionCount( e2 ) ) )
                                .get();
    }

    private int getPartitionCount ( final Entry<Integer, List<Integer>> e )
    {
        return e.getValue().size();
    }

    private void addToDestination ( final Map<Integer, List<Integer>> destinations, final int partitionId )
    {
        final List<Integer> destination = getDestination( destinations );
        destination.add( partitionId );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    private List<Integer> getDestination ( final Map<Integer, List<Integer>> destinations )
    {
        return destinations.entrySet()
                           .stream()
                           .min( ( e1, e2 ) -> compare( getPartitionCount( e1 ), getPartitionCount( e2 ) ) )
                           .get()
                           .getValue();
    }

    private boolean isNewDistributionBalanced ( final int regionId,
                                                final Map<Integer, List<Integer>> currentOwnerships,
                                                final int newReplicaCount,
                                                final int normalCapacity,
                                                final Set<Integer> sourceReplicas,
                                                final Map<Integer, List<Integer>> destinations )
    {
        final int overCapacity = normalCapacity + 1;
        int overCapacityCount = partitionCount % newReplicaCount;
        int normalCapacityCount = newReplicaCount - overCapacityCount;

        for ( Integer sourceReplicaIndex : sourceReplicas )
        {
            final List<Integer> p = currentOwnerships.get( sourceReplicaIndex );
            if ( p.size() == normalCapacity && normalCapacityCount > 0 )
            {
                normalCapacityCount--;

            }
            else if ( p.size() == overCapacity && overCapacityCount > 0 )
            {
                overCapacityCount--;
            }
        }

        for ( final List<Integer> p : destinations.values() )
        {
            if ( p.size() == normalCapacity && normalCapacityCount > 0 )
            {
                normalCapacityCount--;
            }
            else if ( p.size() == overCapacity && overCapacityCount > 0 )
            {
                overCapacityCount--;
            }
            else if ( p.size() > overCapacity )
            {
                throw new IllegalStateException( "regionId=" + regionId
                                                 + " rebalancing destination is over over-capacity! current ownerships: "
                                                 + currentOwnerships + " new replica count: " + newReplicaCount + " normal capacity: "
                                                 + normalCapacity + " destinations: " + destinations );
            }
        }

        return normalCapacityCount == 0 && overCapacityCount == 0;
    }

    private void updateDistribution ( final int[] distribution, final Map<Integer, List<Integer>> destinations )
    {
        for ( Entry<Integer, List<Integer>> e : destinations.entrySet() )
        {
            final Integer replicaIndex = e.getKey();
            final List<Integer> partitionIds = e.getValue();
            for ( int partitionId : partitionIds )
            {
                distribution[ partitionId ] = replicaIndex;
            }
        }
    }

}
