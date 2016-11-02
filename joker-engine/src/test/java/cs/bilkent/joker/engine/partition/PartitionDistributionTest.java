package cs.bilkent.joker.engine.partition;

import org.junit.Test;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PartitionDistributionTest
{

    @Test
    public void shouldExtendPartitionReplicas ()
    {
        final PartitionDistribution distribution1 = new PartitionDistribution( new int[] { 0, 0, 1, 1 } );
        final PartitionDistribution distribution2 = new PartitionDistribution( new int[] { 0, 2, 3, 1 } );

        assertEquals( singletonList( 1 ), distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 0 ) );
        assertEquals( singletonList( 1 ), distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 2 ) );
        assertEquals( singletonList( 2 ), distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 1 ) );
        assertEquals( singletonList( 2 ), distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 3 ) );
    }

    @Test
    public void shouldShrinkPartitionReplicas ()
    {
        final PartitionDistribution distribution1 = new PartitionDistribution( new int[] { 0, 1, 2, 3 } );
        final PartitionDistribution distribution2 = new PartitionDistribution( new int[] { 0, 1, 0, 1 } );

        assertEquals( singletonList( 2 ), distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 2 ) );
        assertEquals( singletonList( 2 ), distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 0 ) );
        assertEquals( singletonList( 3 ), distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 3 ) );
        assertEquals( singletonList( 3 ), distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 1 ) );
    }

    @Test
    public void shouldNotMigrateBetweenAlreadyExistingReplicasWhileExtending ()
    {
        final PartitionDistribution distribution1 = new PartitionDistribution( new int[] { 0, 0, 1, 1 } );
        final PartitionDistribution distribution2 = new PartitionDistribution( new int[] { 0, 1, 3, 2 } );

        try
        {
            distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 0 );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }

        try
        {
            distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 1 );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }
    }

    @Test
    public void shouldNotMigrateBetweenNonDisappearingReplicasWhileShrinking ()
    {
        final PartitionDistribution distribution1 = new PartitionDistribution( new int[] { 0, 0, 1, 1 } );
        final PartitionDistribution distribution2 = new PartitionDistribution( new int[] { 0, 1, 3, 2 } );

        try
        {
            distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 0 );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }

        try
        {
            distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 1 );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }
    }

    @Test
    public void shouldNotMigrateBetweenSameReplicaCount ()
    {
        final PartitionDistribution distribution1 = new PartitionDistribution( new int[] { 0, 0, 1, 1 } );
        final PartitionDistribution distribution2 = new PartitionDistribution( new int[] { 0, 0, 1, 1 } );

        try
        {
            distribution1.getPartitionIdsMigratedFromReplicaIndex( distribution2, 0 );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }

        try
        {
            distribution1.getPartitionIdsMigratedToReplicaIndex( distribution2, 1 );
            fail();
        }
        catch ( IllegalStateException expected )
        {

        }
    }

}
