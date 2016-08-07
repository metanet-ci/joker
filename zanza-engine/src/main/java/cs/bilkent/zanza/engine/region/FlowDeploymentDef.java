package cs.bilkent.zanza.engine.region;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.zanza.flow.FlowDef;
import static java.util.Collections.unmodifiableList;

public class FlowDeploymentDef
{

    private final FlowDef flow;

    private final List<RegionDef> regions;

    private final List<RegionGroup> regionGroups;

    public FlowDeploymentDef ( final FlowDef flow, final List<RegionDef> regions, final List<RegionGroup> regionGroups )
    {
        this.flow = flow;
        this.regions = unmodifiableList( new ArrayList<>( regions ) );
        this.regionGroups = unmodifiableList( new ArrayList<>( regionGroups ) );
    }

    public FlowDef getFlow ()
    {
        return flow;
    }

    public List<RegionDef> getRegions ()
    {
        return regions;
    }

    public List<RegionGroup> getRegionGroups ()
    {
        return regionGroups;
    }

    public static class RegionGroup
    {

        private final List<RegionDef> regions;

        public RegionGroup ( final List<RegionDef> regions )
        {
            checkArgument( regions != null && regions.size() > 0 );
            this.regions = unmodifiableList( new ArrayList<>( regions ) );
        }

        public List<RegionDef> getRegions ()
        {
            return regions;
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

            final RegionGroup that = (RegionGroup) o;

            return regions.equals( that.regions );

        }

        @Override
        public int hashCode ()
        {
            return regions.hashCode();
        }

        @Override
        public String toString ()
        {
            return "RegionGroup{" + regions + '}';
        }

    }

}
