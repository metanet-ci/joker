package cs.bilkent.zanza.operator;

import java.util.List;

/**
 * Only for internal use
 */
public class PortsToTuplesAccessor
{

    public static void addAll ( final PortsToTuples portsToTuples, final int portIndex, final List<Tuple> tuples )
    {
        portsToTuples.addAll( portIndex, tuples, false );
    }

}
