package cs.bilkent.zanza.operator;

import java.util.List;
import java.util.function.Function;

import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;

/**
 * Only for internal use
 */
public class PortsToTuplesAccessor
{

    public static final Function<Int2ObjectHashMap<List<Tuple>>, PortsToTuples> PORTS_TO_TUPLES_CONSTRUCTOR = PortsToTuples::new;

}
