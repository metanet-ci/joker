package cs.bilkent.joker.engine.region;

import java.util.List;

import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.utils.Pair;

public interface FlowDefOptimizer
{

    Pair<FlowDef, List<RegionDef>> optimize ( FlowDef flow, List<RegionDef> regions );

}
