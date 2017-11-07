package cs.bilkent.joker.experiment;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.flow.FlowDef;

public interface FlowDefFactory
{

    FlowDef createFlow ( JokerConfig jokerConfig );

}
