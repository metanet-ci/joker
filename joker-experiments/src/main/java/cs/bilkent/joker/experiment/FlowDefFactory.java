package cs.bilkent.joker.experiment;

import com.typesafe.config.Config;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.flow.FlowDef;

public interface FlowDefFactory
{

    FlowDef createFlow ( Config config, JokerConfig jokerConfig );

}
