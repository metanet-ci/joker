package cs.bilkent.joker.engine.util;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import cs.bilkent.joker.engine.config.JokerConfig;
import cs.bilkent.joker.engine.config.JokerConfigBuilder;
import cs.bilkent.joker.engine.flow.RegionDef;
import cs.bilkent.joker.engine.region.FlowDefOptimizer;
import cs.bilkent.joker.engine.region.RegionDefFormer;
import cs.bilkent.joker.engine.region.impl.FlowDefOptimizerImpl;
import cs.bilkent.joker.engine.region.impl.IdGenerator;
import cs.bilkent.joker.engine.region.impl.RegionDefFormerImpl;
import static cs.bilkent.joker.engine.util.RegionUtil.getLeftMostRegions;
import static cs.bilkent.joker.engine.util.RegionUtil.getRegionByFirstOperator;
import static cs.bilkent.joker.engine.util.RegionUtil.getWholeDownstream;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.flow.FlowDefBuilder;
import cs.bilkent.joker.operator.InitCtx;
import cs.bilkent.joker.operator.InvocationCtx;
import cs.bilkent.joker.operator.Operator;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.OperatorDefBuilder;
import cs.bilkent.joker.operator.scheduling.SchedulingStrategy;
import cs.bilkent.joker.operator.spec.OperatorSpec;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class RegionUtilTest extends AbstractJokerTest
{

    /*
        stateful1 -> stateful2 -> stateful3
                           \
                            \---> stateful4 ---> stateless1 -> stateless2
                                     /  \
                     stateful5 -----/    \-----> stateful6
     */

    private FlowDef flow;

    private List<RegionDef> regions;

    private RegionDef statefulRegion1;

    private RegionDef statefulRegion2;

    private RegionDef statefulRegion3;

    private RegionDef statefulRegion4;

    private RegionDef statefulRegion5;

    private RegionDef statefulRegion6;

    private RegionDef statelessRegion;

    @Before
    public void init ()
    {

        final FlowDefBuilder flowDefBuilder = new FlowDefBuilder();

        final OperatorDef stateful1 = OperatorDefBuilder.newInstance( "stateful1", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef stateful2 = OperatorDefBuilder.newInstance( "stateful2", StatefulOperatorInput1Output1.class ).build();
        final OperatorDef stateful3 = OperatorDefBuilder.newInstance( "stateful3", StatefulOperatorInput1Output1.class ).build();
        final OperatorDef stateful4 = OperatorDefBuilder.newInstance( "stateful4", StatefulOperatorInput1Output1.class ).build();
        final OperatorDef stateful5 = OperatorDefBuilder.newInstance( "stateful5", StatefulOperatorInput0Output1.class ).build();
        final OperatorDef stateful6 = OperatorDefBuilder.newInstance( "stateful6", StatefulOperatorInput1Output1.class ).build();
        final OperatorDef stateless1 = OperatorDefBuilder.newInstance( "stateless1", StatelessOperatorInput1Output1.class ).build();
        final OperatorDef stateless2 = OperatorDefBuilder.newInstance( "stateless2", StatelessOperatorInput1Output1.class ).build();

        flow = flowDefBuilder.add( stateful1 )
                             .add( stateful2 )
                             .add( stateful3 )
                             .add( stateful4 )
                             .add( stateful5 )
                             .add( stateful6 )
                             .add( stateless1 )
                             .add( stateless2 )
                             .connect( stateful1.getId(), stateful2.getId() )
                             .connect( stateful2.getId(), stateful3.getId() )
                             .connect( stateful2.getId(), stateful4.getId() )
                             .connect( stateful5.getId(), stateful4.getId() )
                             .connect( stateful4.getId(), stateful6.getId() )
                             .connect( stateful4.getId(), stateless1.getId() )
                             .connect( stateless1.getId(), stateless2.getId() )
                             .build();

        final JokerConfigBuilder configBuilder = new JokerConfigBuilder();
        configBuilder.getFlowDefOptimizerConfigBuilder().disableMergeRegions();
        final JokerConfig config = configBuilder.build();

        final IdGenerator idGenerator = new IdGenerator();
        final RegionDefFormer regionDefFormer = new RegionDefFormerImpl( idGenerator );
        final FlowDefOptimizer flowDefOptimizer = new FlowDefOptimizerImpl( config, idGenerator );

        regions = flowDefOptimizer.optimize( flow, regionDefFormer.createRegions( flow ) )._2;

        statefulRegion1 = getRegionByFirstOperator( regions, stateful1.getId() );
        statefulRegion2 = getRegionByFirstOperator( regions, stateful2.getId() );
        statefulRegion3 = getRegionByFirstOperator( regions, stateful3.getId() );
        statefulRegion4 = getRegionByFirstOperator( regions, stateful4.getId() );
        statefulRegion5 = getRegionByFirstOperator( regions, stateful5.getId() );
        statefulRegion6 = getRegionByFirstOperator( regions, stateful6.getId() );
        statelessRegion = getRegionByFirstOperator( regions, stateless1.getId() );
    }

    @Test
    public void testStatefulRegion1Downstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statefulRegion1 );

        assertThat( downstream, hasSize( 5 ) );
        assertThat( downstream, hasItem( statefulRegion2 ) );
        assertThat( downstream, hasItem( statefulRegion3 ) );
        assertThat( downstream, hasItem( statefulRegion4 ) );
        assertThat( downstream, hasItem( statefulRegion6 ) );
        assertThat( downstream, hasItem( statelessRegion ) );
    }

    @Test
    public void testStatefulRegion2Downstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statefulRegion2 );

        assertThat( downstream, hasSize( 4 ) );
        assertThat( downstream, hasItem( statefulRegion3 ) );
        assertThat( downstream, hasItem( statefulRegion4 ) );
        assertThat( downstream, hasItem( statefulRegion6 ) );
        assertThat( downstream, hasItem( statelessRegion ) );
    }

    @Test
    public void testStatefulRegion3Downstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statefulRegion3 );

        assertThat( downstream, hasSize( 0 ) );
    }

    @Test
    public void testStatefulRegion4Downstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statefulRegion4 );

        assertThat( downstream, hasSize( 2 ) );
        assertThat( downstream, hasItem( statefulRegion6 ) );
        assertThat( downstream, hasItem( statelessRegion ) );
    }

    @Test
    public void testStatefulRegion5Downstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statefulRegion5 );

        assertThat( downstream, hasSize( 3 ) );
        assertThat( downstream, hasItem( statefulRegion4 ) );
        assertThat( downstream, hasItem( statefulRegion6 ) );
        assertThat( downstream, hasItem( statelessRegion ) );
    }

    @Test
    public void testStatefulRegion6Downstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statefulRegion6 );

        assertThat( downstream, hasSize( 0 ) );
    }

    @Test
    public void testStatelessRegionDownstream ()
    {
        final List<RegionDef> downstream = getWholeDownstream( flow, regions, statelessRegion );

        assertThat( downstream, hasSize( 0 ) );
    }

    @Test
    public void testLeftMostRegions1 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statefulRegion1 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion1 ) ) );
    }

    @Test
    public void testLeftMostRegions2 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statefulRegion2 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion2 ) ) );
    }

    @Test
    public void testLeftMostRegions3 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statefulRegion3 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion3 ) ) );
    }

    @Test
    public void testLeftMostRegions4 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statefulRegion4 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion4 ) ) );
    }

    @Test
    public void testLeftMostRegions5 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statefulRegion5 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion5 ) ) );
    }

    @Test
    public void testLeftMostRegions6 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statefulRegion6 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion6 ) ) );
    }

    @Test
    public void testLeftMostRegions7 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, singletonList( statelessRegion ) );

        assertThat( leftMost, equalTo( singletonList( statelessRegion ) ) );
    }

    @Test
    public void testLeftMostRegions8 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion1, statefulRegion2, statefulRegion3 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion1 ) ) );
    }

    @Test
    public void testLeftMostRegions9 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion1, statefulRegion3, statefulRegion6 ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion1 ) ) );
    }

    @Test
    public void testLeftMostRegions10 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow,
                                                             regions,
                                                             asList( statefulRegion1, statefulRegion3, statefulRegion5, statefulRegion6 ) );

        assertThat( leftMost, hasSize( 2 ) );
        assertThat( leftMost, hasItem( statefulRegion1 ) );
        assertThat( leftMost, hasItem( statefulRegion5 ) );
    }

    @Test
    public void testLeftMostRegions11 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion4, statelessRegion ) );

        assertThat( leftMost, equalTo( singletonList( statefulRegion4 ) ) );
    }

    @Test
    public void testLeftMostRegions12 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion6, statelessRegion ) );

        assertThat( leftMost, equalTo( asList( statefulRegion6, statelessRegion ) ) );
    }

    @Test
    public void testLeftMostRegions13 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion1, statefulRegion5 ) );

        assertThat( leftMost, equalTo( asList( statefulRegion1, statefulRegion5 ) ) );
    }

    @Test
    public void testLeftMostRegions14 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion2, statefulRegion5 ) );

        assertThat( leftMost, equalTo( asList( statefulRegion2, statefulRegion5 ) ) );
    }

    @Test
    public void testLeftMostRegions15 ()
    {
        final List<RegionDef> leftMost = getLeftMostRegions( flow, regions, asList( statefulRegion3, statefulRegion5 ) );

        assertThat( leftMost, equalTo( asList( statefulRegion3, statefulRegion5 ) ) );
    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 0, outputPortCount = 1 )
    public static class StatefulOperatorInput0Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }


    @OperatorSpec( type = STATEFUL, inputPortCount = 1, outputPortCount = 1 )
    public static class StatefulOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }


    @OperatorSpec( type = STATELESS, inputPortCount = 1, outputPortCount = 1 )
    public static class StatelessOperatorInput1Output1 implements Operator
    {

        @Override
        public SchedulingStrategy init ( final InitCtx ctx )
        {
            return null;
        }

        @Override
        public void invoke ( final InvocationCtx ctx )
        {

        }

    }

}
