package ecnu.db.generator.constraintchain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.ortools.sat.CpModel;
import com.google.ortools.sat.IntVar;
import com.google.ortools.sat.LinearExpr;
import ecnu.db.generator.constraintchain.agg.ConstraintChainAggregateNode;
import ecnu.db.generator.constraintchain.filter.ConstraintChainFilterNode;
import ecnu.db.generator.constraintchain.filter.Parameter;
import ecnu.db.generator.constraintchain.join.ConstraintChainFkJoinNode;
import ecnu.db.generator.constraintchain.join.ConstraintChainPkJoinNode;
import ecnu.db.generator.constraintchain.join.ConstraintNodeJoinType;
import ecnu.db.generator.joininfo.JoinStatus;
import ecnu.db.utils.exception.schema.CannotFindColumnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private final List<ConstraintChainNode> nodes = new ArrayList<>();
    private final Logger logger = LoggerFactory.getLogger(ConstraintChain.class);

    @JsonIgnore
    private final Set<String> joinTables = new HashSet<>();
    private String tableName;

    public ConstraintChain() {
    }

    public ConstraintChain(String tableName) {
        this.tableName = tableName;
    }

    public void addJoinTable(String tableName) {
        joinTables.add(tableName);
    }

    public Set<String> getJoinTables() {
        return joinTables;
    }

    public void addNode(ConstraintChainNode node) {
        nodes.add(node);
    }

    public List<ConstraintChainNode> getNodes() {
        return nodes;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonIgnore
    public List<Parameter> getParameters() {
        return nodes.stream().filter(ConstraintChainFilterNode.class::isInstance)
                .map(ConstraintChainFilterNode.class::cast)
                .map(ConstraintChainFilterNode::getParameters)
                .flatMap(Collection::stream).toList();
    }

    @Override
    public String toString() {
        return "{tableName:" + tableName + ",nodes:" + nodes + "}";
    }

    /**
     * 给定range空间 计算filter的状态
     *
     * @param range 批大小
     * @return filter状态
     */
    public boolean[] evaluateFilterStatus(int range) {
        if (nodes.get(0).getConstraintChainNodeType() == ConstraintChainNodeType.FILTER) {
            try {
                return ((ConstraintChainFilterNode) nodes.get(0)).evaluate();
            } catch (CannotFindColumnException e) {
                e.printStackTrace();
                return new boolean[0];
            }
        } else {
            boolean[] ret = new boolean[range];
            Arrays.fill(ret, true);
            return ret;
        }
    }

    public void computePkStatus(List<List<boolean[]>> fkStatus, boolean[] filterStatus, boolean[][] pkStatus) {
        for (ConstraintChainNode node : nodes) {
            if (node.getConstraintChainNodeType() == ConstraintChainNodeType.FK_JOIN) {
                ConstraintChainFkJoinNode fkJoinNode = (ConstraintChainFkJoinNode) node;
                int joinStatusIndex = fkJoinNode.joinStatusIndex;
                int joinStatusLocation = fkJoinNode.joinStatusLocation;
                for (int i = 0; i < fkStatus.size(); i++) {
                    if (filterStatus[i]) {
                        filterStatus[i] = fkStatus.get(i).get(joinStatusIndex)[joinStatusLocation]==
                                (fkJoinNode.getType() != ConstraintNodeJoinType.ANTI_SEMI_JOIN);
                    }
                }
            } else if (node.getConstraintChainNodeType() == ConstraintChainNodeType.PK_JOIN) {
                pkStatus[((ConstraintChainPkJoinNode) node).getPkTag()] = filterStatus;
                break;
            }
        }
    }

    public void addConstraint2Model(CpModel model, IntVar[] vars, int filterIndex, int filterSize, int unFilterSize,
                                    List<Map.Entry<JoinStatus, List<boolean[]>>> filterStatus2PkStatus) {
        // 每一组填充状态是否能到达这个算子
        boolean[] canBeInput = new boolean[filterStatus2PkStatus.size()];
        Arrays.fill(canBeInput, true);
        for (ConstraintChainNode node : nodes) {
            // filter的状态 根据其filter status对应的index的位置获得
            switch (node.getConstraintChainNodeType()) {
                case FILTER -> {
                    int i = 0;
                    for (Map.Entry<JoinStatus, List<boolean[]>> status2PkStatus : filterStatus2PkStatus) {
                        canBeInput[i++] = status2PkStatus.getKey().status()[filterIndex];
                    }
                }
                // join的状态根据前缀状态获得
                // todo 考虑index join
                // todo 考虑semi join 和 outer join
                case FK_JOIN -> {
                    ConstraintChainFkJoinNode fkJoinNode = (ConstraintChainFkJoinNode) node;
                    // 获取join对应的位置
                    int joinStatusIndex = fkJoinNode.joinStatusIndex;
                    int joinStatusLocation = fkJoinNode.joinStatusLocation;
                    // 找到有效的CpModel变量
                    if (fkJoinNode.getProbabilityWithFailFilter() != null) {
                        List<IntVar> indexJoinVars = new ArrayList<>();
                        IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> !canBeInput[i]).forEach(i -> {
                            if (filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex)[joinStatusLocation] ==
                                    (fkJoinNode.getType() != ConstraintNodeJoinType.ANTI_SEMI_JOIN)) {
                                logger.info("indexJoin {}", i);
                                logger.info(filterStatus2PkStatus.get(i).getKey().toString());
                                logger.info(filterStatus2PkStatus.get(i).getValue().toString());
                                indexJoinVars.add(vars[i]);
                            }
                        });
                        int unfilterSize = fkJoinNode.getProbabilityWithFailFilter().multiply(BigDecimal.valueOf(unFilterSize)).intValue();
                        logger.info("输出的数据量为:{}, 属于第{}个过滤算子, 为第{}个表的第{}个状态", unfilterSize, filterIndex, joinStatusIndex, joinStatusLocation);
                        model.addEquality(LinearExpr.sum(indexJoinVars.toArray(IntVar[]::new)), unfilterSize);
                    }
                    List<IntVar> validVars = new ArrayList<>();
                    IntStream.range(0, filterStatus2PkStatus.size()).filter(i -> canBeInput[i]).forEach(i -> {
                        if (filterStatus2PkStatus.get(i).getValue().get(joinStatusIndex)[joinStatusLocation] ==
                                (fkJoinNode.getType() != ConstraintNodeJoinType.ANTI_SEMI_JOIN)) {
                            validVars.add(vars[i]);
                        } else {
                            canBeInput[i] = false;
                        }
                    });
                    filterSize = fkJoinNode.getProbability().multiply(BigDecimal.valueOf(filterSize)).intValue();
                    logger.info("输出的数据量为:{}, 属于第{}个过滤算子, 为第{}个表的第{}个状态", filterSize, filterIndex, joinStatusIndex, joinStatusLocation);
                    model.addEquality(LinearExpr.sum(validVars.toArray(IntVar[]::new)), filterSize);
                }
                default -> {
                }
            }
        }
    }

    public StringBuilder presentConstraintChains(Map<String, SubGraph> subGraphHashMap, String color) {
        String lastNodeInfo = "";
        double lastProbability = 0;
        String conditionColor = String.format("[style=filled, color=\"%s\"];%n", color);
        String tableColor = String.format("[shape=box,style=filled, color=\"%s\"];%n", color);
        StringBuilder graph = new StringBuilder();
        for (ConstraintChainNode node : nodes) {
            String currentNodeInfo;
            double currentProbability = 0;
            switch (node.constraintChainNodeType) {
                case FILTER -> {
                    currentNodeInfo = String.format("\"%s\"", node);
                    currentProbability = ((ConstraintChainFilterNode) node).getProbability().doubleValue();
                    graph.append("\t").append(currentNodeInfo).append(conditionColor);
                }
                case FK_JOIN -> {
                    ConstraintChainFkJoinNode fkJoinNode = ((ConstraintChainFkJoinNode) node);
                    String pkCols = fkJoinNode.getRefCols().split("\\.")[2];
                    currentNodeInfo = String.format("\"Fk%s%d\"", pkCols, fkJoinNode.getPkTag());
                    String subGraphTag = String.format("cluster%s%d", pkCols, fkJoinNode.getPkTag());
                    currentProbability = fkJoinNode.getProbability().doubleValue();
                    subGraphHashMap.putIfAbsent(subGraphTag, new SubGraph(subGraphTag));
                    subGraphHashMap.get(subGraphTag).fkInfo = currentNodeInfo + conditionColor;
                    subGraphHashMap.get(subGraphTag).joinLabel = switch (fkJoinNode.getType()) {
                        case INNER_JOIN -> "eq join";
                        case SEMI_JOIN -> "semi join: " + fkJoinNode.getPkDistinctProbability();
                        case OUTER_JOIN -> "outer join: " + fkJoinNode.getPkDistinctProbability();
                        case ANTI_SEMI_JOIN -> "anti join";
                    };
                    if (fkJoinNode.getProbabilityWithFailFilter() != null) {
                        subGraphHashMap.get(subGraphTag).joinLabel = String.format("%s filterWithCannotJoin: %2$,.4f",
                                subGraphHashMap.get(subGraphTag).joinLabel,
                                fkJoinNode.getProbabilityWithFailFilter());
                    }
                }
                case PK_JOIN -> {
                    ConstraintChainPkJoinNode pkJoinNode = ((ConstraintChainPkJoinNode) node);
                    String locPks = pkJoinNode.getPkColumns()[0];
                    currentNodeInfo = String.format("\"Pk%s%d\"", locPks, pkJoinNode.getPkTag());
                    String localSubGraph = String.format("cluster%s%d", locPks, pkJoinNode.getPkTag());
                    subGraphHashMap.putIfAbsent(localSubGraph, new SubGraph(localSubGraph));
                    subGraphHashMap.get(localSubGraph).pkInfo = currentNodeInfo + conditionColor;
                }
                case AGGREGATE -> {
                    ConstraintChainAggregateNode aggregateNode = ((ConstraintChainAggregateNode) node);
                    List<String> keys = aggregateNode.getGroupKey();
                    currentProbability = aggregateNode.getAggProbability().doubleValue();
                    currentNodeInfo = String.format("\"GroupKey:%s\"", keys == null ? "" : String.join(",", keys));
                    graph.append("\t").append(currentNodeInfo).append(conditionColor);
                    if (aggregateNode.getAggFilter() != null) {
                        if (!lastNodeInfo.isBlank()) {
                            graph.append(String.format("\t%s->%s[label=\"%3$,.4f\"];%n", lastNodeInfo, currentNodeInfo, lastProbability));
                        } else {
                            graph.append(String.format("\t\"%s\"%s", tableName, tableColor));
                            graph.append(String.format("\t\"%s\"->%s[label=\"1.0\"]%n", tableName, currentNodeInfo));
                        }
                        lastNodeInfo = currentNodeInfo;
                        lastProbability = currentProbability;
                        ConstraintChainFilterNode aggFilter = aggregateNode.getAggFilter();
                        currentNodeInfo = String.format("\"%s\"", aggFilter);
                        graph.append("\t").append(currentNodeInfo).append(conditionColor);
                        currentProbability = aggFilter.getProbability().doubleValue();
                    }
                }
                default -> throw new UnsupportedOperationException();
            }
            if (!lastNodeInfo.isBlank()) {
                graph.append(String.format("\t%s->%s[label=\"%3$,.4f\"];%n", lastNodeInfo, currentNodeInfo, lastProbability));
            } else {
                graph.append(String.format("\t\"%s\"%s", tableName, tableColor));
                graph.append(String.format("\t\"%s\"->%s[label=\"1.0\"]%n", tableName, currentNodeInfo));
            }
            lastNodeInfo = currentNodeInfo;
            lastProbability = currentProbability;
        }
        if (!lastNodeInfo.startsWith("\"Pk")) {
            graph.append("\t").append("RESULT").append(conditionColor);
            graph.append(String.format("\t%s->RESULT[label=\"%2$,.4f\"]%n", lastNodeInfo, lastProbability));
        }
        return graph;
    }

    @JsonIgnore
    public int getJoinTag() {
        int joinTag = Integer.MIN_VALUE;
        for (ConstraintChainNode node : nodes) {
            if (node.getConstraintChainNodeType() == ConstraintChainNodeType.PK_JOIN) {
                if (joinTag == Integer.MIN_VALUE) {
                    joinTag = ((ConstraintChainPkJoinNode) node).getPkTag();
                } else {
                    throw new UnsupportedOperationException("约束链中存在双主键");
                }
            }
        }
        return joinTag;
    }

    static class SubGraph {
        private final String joinTag;
        String pkInfo;
        String fkInfo;
        String joinLabel;

        public SubGraph(String joinTag) {
            this.joinTag = joinTag;
        }

        @Override
        public String toString() {
            return String.format("""
                    subgraph "%s" {
                            %s
                            %slabel="%s";labelloc=b;
                    }""".indent(4), joinTag, pkInfo, fkInfo, joinLabel);
        }
    }
}
