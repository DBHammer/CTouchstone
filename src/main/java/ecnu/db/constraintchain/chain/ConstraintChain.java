package ecnu.db.constraintchain.chain;

import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.utils.exception.TouchstoneException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author wangqingshuai
 */
public class ConstraintChain {

    private final List<ConstraintChainNode> nodes = new ArrayList<>();
    private String tableName;
    private List<Parameter> parameters = new ArrayList<>();

    public ConstraintChain() {
    }

    public ConstraintChain(String tableName) {
        this.tableName = tableName;
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

    public void addParameters(List<Parameter> parameters) {
        this.parameters.addAll(parameters);
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "{tableName:" + tableName + ",nodes:" + nodes + "}";
    }

    /**
     * 计算pk和fk的bitmap
     *
     * @param pkBitMap  pkTag -> bitmaps
     * @param fkBitMaps ref_col+local_col -> bitmaps
     * @throws TouchstoneException 计算失败
     */
    public void evaluate(long[] pkBitMap, Map<String, long[]> fkBitMaps) throws TouchstoneException {
        boolean[] flag = new boolean[pkBitMap.length];
        Arrays.fill(flag, true);
        for (ConstraintChainNode node : nodes) {
            switch (node.getConstraintChainNodeType()) {
                case FILTER:
                    boolean[] evaluateStatus = ((ConstraintChainFilterNode) node).evaluate();
                    assert evaluateStatus.length == flag.length;
                    for (int i = 0; i < flag.length; i++) {
                        flag[i] &= evaluateStatus[i];
                    }
                    break;
                case FK_JOIN:
                    //todo 引入规则
                    ConstraintChainFkJoinNode constraintChainFkJoinNode = (ConstraintChainFkJoinNode) node;
                    double probability = constraintChainFkJoinNode.getProbability().doubleValue();
                    long[] fkBitMap = fkBitMaps.get(constraintChainFkJoinNode.getJoinInfoName());
                    long fkTag = constraintChainFkJoinNode.getPkTag();
                    for (int i = 0; i < flag.length; i++) {
                        if (flag[i]) {
                            flag[i] &= ThreadLocalRandom.current().nextDouble() > probability;
                            fkBitMap[i] += fkTag * (1 + Boolean.compare(flag[i], false));
                        }
                    }
                    break;
                case PK_JOIN:
                    long pkTag = ((ConstraintChainPkJoinNode) node).getPkTag();
                    for (int i = 0; i < flag.length; i++) {
                        pkBitMap[i] += pkTag * (1 + Boolean.compare(flag[i], false));
                    }
                    break;
                default:
                    throw new TouchstoneException("不支持的Node类型");
            }
        }
    }
}
