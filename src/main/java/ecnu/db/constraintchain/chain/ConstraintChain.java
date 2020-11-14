package ecnu.db.constraintchain.chain;

import com.google.common.collect.Table;
import ecnu.db.constraintchain.filter.Parameter;
import ecnu.db.exception.TouchstoneException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

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
     * @param size     需要的size
     * @param pkBitMap pkTag -> bitmaps
     * @param fkBitMap ref_col+local_col -> bitmaps
     * @throws TouchstoneException 计算失败
     */
    public void evaluate(int size, Map<Integer, boolean[]> pkBitMap, Table<String, Integer, boolean[]> fkBitMaps) throws TouchstoneException {
        boolean[] flag = new boolean[size];
        Arrays.fill(flag, true);
        for (ConstraintChainNode node : nodes) {
            switch (node.getConstraintChainNodeType()) {
                case FILTER:
                    int j = 0;
                    // // TODO: 2020/11/14  setsize
                    for (boolean b : ((ConstraintChainFilterNode) node).evaluate()) {
                        flag[j++] &= b;
                    }
                    break;
                case FK_JOIN:
                    //todo 引入规则
                    ConstraintChainFkJoinNode constraintChainFkJoinNode = (ConstraintChainFkJoinNode) node;
                    double probability = constraintChainFkJoinNode.getProbability().doubleValue();
                    for (int i = 0; i < size; i++) {
                        flag[i] &= ThreadLocalRandom.current().nextDouble() > probability;
                    }
                    fkBitMaps.put(constraintChainFkJoinNode.getJoinInfoName(), constraintChainFkJoinNode.getPkTag(), flag.clone());
                    break;
                case PK_JOIN:
                    pkBitMap.put(((ConstraintChainPkJoinNode) node).getPkTag(), flag.clone());
                    break;
                default:
                    throw new TouchstoneException("不支持的Node类型");
            }
        }
    }
}
