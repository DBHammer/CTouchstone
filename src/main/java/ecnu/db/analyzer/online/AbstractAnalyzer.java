package ecnu.db.analyzer.online;

import ecnu.db.analyzer.statical.QueryAliasParser;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractAnalyzer {
    protected AbstractDbConnector dbConnector;
    protected Map<String, String> aliasDic;
    protected QueryAliasParser queryAliasParser = new QueryAliasParser();
    protected HashMap<String, Schema> schemas;
    protected int sqlArgIndex = 0;
    protected int lastArgIndex = 0;
    protected HashMap<String, List<String>> argsAndIndex = new HashMap<>();


    AbstractAnalyzer(AbstractDbConnector dbConnector, HashMap<String, Schema> schemas) {
        this.dbConnector = dbConnector;
        this.schemas = schemas;
    }

    abstract String[] getSqlInfoColumns();

    public abstract String getDbType();

    /**
     * 查询树的解析
     *
     * @param queryPlan query解析出的查询计划，带具体的行数
     * @return 查询树Node信息
     * @throws TouchstoneToolChainException 查询树无法解析
     */
    public abstract ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneToolChainException;

    public List<String[]> getQueryPlan(String sql) throws SQLException {
        aliasDic = queryAliasParser.getTableAlias(sql, getDbType());
        return dbConnector.explainQuery(sql, getSqlInfoColumns());
    }

    /**
     * 将查询树重构为约束链
     *
     * @param root 查询树的根
     * @return 该查询树结构出的约束链
     */
    public List<String> outputNode(ExecutionNode root) throws SQLException {

        List<String> queryInfos = new ArrayList<>();
        do {
            QueryInfoChain queryInfo = null;
            try {
                queryInfo = getQueryInfo(root);
            } catch (TouchstoneToolChainException e) {
                e.printStackTrace();
            }
            if (queryInfo == null) {
                break;
            } else {
                if (!queryInfo.getQueryInfo().isBlank()) {
                    queryInfos.add("[" + queryInfo.getTableName() + "];" + queryInfo.getQueryInfo());
                }
            }
        } while (true);

        return queryInfos;
    }

    /**
     * 获取一条没有输出过的query约束链
     *
     * @param node 输入查询语法树的根节点
     * @return 没有输出过的query约束链
     */
    private QueryInfoChain getQueryInfo(ExecutionNode node) throws TouchstoneToolChainException, SQLException {

        // 如果节点为空则直接返回
        if (node == null) {
            return null;
        }
        // 如果节点为filter节点，且节点的子节点也已经被访问过则返回
        if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
            if (node.isVisited()) {
                if (node.getLeftNode() == null || node.getLeftNode().isVisited()) {
                    return null;
                }
            }
        }
        //如果节点为join节点，且已经被访问过，则直接返回
        else {
            if (node.isVisited()) {
                return null;
            }
        }

        //获取来自子节点的query info
        QueryInfoChain queryInfo = getQueryInfo(node.getLeftNode());
        if (queryInfo == null) {
            queryInfo = getQueryInfo(node.getRightNode());
        }

        // 如果没有获取到query info，则本身为filter节点或者scan节点
        if (queryInfo == null) {
            node.setVisited();
            if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
                Pair<String, String> tableNameAndSelectCondition = analyzeSelectCondition(node.getInfo());
                String selectInfo = "[0," + tableNameAndSelectCondition.getRight() + "," +
                        (double) node.getOutputRows() / schemas.get(tableNameAndSelectCondition.getLeft()).getTableSize() + "];";
                return new QueryInfoChain(selectInfo, tableNameAndSelectCondition.getLeft(), node.getOutputRows());
            } else if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
                String tableName = "";
                if (node.getInfo().contains("funcs")) {
                    int index = node.getInfo().split(",")[1].lastIndexOf(".");
                    tableName = node.getInfo().split(",")[1].substring(6, index).toLowerCase();
                } else {
                    tableName = node.getInfo().split(",")[0].substring(6).toLowerCase();
                }
                if (aliasDic != null && aliasDic.containsKey(tableName)) {
                    tableName = aliasDic.get(tableName);
                }
                return new QueryInfoChain("", tableName, schemas.get(tableName).getTableSize());
            } else {
                throw new TouchstoneToolChainException("join节点的左右节点已经被访问过");
            }
        } else {
            // 如果遍历结束，则不需要继续遍历
            if (!queryInfo.isStop()) {
                if (node.getType() == ExecutionNode.ExecutionNodeType.join) {
                    String[] joinColumnInfos = analyzeJoinInfo(node.getInfo());
                    //如果当前的join节点，不属于之前遍历的节点，则停止继续向上访问
                    if (!joinColumnInfos[0].equals(queryInfo.getTableName())
                            && !joinColumnInfos[2].equals(queryInfo.getTableName())) {
                        System.out.println(node.getInfo());
                        System.out.println(queryInfo.getTableName() + " " + joinColumnInfos[0] + " " + joinColumnInfos[2]);
                        queryInfo.setStop();
                    } else {
                        //将本表的信息放在前面，交换位置
                        if (queryInfo.getTableName().equals(joinColumnInfos[2])) {
                            String temp = joinColumnInfos[0];
                            joinColumnInfos[0] = joinColumnInfos[2];
                            joinColumnInfos[2] = temp;
                            temp = joinColumnInfos[1];
                            joinColumnInfos[1] = joinColumnInfos[3];
                            joinColumnInfos[3] = temp;
                        }
                        //根据主外键分别设置约束链输出信息
                        if (isPrimaryKey(joinColumnInfos)) {
                            queryInfo.setStop();
                            if (node.getJoinTag() < 0) {
                                node.setJoinTag(schemas.get(joinColumnInfos[0]).getJoinTag());
                            }
                            queryInfo.addQueryInfo("[1," + joinColumnInfos[1].replace(',', '#') + "," +
                                    node.getJoinTag() + "," + 2 * node.getJoinTag() + "];");
                            //设置主键
                            schemas.get(joinColumnInfos[0]).setPrimaryKeys(joinColumnInfos[1]);
                        } else {
                            if (node.getJoinTag() < 0) {
                                node.setJoinTag(schemas.get(joinColumnInfos[0]).getJoinTag());
                            }
                            String primaryKey = joinColumnInfos[2] + "." + joinColumnInfos[3];
                            queryInfo.addQueryInfo("[2," + joinColumnInfos[1].replace(',', '#') + "," +
                                    (double) node.getOutputRows() / queryInfo.getLastNodeLineCount() + "," +
                                    primaryKey.replace(',', '#') + "," +
                                    node.getJoinTag() + "," + 2 * node.getJoinTag() + "];");
                            //设置外键
                            System.out.println("table:" + joinColumnInfos[0] + ".column:" + joinColumnInfos[1] + " -ref- table:" +
                                    joinColumnInfos[2] + ".column:" + joinColumnInfos[3]);
                            schemas.get(joinColumnInfos[0]).addForeignKey(joinColumnInfos[1], joinColumnInfos[2], joinColumnInfos[3]);
                            queryInfo.setLastNodeLineCount(node.getOutputRows());
                        }
                        if (node.getLeftNode().isVisited() && node.getRightNode().isVisited()) {
                            node.setVisited();
                        }
                    }
                } else {
                    if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
                        Pair<String, String> tableNameAndSelectCondition = analyzeSelectCondition(node.getInfo());
                        if (queryInfo.getTableName().equals(tableNameAndSelectCondition.getKey())) {
                            node.setVisited();
                            queryInfo.addQueryInfo("[0," + tableNameAndSelectCondition.getRight() + "," +
                                    (double) node.getOutputRows() / queryInfo.getLastNodeLineCount() + "];");
                        }
                    }
                }
            }
            return queryInfo;
        }
    }

    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     *
     * @param columnInfos 输入的列名
     * @return 该列是否为主键
     */
    private boolean isPrimaryKey(String[] columnInfos) throws TouchstoneToolChainException, SQLException {
        if (!columnInfos[1].contains(",")) {
            if (schemas.get(columnInfos[0]).getNdv(columnInfos[1]) == schemas.get(columnInfos[2]).getNdv(columnInfos[3])) {
                return schemas.get(columnInfos[0]).getTableSize() < schemas.get(columnInfos[2]).getTableSize();
            } else {
                return schemas.get(columnInfos[0]).getNdv(columnInfos[1]) > schemas.get(columnInfos[2]).getNdv(columnInfos[3]);
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColumnsNdv(columnInfos[0], columnInfos[1]);
            int rightTableNdv = dbConnector.getMultiColumnsNdv(columnInfos[2], columnInfos[3]);
            if (leftTableNdv == rightTableNdv) {
                return schemas.get(columnInfos[0]).getTableSize() < schemas.get(columnInfos[2]).getTableSize();
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

    /**
     * 分析join信息
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneToolChainException 无法分析的join条件
     */
    abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneToolChainException;


    /**
     * 分析传入的select 过滤条件，传出表名和格式化后的condition
     * todo 增加对or的支持
     *
     * @param selectCondition 传入的select条件语句
     * @return 表名和格式化后的condition
     */
    abstract Pair<String, String> analyzeSelectCondition(String selectCondition) throws TouchstoneToolChainException;

    public HashMap<String, List<String>> getArgsAndIndex() {
        return argsAndIndex;
    }

    public void outputSuccess(boolean success) {
        if (success) {
            lastArgIndex = sqlArgIndex;
        } else {
            sqlArgIndex = lastArgIndex;
        }
        for (Schema schema : schemas.values()) {
            schema.keepJoinTag(success);
        }
    }
}
