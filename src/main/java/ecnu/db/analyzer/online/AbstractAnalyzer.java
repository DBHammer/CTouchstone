package ecnu.db.analyzer.online;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.analyzer.online.node.ExecutionNode;
import ecnu.db.analyzer.online.node.NodeTypeTool;
import ecnu.db.analyzer.online.node.NodeTypeRefFactory;
import ecnu.db.analyzer.statical.QueryAliasParser;
import ecnu.db.dbconnector.DatabaseConnectorInterface;
import ecnu.db.schema.Schema;
import ecnu.db.utils.ConfigConvert;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author wangqingshuai
 */
public abstract class AbstractAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAnalyzer.class);

    protected DatabaseConnectorInterface dbConnector;
    protected Map<String, String> aliasDic;
    protected QueryAliasParser queryAliasParser = new QueryAliasParser();
    protected HashMap<String, Schema> schemas;
    protected int sqlArgIndex = 0;
    protected int lastArgIndex = 0;
    protected HashMap<String, List<String>> argsAndIndex = new HashMap<>();
    protected NodeTypeTool nodeTypeRef;
    protected String databaseVersion;

    AbstractAnalyzer(String databaseVersion, DatabaseConnectorInterface dbConnector, HashMap<String, Schema> schemas) {
        this.nodeTypeRef = NodeTypeRefFactory.getNodeTypeRef(databaseVersion);
        this.databaseVersion = databaseVersion;
        this.dbConnector = dbConnector;
        this.schemas = schemas;
    }

    abstract String[] getSqlInfoColumns(String databaseVersion) throws TouchstoneToolChainException;

    /**
     * 获取数据库使用的静态解析器的数据类型
     * @return 静态解析器使用的数据库类型
     */
    public abstract String getDbType();

    /**
     * 从operator_info里提取tableName
     * @param operatorInfo 需要处理的operator_info
     * @return 提取的表名
     */
    abstract String extractTableName(String operatorInfo);

    /**
     * 查询树的解析
     *
     * @param queryPlan query解析出的查询计划，带具体的行数
     * @return 查询树Node信息
     * @throws TouchstoneToolChainException 查询树无法解析
     */
    public abstract ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneToolChainException;

    /**
     * 分析join信息
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneToolChainException 无法分析的join条件
     */
    abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneToolChainException;

    /**
     * 创建colName到对应的oprator的multimap，并返回关于where_condition的信息
     * @param operatorInfo 需要处理的operator_info
     * @param conditions 用于创建的multimap
     * @return 关于whereExpr的信息
     * @throws TouchstoneToolChainException 无法分析的部分
     */
    abstract WhereConditionInfo buildConditionMap(String operatorInfo, Multimap<String, String> conditions) throws TouchstoneToolChainException;

    /**
     * 分析传入的select 过滤条件，传出表名和格式化后的condition
     * todo 增加对or的支持
     *
     * @param operatorInfo 传入的select的operatorInfo
     * @return 表名和格式化后的condition
     */
    Pair<String, String> analyzeSelectCondition(String operatorInfo) throws TouchstoneToolChainException {
        Multimap<String, String> conditionMap = ArrayListMultimap.create();
        WhereConditionInfo ret = buildConditionMap(operatorInfo, conditionMap);
        boolean useAlias = ret.useAlias, isOr = ret.isOr;
        String tableName = ret.tableName;
        if (aliasDic != null && aliasDic.containsKey(tableName)) {
            tableName = aliasDic.get(tableName);
        }
        StringBuilder conditionStr = new StringBuilder();
        for (Map.Entry<String, String> entry : conditionMap.entries()) {
            String columnName = entry.getKey();
            String operator = entry.getValue();
            conditionStr.append(String.format("%s@%s#", columnName, operator));
            String selectArgName = columnName + " " + operator;
            if (useAlias) {
                selectArgName = tableName + "." + selectArgName;
            }
            StringBuilder selectArgs = new StringBuilder();

            if (operator.contains("in")) {
                int dateOrNot = schemas.get(tableName).isDate(columnName) ? 1 : 0;
                int inCount = Integer.parseInt(operator.split("[()]")[1]);
                selectArgs = new StringBuilder(String.format("(%s)",
                        IntStream.range(0, inCount)
                                .mapToObj((i) -> String.format("'#%d,%d,%d#'", sqlArgIndex, i, dateOrNot))
                                .collect(Collectors.joining(", "))));
                sqlArgIndex++;
            } else {
                if ("bet".equals(operator)) {
                    selectArgs.append(String.format("ween '#%d,0,%d#' and '#%d,1,%d#'",
                            sqlArgIndex,
                            schemas.get(tableName).isDate(columnName) ? 1 : 0,
                            sqlArgIndex++,
                            schemas.get(tableName).isDate(columnName) ? 1 : 0));
                } else {
                    selectArgs.append(String.format("#%d,0,%d#", sqlArgIndex++, schemas.get(tableName).isDate(columnName) ? 1 : 0));
                }
            }
            if (argsAndIndex.containsKey(selectArgName)) {
                argsAndIndex.get(selectArgName).add(selectArgs.toString());
            } else {
                List<String> value = new ArrayList<>();
                value.add(selectArgs.toString());
                argsAndIndex.put(selectArgName, value);
            }
        }
        if (!isOr && conditionMap.size() > 1) {
            conditionStr.append("and");
        } else if (isOr && conditionMap.size() > 1) {
            conditionStr.append("or");
        }


        return new MutablePair<>(tableName, conditionStr.toString());
    }

    public List<String[]> getQueryPlan(String queryCanonicalName, String sql) throws SQLException, TouchstoneToolChainException {
        aliasDic = queryAliasParser.getTableAlias(sql, getDbType());
        return dbConnector.explainQuery(queryCanonicalName, sql, getSqlInfoColumns(databaseVersion));
    }

    /**
     * 获取查询树的约束链信息和表信息
     *
     * @param root 查询树
     * @return 该查询树结构出的约束链信息和表信息
     */
    public List<String> extractQueryInfos(ExecutionNode root) throws SQLException {

        List<String> queryInfos = new ArrayList<>();
        do {

            QueryInfo queryInfo = null;
            try {
                queryInfo = extractConstraintChain(root);
            } catch (TouchstoneToolChainException e) {
                e.printStackTrace();
            }
            if (queryInfo == null) {
                break;
            }
            if (!queryInfo.getData().isBlank()) {
                queryInfos.add("[" + queryInfo.getTableName() + "];" + queryInfo.getData());
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
    private QueryInfo extractConstraintChain(ExecutionNode node) throws TouchstoneToolChainException, SQLException {

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

        //获取来自子节点的constraintChain
        QueryInfo constraintChain = extractConstraintChain(node.getLeftNode());
        if (constraintChain == null) {
            constraintChain = extractConstraintChain(node.getRightNode());
        }

        // 如果没有获取到constraintChain，则本身为filter节点或者scan节点
        if (constraintChain == null) {
            node.setVisited();
            if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
                Pair<String, String> tableNameAndSelectCondition = analyzeSelectCondition(node.getInfo());
                String selectInfo = "[0," + tableNameAndSelectCondition.getRight() + "," +
                        (double) node.getOutputRows() / schemas.get(tableNameAndSelectCondition.getLeft()).getTableSize() + "];";
                return new QueryInfo(selectInfo, tableNameAndSelectCondition.getLeft(), node.getOutputRows());
            } else if (node.getType() == ExecutionNode.ExecutionNodeType.scan) {
                String tableName = extractTableName(node.getInfo());
                Schema schema = schemas.get(tableName);
                return new QueryInfo("", tableName, schema.getTableSize());
            } else {
                throw new TouchstoneToolChainException("join节点的左右节点已经被访问过");
            }
        } else {
            if (node.getType() == ExecutionNode.ExecutionNodeType.join && node.getLeftNode().isVisited() && node.getRightNode().isVisited()) {
                node.setVisited();
            }
            // 如果遍历结束，则不需要继续遍历
            if (!constraintChain.isStop()) {
                if (node.getType() == ExecutionNode.ExecutionNodeType.join) {
                    String[] joinColumnInfos = analyzeJoinInfo(node.getInfo());
                    String pkTable = joinColumnInfos[0], pkCol = joinColumnInfos[1],
                            fkTable = joinColumnInfos[2], fkCol = joinColumnInfos[3];
                    //如果当前的join节点，不属于之前遍历的节点，则停止继续向上访问
                    if (!pkTable.equals(constraintChain.getTableName())
                            && !fkTable.equals(constraintChain.getTableName())) {
                        constraintChain.setStop();
                    } else {
                        //将本表的信息放在前面，交换位置
                        if (constraintChain.getTableName().equals(fkTable)) {
                            pkTable = joinColumnInfos[2];
                            pkCol = joinColumnInfos[3];
                            fkTable = joinColumnInfos[0];
                            fkCol = joinColumnInfos[1];
                        }
                        //根据主外键分别设置约束链输出信息
                        if (isPrimaryKey(pkTable, pkCol, fkTable, fkCol)) {
                            constraintChain.setStop();
                            if (node.getJoinTag() < 0) {
                                node.setJoinTag(schemas.get(pkTable).getJoinTag());
                            }
                            constraintChain.addConstraint("[1," + pkCol.replace(',', '#') + "," +
                                    node.getJoinTag() + "," + 2 * node.getJoinTag() + "];");
                            //设置主键
                            schemas.get(pkTable).setPrimaryKeys(pkCol);
                            constraintChain.setLastNodeLineCount(node.getOutputRows());
                        } else {
                            if (node.getJoinTag() < 0) {
                                node.setJoinTag(schemas.get(pkTable).getJoinTag());
                            }
                            String primaryKey = fkTable + "." + fkCol;
                            constraintChain.addConstraint("[2," + fkCol.replace(',', '#') + "," +
                                    (double) node.getOutputRows() / constraintChain.getLastNodeLineCount() + "," +
                                    primaryKey.replace(',', '#') + "," +
                                    node.getJoinTag() + "," + 2 * node.getJoinTag() + "];");
                            //设置外键
                           LOGGER.debug("table:" + pkTable + ".column:" + pkCol + " -ref- table:" +
                                    fkCol + ".column:" + fkTable);
                            schemas.get(pkTable).addForeignKey(pkCol, fkTable, fkCol);
                            constraintChain.setLastNodeLineCount(node.getOutputRows());
                        }
                    }
                } else if (node.getType() == ExecutionNode.ExecutionNodeType.filter) {
                    Pair<String, String> tableNameAndSelectCondition = analyzeSelectCondition(node.getInfo());
                    if (constraintChain.getTableName().equals(tableNameAndSelectCondition.getKey())) {
                        node.setVisited();
                        constraintChain.addConstraint("[0," + tableNameAndSelectCondition.getRight() + "," +
                                (double) node.getOutputRows() / constraintChain.getLastNodeLineCount() + "];");
                        constraintChain.setLastNodeLineCount(node.getOutputRows());
                    }
                }
            }
            return constraintChain;
        }
    }

    /**
     * 根据输入的列名统计非重复值的个数，进而给出该列是否为主键
     * @param pkTable
     * @param pkCol
     * @param fkTable
     * @param fkCol
     * @return 该列是否为主键
     * @throws TouchstoneToolChainException
     * @throws SQLException
     */
    private boolean isPrimaryKey(String pkTable, String pkCol, String fkTable, String fkCol) throws TouchstoneToolChainException, SQLException {
        if (String.format("%s.%s", pkTable, pkCol).equals(schemas.get(fkTable).getMetaDataFks().get(fkCol))) {
            return true;
        }
        if (String.format("%s.%s", fkTable, fkCol).equals(schemas.get(pkTable).getMetaDataFks().get(pkCol))) {
            return false;
        }
        if (!pkCol.contains(",")) {
            if (schemas.get(pkTable).getNdv(pkCol) == schemas.get(fkTable).getNdv(fkCol)) {
                return schemas.get(pkTable).getTableSize() < schemas.get(fkTable).getTableSize();
            } else {
                return schemas.get(pkTable).getNdv(pkCol) > schemas.get(fkTable).getNdv(fkCol);
            }
        } else {
            int leftTableNdv = dbConnector.getMultiColNdv(pkTable, pkCol);
            int rightTableNdv = dbConnector.getMultiColNdv(fkTable, fkCol);
            if (leftTableNdv == rightTableNdv) {
                return schemas.get(pkTable).getTableSize() < schemas.get(fkTable).getTableSize();
            } else {
                return leftTableNdv > rightTableNdv;
            }
        }
    }

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
