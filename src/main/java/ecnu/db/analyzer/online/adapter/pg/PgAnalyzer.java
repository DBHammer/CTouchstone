package ecnu.db.analyzer.online.adapter.pg;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.ReadContext;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectOperatorInfoLexer;
import ecnu.db.analyzer.online.adapter.pg.parser.PgSelectOperatorInfoParser;
import ecnu.db.generator.constraintchain.filter.logical.AndNode;
import ecnu.db.generator.constraintchain.filter.logical.LogicNode;
import ecnu.db.utils.exception.TouchstoneException;
import java_cup.runtime.ComplexSymbolFactory;

import java.io.StringReader;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static ecnu.db.utils.CommonUtils.matchPattern;


public class PgAnalyzer extends AbstractAnalyzer {

    private static final Pattern CanonicalColumnName = Pattern.compile("[a-zA-Z][a-zA-Z0-9]*\\.[a-zA-Z0-9]+");
    private static final Pattern JOIN_EQ_OPERATOR = Pattern.compile("Cond: \\(.*\\)");
    private static final Pattern EQ_OPERATOR = Pattern.compile("\\(([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+) = ([a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+)\\)");
    private static final String NUMERIC = "'[0-9]+'::numeric";
    private static final String DATE = "'(([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6})|([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})|([0-9]{4}-[0-9]{2}-[0-9]{2}))'::date";
    private static final String TIMESTAMP = "'(([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6})|([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})|([0-9]{4}-[0-9]{2}-[0-9]{2}))'::timestamp without time zone";
    private static final Pattern REDUNDANCY = Pattern.compile(NUMERIC +"|"+ DATE +"|"+ TIMESTAMP);
    private final PgSelectOperatorInfoParser parser = new PgSelectOperatorInfoParser(new PgSelectOperatorInfoLexer(new StringReader("")), new ComplexSymbolFactory());

    public PgAnalyzer() {
        super();
        this.nodeTypeRef = new PgNodeTypeInfo();
    }

    @Override
    public ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneException {
        StringBuilder myQueryPlan = new StringBuilder();
        for (String[] strings : queryPlan) {
            myQueryPlan.append(strings[0]);
        }
        String theQueryPlan = myQueryPlan.toString();
        return buildExecutionTree(theQueryPlan);
    }


    private ExecutionNode buildExecutionTree(String queryPlan) throws TouchstoneException {
        PgNodeTypeInfo NodeTypeRef = new PgNodeTypeInfo();
        ReadContext rc = JsonPath.parse(queryPlan);
        StringBuilder Path = new StringBuilder("$.[0]['Plan']");
        String NodeTypePath = Path + "['Node Type']";
        String NodeType = rc.read(NodeTypePath);
        while (NodeTypeRef.isPassNode(NodeType)) {//找到第一个可以处理的节点
            Path.append("['Plans'][0]");
            NodeTypePath = Path + "['Node Type']";
            NodeType = rc.read(NodeTypePath);
        }
        ExecutionNode node = getExecutionNode(queryPlan, Path.toString());
        Deque<Map.Entry<String, ExecutionNode>> stack = new ArrayDeque<>();
        Deque<Map.Entry<String, ExecutionNode>> rootStack = new ArrayDeque<>();//存放根节点
        rootStack.push(new AbstractMap.SimpleEntry<>(Path.toString(), node));
        stack.push(new AbstractMap.SimpleEntry<>(Path.toString(), node));
        String currentPath;
        while (!stack.isEmpty()) {
            Map.Entry<String, ExecutionNode> pair = stack.pop();
            Path = new StringBuilder(pair.getKey());
            node = pair.getValue();
            if(node.isAdd == false) {
                Map<String, String> allKeys = getKeys(queryPlan, Path.toString());
                if (hasChildPlan(allKeys)) {
                    int plansCount = rc.read(Path + "['Plans'].length()");
                    if (hasChildPlan(allKeys) && plansCount == 2) {
                        currentPath = Path + "['Plans'][1]";
                        String currentNodeType = rc.read(currentPath + "['Node Type']");
                        if (currentNodeType.equals("Hash")) {
                            currentPath += "['Plans'][0]";
                            currentNodeType = rc.read(currentPath + "['Node Type']");
                            while (!(currentNodeType.equals("Seq Scan") || currentNodeType.equals("Hash Join") || currentNodeType.equals("Nested Loop"))) {
                                currentPath += "['Plans'][0]";
                                currentNodeType = rc.read(currentPath + "['Node Type']");
                            }
                        }
                        ExecutionNode currentNode = getExecutionNode(queryPlan, currentPath);
                        node.rightNode = currentNode;
                        stack.push(new AbstractMap.SimpleEntry<>(currentPath, currentNode));
                        plansCount--;
                    }
                    if (hasChildPlan(allKeys) && plansCount == 1) {
                        currentPath = Path + "['Plans'][0]";
                        String rightPath = Path + "['Plans'][1]";
                        if (hasFilterInfo(allKeys)) {
                            String filterInfo = rc.read(Path + "['Filter']");
                            if (filterInfo.equals("(NOT (hashed SubPlan 1))")) {
                                String tableName = rc.read(Path + "['Schema']") + "." + rc.read(Path + "['Relation Name']");
                                aliasDic.put(rc.read(Path + "['Alias']"), tableName);
                                int outPutCount = rc.read(Path + "['Actual Rows']");
                                int removedCount = rc.read(Path + "['Rows Removed by Filter']");
                                int rowCount = outPutCount + removedCount;
                                ExecutionNode currentRightNode = new ExecutionNode(rightPath, ExecutionNode.ExecutionNodeType.scan, rowCount, "table:" + tableName);
                                currentRightNode.setTableName(tableName);
                                currentRightNode.isAdd = true;
                                node.rightNode = currentRightNode;
                                stack.push(new AbstractMap.SimpleEntry<>(rightPath, currentRightNode));
                            }
                        }
                        ExecutionNode currentNode = getExecutionNode(queryPlan, currentPath);
                        node.leftNode = currentNode;
                        stack.push(new AbstractMap.SimpleEntry<>(currentPath, currentNode));
                    }
                }
            }
        }
        Map.Entry<String, ExecutionNode> rootPair = rootStack.pop();
        return rootPair.getValue();
    }

    private ExecutionNode getExecutionNode(String queryPlan, String Path) throws TouchstoneException {
        PgNodeTypeInfo NodeTypeRef = new PgNodeTypeInfo();
        ReadContext rc = JsonPath.parse(queryPlan);
        String NodeTypePath = Path + "['Node Type']";
        String NodeType = rc.read(NodeTypePath);
        String planId = Path;
        int rowCount = rc.read(Path + "['Actual Rows']");
        ExecutionNode node;
        if (NodeTypeRef.isFilterNode(NodeType)) {
            Map<String, String> allKeys = getKeys(queryPlan, Path);
            if (hasFilterInfo(allKeys)) {
                String filterInfo = rc.read(Path + "['Filter']");
                if(filterInfo.equals("(NOT (hashed SubPlan 1))")){
                    String leftNodePath = Path + "['Plans'][0]";
                    List<String> leftNodeResult = rc.read(leftNodePath + "['Output']");
                    List<String> outPut = rc.read(Path + "['Output']");
                    String antiJoinKey1 = "";
                    String antiJoinKey2 = "";
                    String antiJoinTable1 = "";
                    String antiJoinTable2 = "";
                    String joinInfo = "";
                    for (String s : leftNodeResult) {
                        antiJoinKey1 = s.split("\\.")[1];
                        antiJoinTable1 = s.split("\\.")[0];
                        String joinColumn1 =antiJoinKey1.split("_")[1];
                        for (String value : outPut) {
                            antiJoinKey2 = value.split("\\.")[1];
                            antiJoinTable2 = value.split("\\.")[0];
                            String joinColumn2 = antiJoinKey2.split("_")[1];
                            if (joinColumn1.equals(joinColumn2)) {
                                joinInfo = antiJoinTable2+"." + antiJoinKey2 + " = " + antiJoinTable1+"."+antiJoinKey1;
                            }
                        }
                    }
                    joinInfo = "Hash Cond: " + "(" + joinInfo + ")";
                    node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.antiJoin, rowCount, joinInfo);
                    return node;
                }else {
                    String tableName = rc.read(Path + "['Schema']") + "." + rc.read(Path + "['Relation Name']");
                    aliasDic.put(rc.read(Path + "['Alias']"), tableName);
                    node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.filter, rowCount, transFilterInfo(filterInfo));
                    node.setTableName(tableName);
                    return node;
                }
            } else {
                String tableName = rc.read(Path + "['Schema']") + "." + rc.read(Path + "['Relation Name']");
                aliasDic.put(rc.read(Path + "['Alias']"), tableName);
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.scan, rowCount, "table:" + tableName);
                node.setTableName(tableName);
                return node;
            }
        } else if (NodeTypeRef.isJoinNode(NodeType)) {
            Map<String, String> allKeys = getKeys(queryPlan, Path);
            if (NodeType.equals("Hash Join")) {
                String joinInfo = rc.read(Path + "['Hash Cond']");
                joinInfo = "Hash Cond: " + joinInfo;
                if (hasJoinFilter(allKeys)) {
                    String joinFilter = rc.read(Path + "['Join Filter']");
                    joinInfo = joinInfo + " " + "Join Filter: " + joinFilter;
                }
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.join, rowCount, joinInfo);
                return node;
            } else if (NodeType.equals("Nested Loop")) {
                String joinInfo = rc.read(Path + "['Plans'][1]['Index Cond']");
                joinInfo = "Index Cond: " + joinInfo;
                node = new ExecutionNode(planId, ExecutionNode.ExecutionNodeType.join, rowCount, joinInfo);
                return node;
            }
        }
        return null;
    }

    private Map<String, String> getKeys(String queryPlan, String Path) {
        ReadContext rc = JsonPath.parse(queryPlan);
        Map<String, String> allKeys = rc.read(Path);
        return allKeys;
    }

    private boolean hasChildPlan(Map<String, String> allKeys) {
        for (String key : allKeys.keySet()) {
            if (key.equals("Plans")) {
                return true;
            }
        }
        return false;
    }

    private boolean hasFilterInfo(Map<String, String> allKeys) {
        for (String key : allKeys.keySet()) {
            if (key.equals("Filter")) {
                return true;
            }
        }
        return false;
    }

    private boolean hasJoinFilter(Map<String, String> allKeys) {
        for (String key : allKeys.keySet()) {
            if (key.equals("Join Filter")) {
                return true;
            }
        }
        return false;
    }

    public String transFilterInfo(String filterInfo) {
        Matcher m = CanonicalColumnName.matcher(filterInfo);
        StringBuilder filter = new StringBuilder();
        while (m.find()) {
            String[] tableNameAndColName = m.group().split("\\.");
            m.appendReplacement(filter, aliasDic.get(tableNameAndColName[0]) + "." + tableNameAndColName[1]);
        }
        m.appendTail(filter);
        return removeRedundancy(filter.toString());
    }

    public String transJoinInfo(String joinInfo) {
        Matcher m = CanonicalColumnName.matcher(joinInfo);
        StringBuilder join = new StringBuilder();
        while (m.find()) {
            String[] tableNameAndColName = m.group().split("\\.");
            m.appendReplacement(join, aliasDic.get(tableNameAndColName[0]) + "." + tableNameAndColName[1]);
        }
        m.appendTail(join);
        return join.toString();
    }

    public String removeRedundancy(String filterInfo){
        Matcher m = REDUNDANCY.matcher(filterInfo);
        StringBuilder filter = new StringBuilder();
        while (m.find()) {
            String[] DateSym = m.group().split("::");
            String Date = DateSym[0];
            int length = Date.length();
            String DateTrans = Date.substring(1,length-1);
            m.appendReplacement(filter, DateTrans);
        }
        m.appendTail(filter);
        return filter.toString();
    }

    @Override
    public String[] analyzeJoinInfo(String joinInfo) throws TouchstoneException {
        if (joinInfo.contains("other cond:")) {
            throw new TouchstoneException("join中包含其他条件,暂不支持");
        }
        joinInfo = transJoinInfo(joinInfo);
        String[] result = new String[4];
        String leftTable, leftCol, rightTable, rightCol;
        Matcher eqCondition = JOIN_EQ_OPERATOR.matcher(joinInfo);
        if (eqCondition.find()) {
            if (eqCondition.groupCount() > 1) {
                throw new UnsupportedOperationException();
            }
            List<List<String>> matches = matchPattern(EQ_OPERATOR, joinInfo);
            String[] leftJoinInfos = matches.get(0).get(1).split("\\."), rightJoinInfos = matches.get(0).get(2).split("\\.");
            leftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]);
            rightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]);
            List<String> leftCols = new ArrayList<>(), rightCols = new ArrayList<>();
            for (List<String> match : matches) {
                leftJoinInfos = match.get(1).split("\\.");
                rightJoinInfos = match.get(2).split("\\.");
                String currLeftTable = String.format("%s.%s", leftJoinInfos[0], leftJoinInfos[1]),
                        currLeftCol = leftJoinInfos[2],
                        currRightTable = String.format("%s.%s", rightJoinInfos[0], rightJoinInfos[1]),
                        currRightCol = rightJoinInfos[2];
                if (!leftTable.equals(currLeftTable) || !rightTable.equals(currRightTable)) {
                    throw new TouchstoneException("join中包含多个表的约束,暂不支持");
                }
                leftCols.add(currLeftCol);
                rightCols.add(currRightCol);
            }
            leftCol = String.join(",", leftCols);
            rightCol = String.join(",", rightCols);
            result[0] = leftTable;
            result[1] = leftCol;
            result[2] = rightTable;
            result[3] = rightCol;
        }
        return result;
    }


    @Override
    public LogicNode PgAnalyzeSelectOperator(String operatorInfo) throws Exception {
        return parser.parseSelectOperatorInfo(operatorInfo);
    }

    @Override
    public AndNode analyzeSelectOperator(String operatorInfo) throws Exception {
        return null;
    }
}
