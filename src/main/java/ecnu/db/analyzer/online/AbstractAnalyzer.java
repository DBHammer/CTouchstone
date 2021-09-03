package ecnu.db.analyzer.online;

import ecnu.db.generator.constraintchain.filter.logical.AndNode;
import ecnu.db.utils.exception.TouchstoneException;

import java.util.List;

/**
 * @author wangqingshuai
 */
public abstract class AbstractAnalyzer {

    protected NodeTypeTool nodeTypeRef;

    /**
     * 查询树的解析
     *
     * @param queryPlan query解析出的查询计划，带具体的行数
     * @return 查询树Node信息
     * @throws TouchstoneException 查询树无法解析
     */
    public abstract ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneException;

    /**
     * 分析join信息
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneException 无法分析的join条件
     */
    public abstract String[] analyzeJoinInfo(String joinInfo) throws TouchstoneException;

    /**
     * 分析join信息
     *
     * @param operatorInfo 查询的条件
     * @return SelectResult 算数抽象语法树
     * @throws Exception 无法分析的Selection条件
     */
    public abstract AndNode analyzeSelectOperator(String operatorInfo) throws Exception;
}
