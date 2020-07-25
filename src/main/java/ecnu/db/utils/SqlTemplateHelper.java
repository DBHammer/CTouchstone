package ecnu.db.utils;

import ecnu.db.analyzer.online.AbstractAnalyzer;

import java.util.*;
import java.util.stream.Collectors;

import static ecnu.db.utils.CommonUtils.isEndOfConditionExpr;

/**
 * @author alan
 */
public class SqlTemplateHelper {

    /**
     * 模板化SQL语句
     *
     * @param sql            需要处理的SQL语句
     * @param queryAnalyzer  query分析器
     * @param cannotFindArgs 找不到的arguments
     * @param conflictArgs   矛盾的arguments
     * @return 模板化的SQL语句
     * @throws TouchstoneToolChainException 检测不到停止的语法词
     */
    public static String templatizeSql(String sql, AbstractAnalyzer queryAnalyzer, List<String> cannotFindArgs, List<String> conflictArgs) throws TouchstoneToolChainException {
        sql = templatizeSqlIter(sql, queryAnalyzer.getArgsAndIndex(), cannotFindArgs, conflictArgs);
        List<String> reProductArgs = new ArrayList<>();

        for (String cannotFindArg : cannotFindArgs) {
            if (cannotFindArg.contains(" bet")) {
                String[] indexInfos = queryAnalyzer.getArgsAndIndex().
                        get(cannotFindArg).get(0).split(" ");
                indexInfos[1] = indexInfos[1].replace("'", "");
                indexInfos[3] = indexInfos[3].replace("'", "");
                HashMap<String, List<String>> tempInfo = new HashMap<>(CommonUtils.INIT_HASHMAP_SIZE);
                tempInfo.put(cannotFindArg.split(" ")[0] + " >=", Collections.singletonList(indexInfos[1]));
                ArrayList<String> tempList = new ArrayList<>();
                sql = templatizeSqlIter(sql, tempInfo, tempList, new ArrayList<>());
                if (tempList.size() != 0) {
                    tempInfo.clear();
                    tempList.clear();
                    tempInfo.put(cannotFindArg.split(" ")[0] + " >", Collections.singletonList(indexInfos[1]));
                    sql = templatizeSqlIter(sql, tempInfo, tempList, new ArrayList<>());
                }
                tempInfo.clear();
                tempList.clear();
                tempInfo.put(cannotFindArg.split(" ")[0] + " <=", Collections.singletonList(indexInfos[3]));
                sql = templatizeSqlIter(sql, tempInfo, tempList, new ArrayList<>());
                if (tempList.size() != 0) {
                    tempInfo.clear();
                    tempList.clear();
                    tempInfo.put(cannotFindArg.split(" ")[0] + " <", Collections.singletonList(indexInfos[3]));
                    sql = templatizeSqlIter(sql, tempInfo, tempList, new ArrayList<>());
                }
                reProductArgs.add(cannotFindArg);
            }
        }
        cannotFindArgs.removeAll(reProductArgs);

        return sql;
    }

    /**
     * 模板化SQL语句的内部循环函数
     *
     * @param sql            需要处理的SQL语句
     * @param argsAndIndex   需要替换的arguments
     * @param cannotFindArgs 找不到的arguments
     * @param conflictArgs   矛盾的arguments
     * @return 模板化的SQL语句
     * @throws TouchstoneToolChainException 检测不到停止的语法词
     */
    public static String templatizeSqlIter(String sql, Map<String, List<String>> argsAndIndex, List<String> cannotFindArgs,
                                           List<String> conflictArgs) throws TouchstoneToolChainException {
        for (Map.Entry<String, List<String>> argAndIndexes : argsAndIndex.entrySet()) {
            int lastIndex = 0;
            int count = 0;
            if (argAndIndexes.getKey().contains("isnull")) {
                continue;
            }
            if (argAndIndexes.getKey().contains("in(")) {
                while (lastIndex != -1) {
                    lastIndex = sql.indexOf("in (", lastIndex);
                    if (lastIndex != -1) {
                        count++;
                        lastIndex += argAndIndexes.getKey().length();
                    }
                }
                if (count > 1) {
                    conflictArgs.add(argAndIndexes.getKey());
                } else {
                    String backString = sql.substring(sql.indexOf("in (") + "in (".length());
                    backString = backString.substring(backString.indexOf(")") + 1);
                    sql = sql.substring(0, sql.indexOf("in (") + "in ".length()) + argAndIndexes.getValue().get(0) + backString;
                }
            } else {
                while (lastIndex != -1) {
                    lastIndex = sql.indexOf(argAndIndexes.getKey(), lastIndex);
                    if (lastIndex != -1) {
                        count++;
                        lastIndex += argAndIndexes.getKey().length();
                    }
                }
                if (count == 0) {
                    cannotFindArgs.add(argAndIndexes.getKey());
                } else if (count == 1) {
                    int front = sql.indexOf(argAndIndexes.getKey()) + argAndIndexes.getKey().length();
                    StringBuilder backString = new StringBuilder(sql.substring(front + 1));
                    String[] sqlTuples = backString.toString().split(" ");
                    int i = 0;
                    boolean hasBetween = false;
                    if (argAndIndexes.getKey().contains(" bet")) {
                        hasBetween = true;
                    }
                    for (; i < sqlTuples.length; i++) {
                        if (!hasBetween) {
                            if (isEndOfConditionExpr(sqlTuples[i].toLowerCase()) || sqlTuples[i].contains(";")) {
                                break;
                            }
                        } else {
                            if ("and".equals(sqlTuples[i])) {
                                hasBetween = false;
                            }
                        }
                    }
                    if (i < sqlTuples.length) {
                        backString = new StringBuilder();
                        if (sqlTuples[i].contains(";")) {
                            backString.append(";");
                        } else {
                            for (; i < sqlTuples.length; i++) {
                                backString.append(" ").append(sqlTuples[i]);
                            }
                        }
                        if (argAndIndexes.getKey().contains(" bet")) {
                            sql = sql.substring(0, front) + argAndIndexes.getValue().get(0) + backString.toString();
                        } else {
                            sql = String.format("%s'%s'%s", sql.substring(0, front + 1), argAndIndexes.getValue().get(0), backString.toString());
                        }

                    } else {
                        throw new TouchstoneToolChainException("检测不到停止的语法词");
                    }
                } else if (count > 1) {
                    conflictArgs.add(argAndIndexes.getKey());
                }
            }
        }
        return sql;
    }

    /**
     * 为模板化后的SQL语句添加conflictArgs和cannotFindArgs参数
     * @param argsMap 参数的map,
     * @param title 标题
     * @param args 需要添加的
     * @return 添加的参数部分
     */
    public static String appendArgs(Map<String, List<String>> argsMap, String title, List<String> args) {
        String argStr;
        argStr = String.format("-- %s:%s%s",
                title,
                args
                .stream()
                .map((arg) -> String.format("%s:%s", arg, argsMap.get(arg)))
                .collect(Collectors.joining(",")),
                System.lineSeparator());
        return argStr;
    }
}
