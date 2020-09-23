package ecnu.db.utils;

import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.Token;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import ecnu.db.constraintchain.filter.Parameter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author alan
 */
public class SqlTemplateHelper {
    private static final Logger logger = LoggerFactory.getLogger(SqlTemplateHelper.class);

    /**
     * 模板化SQL语句
     *
     * @param queryCanonicalName query标准名
     * @param query              需要处理的SQL语句
     * @param dbType             数据库类型
     * @param parameters         需要模板化的参数
     * @return 模板化的SQL语句
     */
    public static String templatizeSql(String queryCanonicalName, String query, String dbType, List<Parameter> parameters) {
        Lexer lexer = new Lexer(query, null, dbType);
        Multimap<String, Pair<Integer, Integer>> literalMap = ArrayListMultimap.create();
        int lastPos = 0, pos;
        while (!lexer.isEOF()) {
            lexer.nextToken();
            Token token = lexer.token();
            pos = lexer.pos();
            if (token == Token.LITERAL_INT || token == Token.LITERAL_FLOAT || token == Token.LITERAL_CHARS) {
                String str = query.substring(lastPos, pos).trim();
                literalMap.put(str, Pair.of(pos - str.length(), pos));
            }
            lastPos = pos;
        }

        // replacement
        List<Parameter> cannotFindArgs = new ArrayList<>(), conflictArgs = new ArrayList<>();
        TreeMap<Integer, Pair<Parameter, Pair<Integer, Integer>>> replaceParams = new TreeMap<>();
        for (Parameter parameter : parameters) {
            String data = parameter.getData();
            if (parameter.isNeedQuote()) {
                data = String.format("'%s'", data);
            }
            Collection<Pair<Integer, Integer>> matches = literalMap.get(data);
            if (matches.size() == 0) {
                cannotFindArgs.add(parameter);
            } else if (matches.size() > 1) {
                conflictArgs.add(parameter);
            } else {
                Pair<Integer, Integer> pair = matches.stream().findFirst().get();
                int startPos = pair.getLeft();
                replaceParams.put(startPos, Pair.of(parameter, pair));
            }
        }
        List<String> fragments = new ArrayList<>();
        int currentPos = 0;
        while (!replaceParams.isEmpty()) {
            Map.Entry<Integer, Pair<Parameter, Pair<Integer, Integer>>> entry = replaceParams.pollFirstEntry();
            Pair<Parameter, Pair<Integer, Integer>> pair = entry.getValue();
            Parameter parameter = pair.getKey();
            int startPos = pair.getValue().getLeft(), endPos = pair.getValue().getRight();
            fragments.add(query.substring(currentPos, startPos));
            fragments.add(String.format("'%s,%d'", parameter.getId(), parameter.getIsDate() ? 1 : 0));
            currentPos = endPos;
        }
        fragments.add(query.substring(currentPos));
        query = String.join("", fragments);
        if (cannotFindArgs.size() > 0) {
            logger.warn(String.format("请注意%s中有参数无法完成替换，请查看该sql输出，手动替换;", queryCanonicalName));
            query = SqlTemplateHelper.appendArgs("cannotFindArgs", cannotFindArgs) + query;
        }
        if (conflictArgs.size() > 0) {
            logger.warn(String.format("请注意%s中有参数出现多次，无法智能，替换请查看该sql输出，手动替换;", queryCanonicalName));
            query = SqlTemplateHelper.appendArgs("conflictArgs", conflictArgs) + query;
        }

        return query;
    }

    /**
     * 为模板化后的SQL语句添加conflictArgs和cannotFindArgs参数
     *
     * @param title  标题
     * @param params 需要添加的
     * @return 添加的参数部分
     */
    public static String appendArgs(String title, List<Parameter> params) {
        String argsString = params.stream().map(
                (parameter) ->
                        String.format("{id:%s,data:%s,operator:%s,operand:%s,isDate:%d}",
                                parameter.getId(),
                                parameter.isNeedQuote() ? "'" + parameter.getData() + "'" : parameter.getData(),
                                parameter.getOperator().toString().toLowerCase(),
                                parameter.getOperand(),
                                parameter.getIsDate() ? 1 : 0
                        ))
                .collect(Collectors.joining(","));
        return String.format("-- %s:%s%s", title, argsString, System.lineSeparator());
    }
}
