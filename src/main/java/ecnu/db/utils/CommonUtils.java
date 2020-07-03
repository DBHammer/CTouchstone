package ecnu.db.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    private static final HashSet<String> WHERE_DELIMITERS = new HashSet<>(Arrays.asList("and", "limit", "group", ")", "or", "order", "||", "&&"));

    /**
     * 获取正则表达式的匹配
     *
     * @param pattern
     * @param str
     * @return 成功的所有匹配，一个{@code List<String>}对应一个匹配的所有group
     */
    public static List<List<String>> matchPattern(Pattern pattern, String str) {
        Matcher matcher = pattern.matcher(str);
        List<List<String>> ret = new ArrayList<>();
        while (matcher.find()) {
            List<String> groups = new ArrayList<>();
            for (int i = 0; i <= matcher.groupCount(); i++) {
                groups.add(matcher.group(i));
            }
            ret.add(groups);
        }

        return ret;
    }

    /**
     * 检查是否是SQL语句里where_condition的边界token
     *
     * @param token 需要检查的token
     * @return
     */
    public static boolean isEndOfConditionExpr(String token) {
        return WHERE_DELIMITERS.contains(token);
    }
}
