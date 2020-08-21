package ecnu.db.utils;

import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    public static final int INIT_HASHMAP_SIZE = 16;
    public static final MathContext BIG_DECIMAL_DEFAULT_PRECISION = new MathContext(10);
    public static final String DUMP_FILE_POSTFIX = "dump";
    private static final Pattern CANONICAL_TBL_NAME = Pattern.compile("[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+");

    /**
     * 获取正则表达式的匹配
     *
     * @param pattern 正则表达式
     * @param str     传入的字符串
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
     * 单个数据库时把表转换为<database>.<table>的形式
     *
     * @param databaseName 未跨数据库情况下数据库名称
     * @param name         表名
     * @return 转换后的表名
     */
    public static String addDatabaseNamePrefix(String databaseName, String name) {
        if (!isCanonicalTableName(name)) {
            name = String.format("%s.%s", databaseName, name);
        }
        return name;
    }

    /**
     * 是否为<database>.<table>的形式的表名
     *
     * @param tableName 表名
     * @return true or false
     */
    public static boolean isCanonicalTableName(String tableName) {
        List<List<String>> matches = matchPattern(CANONICAL_TBL_NAME, tableName);
        return matches.size() == 1 && matches.get(0).get(0).length() == tableName.length();
    }

    public static boolean isInteger(String str) {
        try {
            int val = Integer.parseInt(str);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static boolean isFloat(String str) {
        try {
            float val = Float.parseFloat(str);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }

    public static String extractSimpleColumnName(String canonicalColumnName) {
        return canonicalColumnName.split("\\.")[2];
    }

    public static void shuffle(int size, ThreadLocalRandom rand, int[] tupleData) {
        int tmp;
        for (int i = size; i > 1; i--) {
            int idx = rand.nextInt(i);
            tmp = tupleData[i - 1];
            tupleData[i - 1] = tupleData[idx];
            tupleData[idx] = tmp;
        }
    }

    public static void shuffle(int size, ThreadLocalRandom rand, double[] tupleData) {
        double tmp;
        for (int i = size; i > 1; i--) {
            int idx = rand.nextInt(i);
            tmp = tupleData[i - 1];
            tupleData[i - 1] = tupleData[idx];
            tupleData[idx] = tmp;
        }
    }

    public static void shuffle(int size, ThreadLocalRandom rand, long[] tupleData) {
        long tmp;
        for (int i = size; i > 1; i--) {
            int idx = rand.nextInt(i);
            tmp = tupleData[i - 1];
            tupleData[i - 1] = tupleData[idx];
            tupleData[idx] = tmp;
        }
    }

    public static double calcuate(double[] ret) {
        double sum = 0;
        for (int i = 0; i < ret.length; i++) {
            sum += ret[i];
        }
        double ratio = sum * 1.0 / ret.length;
        return ratio;
    }

    public static double calcuate(int[] ret) {
        double sum = 0;
        for (int i = 0; i < ret.length; i++) {
            sum += ret[i];
        }
        double ratio = sum * 1.0 / ret.length;
        return ratio;
    }

    public static double calcuate(boolean[] ret) {
        double sum = 0;
        for (int i = 0; i < ret.length; i++) {
            sum += (ret[i] ? 1 : 0);
        }
        double ratio = sum * 1.0 / ret.length;
        return ratio;
    }

    public static double min(int[] ret) {
        double min = ret[0];
        for (int i = 0; i < ret.length; i++) {
            if (min >= ret[i]) {
                min = ret[i];
            }
        }
        return min;
    }
}
