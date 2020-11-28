package ecnu.db.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeDeserializer;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.chain.ConstraintChainNodeDeserializer;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprNodeDeserializer;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;

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
    public static final int stepSize = 10000;
    public static String DEFAULT_DATABASE;
    public static final int INIT_HASHMAP_SIZE = 16;
    public static final MathContext BIG_DECIMAL_DEFAULT_PRECISION = new MathContext(10);
    public static final String DUMP_FILE_POSTFIX = "dump";
    private static final Pattern CANONICAL_TBL_NAME = Pattern.compile("[a-zA-Z0-9_$]+\\.[a-zA-Z0-9_$]+");
    public static final String SQL_FILE_POSTFIX = ".sql";
    private static String resultDir = "result/";

    public static String getResultDir() {
        return resultDir;
    }

    public static void setResultDir(String resultDir) {
        CommonUtils.resultDir = resultDir;
    }

    public static final double skipNodeThreshold = 0.01;
    public static String PERSIST_PATH;

    public static final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .registerModule(new SimpleModule()
                    .addDeserializer(AbstractColumn.class, new ColumnDeserializer())
                    .addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer())
                    .addDeserializer(ConstraintChainNode.class, new ConstraintChainNodeDeserializer())
                    .addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer()));


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
     * todo
     * 单个数据库时把表转换为<database>.<table>的形式
     *
     * @param tableName 表名
     * @return 转换后的表名
     */
    public static String addDatabaseNamePrefix(String tableName) {
        List<List<String>> matches = matchPattern(CANONICAL_TBL_NAME, tableName);
        return matches.size() == 1 && matches.get(0).get(0).length() == tableName.length() ?
                tableName : String.format("%s.%s", DEFAULT_DATABASE, tableName);
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

    public static double min(int[] ret) {
        double min = ret[0];
        for (int value : ret) {
            if (min >= value) {
                min = value;
            }
        }
        return min;
    }
}
