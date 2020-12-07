package ecnu.db.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ecnu.db.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.constraintchain.arithmetic.ArithmeticNodeDeserializer;
import ecnu.db.constraintchain.chain.ConstraintChain;
import ecnu.db.constraintchain.chain.ConstraintChainNode;
import ecnu.db.constraintchain.chain.ConstraintChainNodeDeserializer;
import ecnu.db.constraintchain.filter.BoolExprNode;
import ecnu.db.constraintchain.filter.BoolExprNodeDeserializer;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnDeserializer;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    public final static int stepSize = 10000;
    public final static MathContext BIG_DECIMAL_DEFAULT_PRECISION = new MathContext(10);
    public final static String DUMP_FILE_POSTFIX = "dump";
    public final static String SQL_FILE_POSTFIX = ".sql";
    public final static String QUERY_DIR = "/queries/";
    public final static String CONSTRAINT_CHAINS_INFO = "/constraintChain.json";
    public final static String SCHEMA_MANAGE_INFO = "/schema.json";
    public final static String COLUMN_MANAGE_INFO = "/distribution.json";
    public final static String CANONICAL_NAME_CONTACT_SYMBOL = ".";
    public final static String CANONICAL_NAME_SPLIT_REGEX = "\\.";
    public final static int SINGLE_THREAD_TUPLE_SIZE = 100;
    public final static int INIT_HASHMAP_SIZE = 16;

    public static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule()).registerModule(new SimpleModule()
            .addDeserializer(AbstractColumn.class, new ColumnDeserializer())
            .addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer())
            .addDeserializer(ConstraintChainNode.class, new ConstraintChainNodeDeserializer())
            .addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer()));

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

    public static Map<String, List<ConstraintChain>> loadConstrainChainResult(String resultDir) throws IOException {
        return CommonUtils.MAPPER.readValue(FileUtils.readFileToString(
                new File(resultDir + SCHEMA_MANAGE_INFO), UTF_8),
                new TypeReference<Map<String, List<ConstraintChain>>>() {
                });
    }
}
