package ecnu.db.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
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
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.math.MathContext;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    public final static int stepSize = 100000;
    public final static MathContext BIG_DECIMAL_DEFAULT_PRECISION = new MathContext(10);
    public final static String DUMP_FILE_POSTFIX = "dump";
    public final static String SQL_FILE_POSTFIX = ".sql";
    public final static String QUERY_DIR = "/queries/";
    public final static String CONSTRAINT_CHAINS_INFO = "/constraintChain.json";
    public final static String SCHEMA_MANAGE_INFO = "/schema.json";
    public final static String COLUMN_MANAGE_INFO = "/distribution.json";
    public final static String CANONICAL_NAME_CONTACT_SYMBOL = ".";
    public final static String CANONICAL_NAME_SPLIT_REGEX = "\\.";
    public final static int SampleDoublePrecision = (int) 1E6;
    public final static int SINGLE_THREAD_TUPLE_SIZE = 100;
    public final static int INIT_HASHMAP_SIZE = 16;
    private static final DateTimeFormatter FMT = new DateTimeFormatterBuilder()
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .appendOptional(new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd")
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0).toFormatter())
            .appendOptional(DateTimeFormatter.ISO_LOCAL_DATE)
            .toFormatter();

    private static final SimpleModule touchStoneJsonModule = new SimpleModule()
            .addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer())
            .addDeserializer(ConstraintChainNode.class, new ConstraintChainNodeDeserializer())
            .addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer());

    private static final DefaultPrettyPrinter dpf = new DefaultPrettyPrinter();
    public static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .setDefaultPrettyPrinter(dpf)
            .registerModule(new JavaTimeModule()).registerModule(touchStoneJsonModule);

    static {
        dpf.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
    }

    public static long getUnixTimeStamp(String timeValue) {
        return LocalDateTime.parse(timeValue, CommonUtils.FMT).toEpochSecond(ZoneOffset.UTC) * 1000;
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
                new File(resultDir + CONSTRAINT_CHAINS_INFO), UTF_8),
                new TypeReference<Map<String, List<ConstraintChain>>>() {
                });
    }
}
