package ecnu.db.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.arithmetic.ArithmeticNodeDeserializer;
import ecnu.db.generator.constraintchain.chain.ConstraintChain;
import ecnu.db.generator.constraintchain.chain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainNodeDeserializer;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprNodeDeserializer;
import ecnu.db.utils.exception.analyze.IllegalQueryTableNameException;

import java.io.*;
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

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    public static final int stepSize = 100000;
    public static final MathContext BIG_DECIMAL_DEFAULT_PRECISION = new MathContext(10);
    public static final String DUMP_FILE_POSTFIX = "dump";
    public static final String SQL_FILE_POSTFIX = ".sql";
    public static final String QUERY_DIR = "/queries/";
    public static final String CONSTRAINT_CHAINS_INFO = "/constraintChain.json";
    public static final String SCHEMA_MANAGE_INFO = "/schema.json";
    public static final String COLUMN_MANAGE_INFO = "/distribution.json";
    public static final String CANONICAL_NAME_CONTACT_SYMBOL = ".";
    public static final String CANONICAL_NAME_SPLIT_REGEX = "\\.";
    public static final int SampleDoublePrecision = (int) 1E6;
    public static final int SINGLE_THREAD_TUPLE_SIZE = 100;
    public static final int INIT_HASHMAP_SIZE = 16;
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
        BufferedReader reader = new BufferedReader(new FileReader(resultDir + CONSTRAINT_CHAINS_INFO));
        StringBuilder fileContent = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            fileContent.append(line);
        }
        reader.close();
        return CommonUtils.MAPPER.readValue(fileContent.toString(), new TypeReference<>() {
        });
    }

    public static String convertTableName2CanonicalTableName(String canonicalTableName,
                                                             Pattern canonicalTblName,
                                                             String defaultDatabase) throws IllegalQueryTableNameException {
        List<List<String>> matches = matchPattern(canonicalTblName, canonicalTableName);
        if (matches.size() == 1 && matches.get(0).get(0).length() == canonicalTableName.length()) {
            return canonicalTableName;
        } else {
            if (defaultDatabase == null) {
                throw new IllegalQueryTableNameException();
            }
            return String.format("%s.%s", defaultDatabase, canonicalTableName);
        }
    }

    public static String readFile(String path) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        StringBuilder fileContent = new StringBuilder();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            fileContent.append(line);
        }
        bufferedReader.close();
        return fileContent.toString();
    }

    public static void writeFile(String path, String content) throws IOException {
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path));
        bufferedWriter.write(content);
        bufferedWriter.close();
    }
}
