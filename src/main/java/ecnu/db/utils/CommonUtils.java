package ecnu.db.utils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import ecnu.db.generator.constraintchain.chain.ConstraintChainNode;
import ecnu.db.generator.constraintchain.chain.ConstraintChainNodeDeserializer;
import ecnu.db.generator.constraintchain.filter.BoolExprNode;
import ecnu.db.generator.constraintchain.filter.BoolExprNodeDeserializer;
import ecnu.db.generator.constraintchain.filter.arithmetic.ArithmeticNode;
import ecnu.db.generator.constraintchain.filter.arithmetic.ArithmeticNodeDeserializer;

import java.io.*;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author xuechao.lian
 */
public class CommonUtils {
    public static final int STEP_SIZE = 100000;
    public static final MathContext BIG_DECIMAL_DEFAULT_PRECISION = new MathContext(10);
    public static final String CANONICAL_NAME_CONTACT_SYMBOL = ".";
    public static final String CANONICAL_NAME_SPLIT_REGEX = "\\.";
    public static final int SAMPLE_DOUBLE_PRECISION = (int) 1E6;
    public static final int SINGLE_THREAD_TUPLE_SIZE = 100;
    public static final int INIT_HASHMAP_SIZE = 16;


    private static final SimpleModule touchStoneJsonModule = new SimpleModule()
            .addDeserializer(ArithmeticNode.class, new ArithmeticNodeDeserializer())
            .addDeserializer(ConstraintChainNode.class, new ConstraintChainNodeDeserializer())
            .addDeserializer(BoolExprNode.class, new BoolExprNodeDeserializer());

    private static final DefaultPrettyPrinter dpf = new DefaultPrettyPrinter();

    static {
        dpf.indentArraysWith(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE);
    }

    public static final CsvMapper CSV_MAPPER = new CsvMapper();

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
            .setDefaultPrettyPrinter(dpf)
            .registerModule(new JavaTimeModule()).registerModule(touchStoneJsonModule);

    public static boolean isNotCanonicalColumnName(String columnName) {
        return columnName.split(CANONICAL_NAME_SPLIT_REGEX).length != 3;
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

    public static String readFile(String path) throws IOException {
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(path))) {
            List<String> fileContent = new ArrayList<>();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                fileContent.add(line);
            }
            return fileContent.stream().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    public static void writeFile(String path, String content) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(path))) {
            bufferedWriter.write(content);
        }
    }

}
