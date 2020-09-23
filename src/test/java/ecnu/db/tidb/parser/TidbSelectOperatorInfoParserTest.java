package ecnu.db.tidb.parser;

import ecnu.db.constraintchain.filter.SelectResult;
import ecnu.db.constraintchain.filter.logical.AndNode;
import ecnu.db.exception.UnsupportedDBTypeException;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.DecimalColumn;
import ecnu.db.schema.column.IntColumn;
import ecnu.db.schema.column.StringColumn;
import ecnu.db.tidb.TidbAnalyzer;
import ecnu.db.tidb.TidbInfo;
import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneSupportedDatabaseVersion;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TidbSelectOperatorInfoParserTest {
    private final TidbSelectOperatorInfoLexer lexer = new TidbSelectOperatorInfoLexer(new StringReader(""));
    private final TidbSelectOperatorInfoParser parser = new TidbSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());

    @BeforeEach
    void setUp() throws UnsupportedDBTypeException, IOException {
        SystemConfig config = new SystemConfig();
        config.setDatabaseVersion(TouchstoneSupportedDatabaseVersion.TiDB4);
        Map<String, Schema> schemas = new HashMap<>();
        Schema schema = new Schema();
        Map<String, AbstractColumn> columns = new HashMap<>();
        columns.put("col1", new IntColumn("col1"));
        columns.put("col2", new IntColumn("col2"));
        columns.put("col3", new StringColumn("col3"));
        columns.put("col4", new DecimalColumn("col4"));
        schema.setColumns(columns);
        schemas.put("db.table", schema);
        parser.setAnalyzer(new TidbAnalyzer(config, null, new TidbInfo(TouchstoneSupportedDatabaseVersion.TiDB4), schemas, null));
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method")
    @Test
    void testParse() throws Exception {
        String testCase = "ge(db.table.col1, 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(ge(db.table.col1, {id:0, data:2}))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with arithmetic ops")
    @Test
    void testParseWithArithmeticOps() throws Exception {
        String testCase = "ge(mul(db.table.col1, plus(db.table.col2, 3)), 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(ge(mul(db.table.col1, plus(db.table.col2, 3.0)), {id:0, data:2}))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with logical ops")
    @Test
    void testParseWithLogicalOps() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), lt(db.table.col4, 3.0))";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), lt(db.table.col4, {id:1, data:3.0})))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with erroneous grammar")
    @Test()
    void testParseWithLogicalOpsFailed() {
        assertThrows(Exception.class, () -> {
            String testCase = "or(ge((db.table.col1), 2), mul(db.table.col2, 3))";
            parser.parseSelectOperatorInfo(testCase);
        });
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with not")
    @Test()
    void testParseWithNot() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), not(in(db.table.col3, \"3\", \"2\")))";
        SelectResult result = parser.parseSelectOperatorInfo(testCase);
        AndNode node = result.getCondition();
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), not(in(db.table.col3, {id:1, data:'3'}, {id:2, data:'2'}))))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with isnull")
    @Test()
    void testParseWithIsnull() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), not(isnull(db.table.col2)))";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), not(isnull(db.table.col2))))", node.toString());
    }
}