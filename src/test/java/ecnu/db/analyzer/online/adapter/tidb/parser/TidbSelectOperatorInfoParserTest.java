package ecnu.db.analyzer.online.adapter.tidb.parser;

import ecnu.db.generator.constraintchain.filter.logical.AndNode;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnType;
import ecnu.db.utils.exception.TouchstoneException;
import ecnu.db.schema.ColumnManager;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TidbSelectOperatorInfoParserTest {
    private final TidbSelectOperatorInfoLexer lexer = new TidbSelectOperatorInfoLexer(new StringReader(""));
    private TidbSelectOperatorInfoParser parser;

    @BeforeEach
    void setUp() throws TouchstoneException {
        parser = new TidbSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());
        ColumnManager.getInstance().addColumn("db.table.col1", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.table.col2", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.table.col3", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.table.col4", new Column(ColumnType.INTEGER));
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method")
    @Test
    void testParse() throws Exception {
        String testCase = "ge(db.table.col1, 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(ge(db.table.col1, {id:0, data:2}))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with arithmetic ops")
    @Test
    void testParseWithArithmeticOps() throws Exception {
        String testCase = "ge(mul(db.table.col1, plus(db.table.col2, 3)), 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(ge(MUL(db.table.col1, PLUS(db.table.col2, 3)), {id:0, data:2}))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with logical ops")
    @Test
    void testParseWithLogicalOps() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), lt(db.table.col4, 3.0))";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), lt(db.table.col4, {id:1, data:3.0})))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with erroneous grammar")
    @Test()
    void testParseWithLogicalOpsFailed() {
        assertThrows(Exception.class, () -> {
            String testCase = "or(ge(db.table.col1, 2), mul(db.table.col2, 3))";
            parser.parseSelectOperatorInfo(testCase);
        });
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with not")
    @Test()
    void testParseWithNot() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), not(in(db.table.col3, \"3\", \"2\")))";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), not_in(db.table.col3, {id:1, data:3}, {id:2, data:2})))", node.toString());
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method with isnull")
    @Test()
    void testParseWithIsnull() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), not(isnull(db.table.col2)))";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), not_isnull(db.table.col2)))", node.toString());
    }
}