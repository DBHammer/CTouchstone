package ecnu.db.analyzer.online.select.tidb;

import ecnu.db.analyzer.online.TidbAnalyzer;
import ecnu.db.constraintchain.filter.logical.AndNode;
import ecnu.db.utils.SystemConfig;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.*;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TidbSelectOperatorInfoParserTest {
    private TidbSelectOperatorInfoLexer lexer = new TidbSelectOperatorInfoLexer(new StringReader(""));
    private TidbSelectOperatorInfoParser parser = new TidbSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());

    @BeforeEach
    void setUp() {
        SystemConfig config = new SystemConfig();
        parser.setAnalyzer(new TidbAnalyzer(config, null, null, null));
    }

    @DisplayName("test TidbSelectOperatorInfoParser.parse method")
    @Test
    void testParse() throws Exception {
        String testCase = "ge(db.table.col, 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals( "and(ge(column(db.table.col), Parameter{id=0, data='2'}))", node.toString());
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with arithmetic ops")
    @Test
    void testParseWithArithmeticOps() throws Exception {
        String testCase = "ge(mul(db.table.col1, plus(db.table.col2, 3)), 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(ge(mul(column(db.table.col1), plus(column(db.table.col2), 3.0)), Parameter{id=0, data='2'}))", node.toString());
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with logical ops")
    @Test
    void testParseWithLogicalOps() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), lt(db.table.col2, 3.0))";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(or(ge(column(db.table.col1), Parameter{id=0, data='2'}), lt(column(db.table.col2), Parameter{id=1, data='3.0'})))", node.toString());
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
        String testCase = "or(ge(db.table.col1, 2), not(in(db.table.col2, \"3\", \"2\")))";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(or(ge(column(db.table.col1), Parameter{id=0, data='2'}), not_in(column(db.table.col2), Parameter{id=1, data='3'}, Parameter{id=2, data='2'})))", node.toString());
    }
    @DisplayName("test TidbSelectOperatorInfoParser.parse method with isnull")
    @Test()
    void testParseWithIsnull() throws Exception {
        String testCase = "or(ge(db.table.col1, 2), not(isnull(db.table.col2)))";
        AndNode node = parser.parseSelectOperatorInfo(testCase);
        assertEquals("and(or(ge(column(db.table.col1), Parameter{id=0, data='2'}), not_isnull(column(db.table.col2))))", node.toString());
    }
}