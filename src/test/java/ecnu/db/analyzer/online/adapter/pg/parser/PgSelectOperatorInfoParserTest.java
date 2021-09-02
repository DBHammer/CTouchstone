package ecnu.db.analyzer.online.adapter.pg.parser;

import ecnu.db.generator.constraintchain.filter.SelectResult;
import ecnu.db.generator.constraintchain.filter.logical.AndNode;
import ecnu.db.schema.Column;
import ecnu.db.schema.ColumnManager;
import ecnu.db.schema.ColumnType;
import ecnu.db.utils.exception.TouchstoneException;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.StringReader;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PgSelectOperatorInfoParserTest {
    private final PgSelectOperatorInfoLexer lexer = new PgSelectOperatorInfoLexer(new StringReader(""));
    private PgSelectOperatorInfoParser parser;

    @BeforeEach
    void setUp() throws TouchstoneException {
        parser = new PgSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());
        ColumnManager.getInstance().addColumn("db.table.col1", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.table.col2", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.table.col3", new Column(ColumnType.INTEGER));
        ColumnManager.getInstance().addColumn("db.table.col4", new Column(ColumnType.INTEGER));
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method")
    @Test
    void testParse() throws Exception {
        String testCase = "(db.table.col1 >= 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(ge(db.table.col1, {id:0, data:2}))", node.toString());
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method with arithmetic ops")
    @Test
    void testParseWithArithmeticOps() throws Exception {
        String testCase = "((db.table.col1 * (db.table.col2 + 3.0)) >= 2)";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(ge(MUL(db.table.col1, PLUS(db.table.col2, 3.0)), {id:0, data:2}))", node.toString());
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method with logical ops")
    @Test
    void testParseWithLogicalOps() throws Exception {
        String testCase = "((db.table.col1 >= 2) or (db.table.col4 < 3.0))";
        AndNode node = parser.parseSelectOperatorInfo(testCase).getCondition();
        assertEquals("and(or(ge(db.table.col1, {id:0, data:2}), lt(db.table.col4, {id:1, data:3.0})))", node.toString());
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method with erroneous grammar")
    @Test()
    void testParseWithLogicalOpsFailed() {
        assertThrows(Exception.class, () -> {
            String testCase = "((db.table.col1 >= 2) or (db.table.col2 * 3))";
            parser.parseSelectOperatorInfo(testCase);
        });
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method with not")
    @Test()
    void testParseWithNot() throws Exception {
        String testCase = "((db.table.col3) ~~ 'STRING')";
        SelectResult result = parser.parseSelectOperatorInfo(testCase);
        AndNode node = result.getCondition();
        assertEquals("and(like(db.table.col3, {id:0, data:STRING}))", node.toString());
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method is null")
    @Test()
    void testParseIsNull() throws Exception {
        String testCase = "(db.table.col3 IS NULL)";
        SelectResult result = parser.parseSelectOperatorInfo(testCase);
        AndNode node = result.getCondition();
        assertEquals("and(isnull(db.table.col3))", node.toString());
    }

    @DisplayName("test PgSelectOperatorInfoParser.parse method in")
    @Test()
    void testParseIN() throws Exception {
        String testCase = "((db.table.col3) = ANY ('{\"dasd\",dasd}'))";
        SelectResult result = parser.parseSelectOperatorInfo(testCase);
        AndNode node = result.getCondition();
        assertEquals("and(in(db.table.col3, {id:0, data:dasd}, {id:1, data:dasd}))", node.toString());
    }

}
