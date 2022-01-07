package ecnu.db.analyzer.online.adapter.tidb.parser;

import ecnu.db.generator.constraintchain.filter.LogicNode;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TidbSelectOperatorInfoParserTest {
    private final TidbSelectOperatorInfoLexer lexer = new TidbSelectOperatorInfoLexer(new StringReader(""));
    private TidbSelectOperatorInfoParser parser;

    @BeforeEach
    void setUp() {
        parser = new TidbSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value = {
            "ge(db.table.col1, 2);" +
                    "and(ge(db.table.col1, {id:0, data:2}))",
            "ge(mul(db.table.col1, plus(db.table.col2, 3)), 2);" +
                    "and(ge(MUL(db.table.col1, PLUS(db.table.col2, 3)), {id:0, data:2}))",
            "or(ge(db.table.col1, 2), lt(db.table.col4, 3.0));" +
                    "and(or(ge(db.table.col1, {id:0, data:2}), lt(db.table.col4, {id:1, data:3.0})))",
            "or(ge(db.table.col1, 2), not(in(db.table.col3, \"3\", \"2\")));" +
                    "and(or(ge(db.table.col1, {id:0, data:2}), not_in(db.table.col3, {id:1, data:3}, {id:2, data:2})))",
            "or(ge(db.table.col1, 2), not(isnull(db.table.col2)));" +
                    "and(or(ge(db.table.col1, {id:0, data:2}), not_isnull(db.table.col2)))"
    })
    void testTidbParse(String input, String output) throws Exception {
        LogicNode node = parser.parseSelectOperatorInfo(input);
        assertEquals(output, node.toString().replaceAll(System.lineSeparator(), " "));
    }
}