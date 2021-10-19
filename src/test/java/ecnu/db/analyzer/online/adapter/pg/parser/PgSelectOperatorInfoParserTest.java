package ecnu.db.analyzer.online.adapter.pg.parser;

import ecnu.db.generator.constraintchain.filter.logical.AndNode;
import ecnu.db.generator.constraintchain.filter.logical.LogicNode;
import ecnu.db.utils.exception.TouchstoneException;
import java_cup.runtime.ComplexSymbolFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PgSelectOperatorInfoParserTest {
    private final PgSelectOperatorInfoLexer lexer = new PgSelectOperatorInfoLexer(new StringReader(""));
    private PgSelectOperatorInfoParser parser;

    @BeforeEach
    void setUp() {
        parser = new PgSelectOperatorInfoParser(lexer, new ComplexSymbolFactory());
    }

    @ParameterizedTest
    @CsvSource(delimiter = ';', value ={
            "(db1.table.col1 >= 2);" +
                    "and(ge(db1.table.col1, {id:0, data:2}))",
            "((db1.table.col1 * (db1.table.col2 + 3.0)) >= 2);" +
                    "and(ge(MUL(db1.table.col1, PLUS(db1.table.col2, 3.0)), {id:0, data:2}))",
            "((db1.table.col1 >= 2) or (db1.table.col4 < 3.0)); " +
                    "and(or(ge(db1.table.col1, {id:0, data:2}), lt(db1.table.col4, {id:1, data:3.0})))",
            "((db1.table.col3) ~~ 'STRING');" +
                    "and(like(db1.table.col3, {id:0, data:STRING}))",
            "(db1.table.col3 IS NULL);" +
                    "and(isnull(db1.table.col3))",
            "(db1.table.col3 = ANY ('{\"dasd\", dasd}'));" +
                    " and(in(db1.table.col3, {id:0, data:dasd}, {id:1, data:dasd}))",
            "(public.part.p_size = ANY ('{42,33,35,6,46,24,15,21}'::integer[]));" +
                    " and(in(public.part.p_size, {id:0, data:42}, {id:1, data:33}, {id:2, data:35}, {id:3, data:6}, {id:4, data:46}, {id:5, data:24}, {id:6, data:15}, {id:7, data:21}))",
            "((public.part.p_type)::text !~~ 'STANDARD ANODIZED%'::text);" +
                    "and(not_like(public.part.p_type, {id:0, data:STANDARD ANODIZED%}))",
            "((public.part.p_type)::text = 'STANDARD BURNISHED NICKEL'::text);" +
                    "and(eq(public.part.p_type, {id:0, data:STANDARD BURNISHED NICKEL}))"
    })
    void testPgParse(String input, String output) throws Exception {
        LogicNode node = parser.parseSelectOperatorInfo(input);
        assertEquals(output, node.toString().replaceAll(System.lineSeparator()," "));
    }
}
