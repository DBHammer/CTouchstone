package ecnu.db.analyzer.online;

import ecnu.db.generator.constraintchain.filter.SelectResult;
import ecnu.db.utils.exception.analyze.UnsupportedSelect;

public interface FilterConstraintParser {
    SelectResult parseSelectOperatorInfo(String operatorInfo) throws UnsupportedSelect;
}
