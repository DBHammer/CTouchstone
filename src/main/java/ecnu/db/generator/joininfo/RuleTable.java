package ecnu.db.generator.joininfo;

import ecnu.db.generator.FkGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RuleTable {
    Map<JoinStatus, List<Map.Entry<Long, Long>>> rules = new HashMap<>();

    public void addRule(JoinStatus status, Map.Entry<Long, Long> range) {
        rules.computeIfAbsent(status, value -> new ArrayList<>());
        rules.get(status).add(range);
    }

    public MergedRuleTable mergeRules(List<Integer> location) {
        Map<JoinStatus, List<Map.Entry<Long, Long>>> mergedRules = new HashMap<>();
        for (Map.Entry<JoinStatus, List<Map.Entry<Long, Long>>> joinStatusListEntry : rules.entrySet()) {
            boolean[] joinStatus = joinStatusListEntry.getKey().status();
            JoinStatus pkStatus = FkGenerator.chooseCorrespondingStatus(joinStatus, location);
            mergedRules.computeIfAbsent(pkStatus, v -> new ArrayList<>());
            mergedRules.get(pkStatus).addAll(joinStatusListEntry.getValue());
        }
        return new MergedRuleTable(mergedRules);
    }


}
