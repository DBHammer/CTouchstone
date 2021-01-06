package ecnu.db.joininfo;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinInfoTableTest {

    @Test
    public void testAddJoinInfo() {
        JoinInfoTable table = new JoinInfoTable(1);
        int maxData = 10_000, maxListSize = 1000;
        JoinInfoTable.setMaxListSize(maxListSize);
        List<int[]> status2Keys = new ArrayList<>();
        for (int i = 0; i < maxData; i++) {
            status2Keys.add(new int[]{i});
        }
        table.addJoinInfo(new ImmutablePair<>(0L, status2Keys));
        List<int[]> ret = table.getAllKeys(0L);
        int sum = 0;
        for (int i = 0; i < maxListSize; i++) {
            sum += ret.get(i)[0];
        }
        assertEquals((1 + maxData) * 1.0 / 2, 1.0 * sum / maxListSize, 400);
    }
}