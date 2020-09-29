package ecnu.db.generation;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JoinInfoTableTest {

    @Test
    public void testAddJoinInfo() {
        JoinInfoTable table = new JoinInfoTable(1);
        int maxData = 10_000, maxListSize = 1000;
        JoinInfoTable.setMaxListSize(maxListSize);
        for (int i = 0; i < maxData; i++) {
            table.addJoinInfo(0L, new int[]{i + 1});
        }
        List<int[]> ret = table.getAllKeys(0L);
        int sum = 0;
        for (int i = 0; i < maxListSize; i++) {
            int val = ret.get(i)[0];
            sum += val;
        }
        assertEquals((1 + maxData) * 1.0 / 2,  1.0 * sum / maxListSize, 200);
    }
}