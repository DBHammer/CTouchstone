package ecnu.db.joininfo;

import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JoinInfoTableManager {
    private final Logger logger = LoggerFactory.getLogger(JoinInfoTableManager.class);
    private final ConcurrentHashMap<String, JoinInfoTable> TABLE_NAME_2_JOIN_INFORMATION_TABLE = new ConcurrentHashMap<>();
    private int joinInfoTableId;
    private int joinInfoTableNum;
    private String joinInfoTablePath;

    public static void setJoinInfoTablePath(String joinInfoTablePath) {
//        this.joinInfoTablePath = joinInfoTablePath;
    }

    public JoinInfoTable getJoinInformationTable(String tableName) {
        TABLE_NAME_2_JOIN_INFORMATION_TABLE.putIfAbsent(tableName, new JoinInfoTable());
        return TABLE_NAME_2_JOIN_INFORMATION_TABLE.get(tableName);
    }

    public void persistentAndMergeOthers(String tableName) throws IOException, TouchstoneException, InterruptedException, ClassNotFoundException {
        File file = new File(joinInfoTablePath + tableName + joinInfoTableId + ".swp");
        new ObjectOutputStream(new FileOutputStream(file)).writeObject(TABLE_NAME_2_JOIN_INFORMATION_TABLE.get(tableName));
        if (file.renameTo(new File(joinInfoTablePath + tableName + joinInfoTableId))) {
            logger.info("持久化JoinInfoTable-" + tableName + "-" + joinInfoTableId + "成功");
        } else {
            throw new TouchstoneException("无法持久化JoinInfoTable-" + tableName + "-" + joinInfoTableId);
        }
        logger.info("开始读取所有的JoinInfoTable-" + tableName);
        for (int i = 0; i < joinInfoTableNum; i++) {
            if (i != joinInfoTableId) {
                File otherJoinInfoTableFile = new File(joinInfoTablePath + tableName + i);
                while (!otherJoinInfoTableFile.exists()) {
                    Thread.sleep(1000);
                }
                TABLE_NAME_2_JOIN_INFORMATION_TABLE.get(tableName).mergeJoinInfo(
                        (JoinInfoTable) new ObjectInputStream(new FileInputStream(otherJoinInfoTableFile)).readObject());
            }
        }
        logger.info("读取所有JoinInfoTable-" + tableName + "成功");
    }



    private void initJoinInfoTable(int size, Map<Integer, boolean[]> pkBitMap) {
        List<Integer> pks = new ArrayList<>(pkBitMap.keySet());
        pks.sort(Integer::compareTo);
        for (int i = 0; i < size; i++) {
            long bitMap = 1L;
            for (int pk : pks) {
                bitMap = ((pkBitMap.get(pk)[i] ? 1L : 0L) & (bitMap << 1));
            }
            //todo
//            joinInfoTable.addJoinInfo(bitMap, new int[]{i});
        }
    }
}
