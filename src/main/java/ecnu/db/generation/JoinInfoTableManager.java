package ecnu.db.generation;

import ecnu.db.app.Generator;
import ecnu.db.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JoinInfoTableManager {
    private static int joinInfoTableId;
    private static int joinInfoTableNum;
    private static String joinInfoTablePath;
    private static final Logger logger = LoggerFactory.getLogger(Generator.class);
    private static final ConcurrentHashMap<String, JoinInfoTable> tableName2JoinInformationTable = new ConcurrentHashMap<>();

    public static JoinInfoTable getJoinInformationTable(String tableName) {
        tableName2JoinInformationTable.putIfAbsent(tableName, new JoinInfoTable());
        return tableName2JoinInformationTable.get(tableName);
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

    public static void persistentAndMergeOthers(String tableName) throws IOException, TouchstoneException, InterruptedException, ClassNotFoundException {
        File file = new File(joinInfoTablePath + tableName + joinInfoTableId + ".swp");
        new ObjectOutputStream(new FileOutputStream(file)).writeObject(tableName2JoinInformationTable.get(tableName));
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
                tableName2JoinInformationTable.get(tableName).mergeJoinInfo(
                        (JoinInfoTable) new ObjectInputStream(new FileInputStream(otherJoinInfoTableFile)).readObject());
            }
        }
        logger.info("读取所有JoinInfoTable-" + tableName + "成功");
    }
}
