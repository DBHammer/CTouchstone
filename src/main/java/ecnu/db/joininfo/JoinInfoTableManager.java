package ecnu.db.joininfo;

import ecnu.db.utils.exception.TouchstoneException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;

public class JoinInfoTableManager {
    private int joinInfoTableId;
    private int joinInfoTableNum;
    private String joinInfoTablePath;
    private static final JoinInfoTableManager INSTANCE = new JoinInfoTableManager();
    private final Logger logger = LoggerFactory.getLogger(JoinInfoTableManager.class);
    private final HashMap<String, JoinInfoTable> TABLE_NAME_2_JOIN_INFORMATION_TABLE = new HashMap<>();


    private JoinInfoTableManager() {
    }

    public static JoinInfoTableManager getInstance() {
        return INSTANCE;
    }

    public void setJoinInfoTablePath(String joinInfoTablePath) {
        this.joinInfoTablePath = joinInfoTablePath;
    }

    public void putJoinInfoTable(String tableName, JoinInfoTable joinInfoTable) {
        if (TABLE_NAME_2_JOIN_INFORMATION_TABLE.containsKey(tableName)) {
            TABLE_NAME_2_JOIN_INFORMATION_TABLE.get(tableName).mergeJoinInfo(joinInfoTable);
        } else {
            TABLE_NAME_2_JOIN_INFORMATION_TABLE.put(tableName, joinInfoTable);
        }
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
                putJoinInfoTable(tableName, (JoinInfoTable) new ObjectInputStream(new FileInputStream(otherJoinInfoTableFile)).readObject());
            }
        }
        logger.info("读取所有JoinInfoTable-" + tableName + "成功");
    }


}
