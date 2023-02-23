package ecnu.db.generator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataWriter {

    private static final Logger logger = LoggerFactory.getLogger(DataWriter.class);
    String outputPath;
    int generatorId;
    String lastSchemaName = null;
    BufferedWriter lastBufferedWriter = null;
    ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    public DataWriter(String outputPath, int generatorId) {
        this.outputPath = outputPath;
        this.generatorId = generatorId;
    }

    public void addWriteTask(String schemaName, StringBuilder[] keyData, String[] attData) {
        if (!schemaName.equals(lastSchemaName)) {
            File file = new File(outputPath, schemaName + generatorId);
            try {
                if (!file.exists() && file.createNewFile()) {
                    logger.info("创建文件{}", file.getName());
                }
                lastBufferedWriter = new BufferedWriter(new FileWriter(file, true));
            } catch (IOException e) {
                logger.error("写入文件失败{}", schemaName, e);
                System.exit(-1);
            }
        }
        BufferedWriter finalWriter = lastBufferedWriter;
        executorService.submit(() -> {
            try {
                StringBuilder file = new StringBuilder();
                for (int i = 0; i < keyData.length; i++) {
                    file.append(keyData[i]).append(attData[i]).append(System.lineSeparator());
                }
                finalWriter.write(file.toString());
                finalWriter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public boolean waitWriteFinish() throws InterruptedException {
        executorService.shutdown();
        return executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

}
