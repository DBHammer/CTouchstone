package ecnu.db.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DataWriter {

    String outputPath;
    int generatorId;

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    public DataWriter(String outputPath, int generatorId) {
        this.outputPath = outputPath;
        this.generatorId = generatorId;
    }

    public void addWriteTask(String schemaName, StringBuilder[] keyData, String[] attData) {
        executorService.submit(() -> {
            try {
                File file = new File(outputPath + "/" + schemaName + generatorId);
                if (!file.exists()) {
                    file.createNewFile();
                }
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
                for (int i = 0; i < keyData.length; i++) {
                    bufferedWriter.append(keyData[i]).append(attData[i]).append(System.lineSeparator());
                }
                bufferedWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void reset() {
        executorService = Executors.newSingleThreadExecutor();
    }

    public boolean waitWriteFinish() throws InterruptedException {
        executorService.shutdown();
        return executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

}
