package ecnu.db.utils;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

/**
 * @author youshuhong
 * 创建configmap、创建一定数量的pod、将configmap的数据添加到pod里数据卷中的特定路径中
 */
public class KubernetesClient {
    private final ApiClient apiClient;
    private final CoreV1Api coreV1Api;

    KubernetesClient(String configPath) throws IOException {
        apiClient = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(configPath))).build();
        Configuration.setDefaultApiClient(apiClient);
        coreV1Api = new CoreV1Api();
    }

    public void sendConfig(HashMap<String, String> config) {
        coreV1Api.createNamespacedConfigMap(nameSpace, yamlConfigMap, null, null, null);
    }

    public void createPods(String nameSpace, int podNum, String podPrefixName) throws ApiException, IOException, TouchstoneToolChainException {
        V1Pod podConfig = (V1Pod) Yaml.load(new File("create-pod.yaml"));
        if (podConfig.getMetadata() == null) {
            throw new TouchstoneToolChainException("未配置pod元数据");
        }
        if (podPrefixName == null) {
            podPrefixName = podConfig.getMetadata().getName();
        }

        //创建一定数量的pod
        for (int i = 0; i < podNum; i++) {
            podConfig.getMetadata().setName(podPrefixName + '-' + i);
            coreV1Api.createNamespacedPod(nameSpace, podConfig, null, null, null);
        }
    }

    public void close() {
        apiClient.getHttpClient().connectionPool().evictAll();
    }
}