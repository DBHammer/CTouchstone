package ecnu.db.k8s;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.Yaml;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Objects;

/**
 * @author youshuhong
 * 创建configmap、创建一定数量的pod、将configmap的数据添加到pod里数据卷中的特定路径中
 */
public class CreatePod {
    public static void main(String[] args)
            throws IOException, ApiException, InterruptedException {
        //加载集群的config文件
        String configPath = "config";
        ApiClient client = ClientBuilder.kubeconfig(KubeConfig.loadKubeConfig(new FileReader(configPath))).build();
        Configuration.setDefaultApiClient(client);
        CoreV1Api coreV1Api = new CoreV1Api();
        //设定要创建的pod的数量
        int podNum = 1;
        //创建configmap
        File file = new File("src/main/resources/kustomization.yaml");
        V1ConfigMap yamlConfigMap = (V1ConfigMap) Yaml.load(file);
        coreV1Api.createNamespacedConfigMap("default", yamlConfigMap, null, null, null);
        //创建一定数量的pod
        for (int i = 0; i < podNum; i++) {
            File filePod = new File("src/main/resources/create-pod.yaml");
            V1Pod yamlPod = (V1Pod) Yaml.load(filePod);
            V1ObjectMeta objectMeta = yamlPod.getMetadata();
            objectMeta.setName(Objects.requireNonNull(yamlPod.getMetadata().getName())+ '-' + i+1);
            yamlPod.setMetadata(objectMeta);
            coreV1Api.createNamespacedPod("default", yamlPod, null, null ,null);
        }
        client.getHttpClient().connectionPool().evictAll();

    }
}