package ecnu.db.utils;

import ecnu.db.utils.exception.TouchstoneException;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * @author youshuhong
 * 创建configmap、创建一定数量的pod、将configmap的数据添加到pod里数据卷中的特定路径中
 */
public class KubernetesClient {

    public static void createJob(String configPath,
                                 String nameSpace,
                                 String schemaConfig,
                                 String constraintChainConfig,
                                 String podPrefixName,
                                 int podNum) throws ApiException, IOException, TouchstoneException {
        //根据给定配置文件初始化Kubernetes客户端
        KubeConfig kubeConfig = KubeConfig.loadKubeConfig(new FileReader(configPath));
        Configuration.setDefaultApiClient(ClientBuilder.kubeconfig(kubeConfig).build());
        CoreV1Api coreV1Api = new CoreV1Api();
        if (!checkJoinTableVolume(coreV1Api)) {
            throw new TouchstoneException("Kubernetes中没有创建共享存储卷'join_table_pv'，请手动创建");
        }
        V1ConfigMap configMap = getConfigMap(podPrefixName, schemaConfig, constraintChainConfig);
        coreV1Api.createNamespacedConfigMap(nameSpace, configMap, null, null, null);
        BatchV1Api batchV1Api = new BatchV1Api();
        V1Job jobConfig = getJobConfig(podPrefixName);
        for (int i = 0; i < podNum; i++) {
            Objects.requireNonNull(jobConfig.getMetadata()).setName(podPrefixName + '-' + i);
            batchV1Api.createNamespacedJob(nameSpace, jobConfig, null, null, null);
        }
    }

    //todo check join table
    private static boolean checkJoinTableVolume(CoreV1Api coreV1Api) {

        return true;
    }

    private static V1ConfigMap getConfigMap(String podPrefixName, String schemaConfig, String constraintChainConfig) {
        Map<String, String> configData = new HashMap<>();
        configData.put("table_config", schemaConfig);
        configData.put("constraint_chain", constraintChainConfig);
        return new V1ConfigMap().metadata(new V1ObjectMeta().name(podPrefixName + "-config")).data(configData);
    }


    private static V1Job getJobConfig(String podPrefixName) {
        var label = new V1ObjectMeta().labels(Collections.singletonMap("jobGroup", podPrefixName));

        // join information location
        var joinTableMount = new V1VolumeMount().name("join_table_pv").mountPath("/join_table");
        // table config location
        var tableConfig = new V1EnvVar().name("TABLE_CONFIG").valueFrom(new V1EnvVarSource()
                .configMapKeyRef(new V1ConfigMapKeySelector().name(podPrefixName + "-config").key("table_config")));
        // constraint chain location
        var constraintChain = new V1EnvVar().name("CONSTRAINT_CHAIN").valueFrom(new V1EnvVarSource()
                .configMapKeyRef(new V1ConfigMapKeySelector().name(podPrefixName + "-config").key("constraint_chain")));

        var podContainer = new V1Container().name("touchstone-generator")
                .image("dbhammer/touchstone:latest").imagePullPolicy("Always")
                .volumeMounts(Collections.singletonList(joinTableMount))
                .env(Arrays.asList(tableConfig, constraintChain));
        var podSpec = new V1PodSpec().containers(Collections.singletonList(podContainer));

        var podTemplateSpec = new V1PodTemplateSpec().metadata(label).spec(podSpec);

        return new V1Job().metadata(label).spec(new V1JobSpec().template(podTemplateSpec));
    }
}