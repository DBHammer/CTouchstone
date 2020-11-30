package ecnu.db.utils;

import ecnu.db.exception.TouchstoneException;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.*;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;

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
        return new V1ConfigMapBuilder()
                .withNewMetadata()
                .withName(podPrefixName + "-config")
                .endMetadata()
                .withData(new HashMap<String, String>() {{
                    put("table_config", schemaConfig);
                    put("constraint_chain", constraintChainConfig);
                }})
                .build();
    }


    private static V1Job getJobConfig(String podPrefixName) {
        return new V1JobBuilder()
                .withNewMetadata()
                .withLabels(Collections.singletonMap("jobgroup", podPrefixName))
                .endMetadata()
                .withNewSpec()
                .withNewTemplate()
                .withNewMetadata()
                .withName(podPrefixName)
                .withLabels(Collections.singletonMap("jobgroup", podPrefixName))
                .endMetadata()
                .withNewSpec()
                .withContainers(Collections.singletonList(
                        new V1ContainerBuilder()
                                .withName("touchstone-generator")
                                .withImage("dbhammer/touchstone:latest")
                                .withImagePullPolicy("Always")
                                .withVolumeMounts(
                                        new V1VolumeMountBuilder()
                                                .withName("join_table_pv")
                                                .withMountPath("/join_table").build())
                                .withEnv(Arrays.asList(
                                        new V1EnvVarBuilder().withName("TABLE_CONFIG")
                                                .withNewValueFrom()
                                                .withNewConfigMapKeyRef()
                                                .withName(podPrefixName + "-config")
                                                .withKey("table_config")
                                                .endConfigMapKeyRef()
                                                .endValueFrom()
                                                .build(),
                                        new V1EnvVarBuilder().withName("CONSTRAINT_CHAIN")
                                                .withNewValueFrom()
                                                .withNewConfigMapKeyRef()
                                                .withName(podPrefixName + "-config")
                                                .withKey("constraint_chain")
                                                .endConfigMapKeyRef()
                                                .endValueFrom()
                                                .build()))
                                .build()))
                .endSpec()
                .endTemplate()
                .endSpec()
                .build();
    }
}