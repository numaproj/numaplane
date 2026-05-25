# Getting Started

This guide presumes the user is using Progressive Rollout strategy.

## Prerequisites

- Kubernetes cluster
- Container engine (Docker/Podman)
- kubectl
- Argo Rollouts 

## Installing Numaplane

Create a namespace for Numaplane:

```shell
kubectl create ns numaplane-system
```

Install Numaplane components:

```shell
kubectl apply -n numaplane-system -f https://raw.githubusercontent.com/numaproj/numaplane/main/config/install.yaml
```

Confirm Numaplane is running successfully: 

```shell
kubectl get pod -n numaplane-system
```

Note the Numaplane ConfigMap which can be customized as needed:

```shell
kubectl get configmap -n numaplane-system numaplane-controller-config -o yaml
```

### Create creating Numaplane resources, which creates Numaflow resources

Create a test namespace:

```shell
kubectl create ns example-namespace
kubectl config set-context --current --namespace=example-namespace
```

If you are running Numaflow in namespaced mode (rather than cluster mode), deploy the `NumaflowControllerRollout`:

```shell
kubectl apply -f config/samples/numaplane.numaproj.io_v1alpha1_numaflowcontrollerrollout.yaml
```

This will deploy a Numaflow Controller in your namespace. Otherwise, make sure you have one running on the cluster level.

Deploy the `ISBServiceRollout`:

```shell
kubectl apply -f config/samples/numaplane.numaproj.io_v1alpha1_isbservicerollout.yaml
```

This will deploy an InterstepBufferService in your namespace. Note that it is suffixed by index number (0 since it's the first one). Note its label: 

`numaflow.numaproj.io/upgradeState`: `promoted`

`promoted` and `trial` are the main labels used on the child resources to indicate whether they are still under assessment or whether they have been promoted forward. Whether it's healthy or not, the first one is always marked `promoted`.

Make sure the InterstepBufferService's Pods all come up successfully (make sure you have sufficient resources in kubernetes). Then deploy the `PipelineRollout`:

```shell
kubectl apply -f config/samples/numaplane.numaproj.io_v1alpha1_pipelinerollout.yaml
```

Like the InterstepBufferService, the first Pipeline is suffixed with index 0 and labeled `promoted`.

Once the Pipeline has been created, we can try to update it. Note that only field changes that are considered risky will result in a Progressive rollout. This includes changing fields like image path, environment variables, command arguments, ConfigMap mount, as well as topology. Any minor changes such as CPU/memory will simply be updated in place. 

```shell
kubectl edit pipelinerollout my-pipeline
```






