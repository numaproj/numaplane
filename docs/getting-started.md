# Getting Started

This guide presumes the user is using Progressive Rollout strategy.

## Prerequisites

- Kubernetes cluster
- Container engine (Docker/Podman)
- kubectl
- Argo Rollouts (this will be automatically deployed if using Option 2 below to install)

## Installing Numaplane

Create a namespace for Numaplane:

```shell
kubectl create ns numaplane-system
```

Option 1: Clone this repo and run `make start` (this will actually deploy Argo Rollouts and Prometheus for you)

Option 2: install Numaplane components using the Kubernetes manifest (make sure you have Argo Rollouts and Prometheus deployed separately):

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

(This may be easier to view in the repo at `config/manager/controller-config.yaml`.)

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

This will deploy a Numaflow Controller in your namespace corresponding to the version number in the spec. (The Numaflow Controller manifest definition is a ConfigMap which lives in the `numaplane-system` namespace.) Otherwise, make sure you have a Numaflow Controller running on the cluster level.

Deploy the `ISBServiceRollout`:

```shell
kubectl apply -f config/samples/numaplane.numaproj.io_v1alpha1_isbservicerollout.yaml
```

This will deploy an InterstepBufferService in your namespace. Note that it is suffixed by index number (0 since it's the first one). Note its label: 

`numaplane.numaproj.io/upgradeState`: `promoted`

`promoted` and `trial` are the main labels used on the child resources to indicate whether they are still under assessment or whether they have been promoted forward. Whether it's healthy or not, the first one is always marked `promoted`.

Make sure the InterstepBufferService's Pods all come up successfully (make sure you have sufficient resources in kubernetes). Then deploy the `PipelineRollout` as well as the corresponding Argo Rollouts `AnalysisTemplate` which it references:

```shell
kubectl apply -f config/samples/numaplane.numaproj.io_v1alpha1_pipelinerollout.yaml
```

Like the InterstepBufferService, the first Pipeline is suffixed with index 0 and labeled `promoted`.

Once the Pipeline has been created, we can try to update it. Note that only field changes that are considered risky will result in a Progressive rollout. This includes changing fields like image path, environment variables, command arguments, ConfigMap mount, as well as topology. Any minor changes such as CPU/memory will simply be updated in place. 

```shell
kubectl edit pipelinerollout my-pipeline
```

### Cause a Progressive Rollout

To make a simple change which will cause a successful Progressive Rollout, add a new inconsequential environment variable in one of the Vertices. For example:

```
      - name: cat
        ...
        udf:
          container:
            ...
            env:
            - name: TEST
              value: test1
```

Save that.

You should see after a few seconds that a second Pipeline gets created with your new spec. This one is indexed with a 1 and has the following label:

`numaplane.numaproj.io/upgradeState`: `trial`

This one is now being assessed. You can view the Status of the PipelineRollout as the assessment is occurring. There is a section called `upgradingPipelineStatus` which provides start time and end time of the assessment, as well as whether it's still in progress, has succeeded, or has failed.

```shell
kubectl get pipelinerollout my-pipeline --watch -o yaml
```

During the assessment process, the original number of Pods running for each Vertex is maintained from before, but is split between the old Pipeline and new Pipeline. Say there were 4 Pods running for a given Vertex previously: there would be 2 running in the old and 2 running in the new. However, if there was only 1 Pod running previously for a given Vertex, then there will be 1 Pod running in the new Pipeline and 0 in the old. (This is manipulated through the `scale.min` and `scale.max` values in Pipeline spec.)

The Progressive Rollout assessment occurs in 2 steps:
1. The Pipeline must come up as healthy within a certain (configurable) number of minutes. It needs to remain healthy for 1 consecutive minute. If this fails, the `trial` Pipeline is deemed "failed".
2. If step 1 succeeds, an Argo Rollouts `AnalysisRun` is created from the `AnalysisTemplate` spec. This spec says to check the metrics `read` and `ack` for each Vertex to verify that `if read > 0, then ack must be > 0`. A certain number of minutes is given to account for slow-processing pipelines. If the `AnalysisRun` is deemed Successful, then the `trial` Pipeline succeeds and becomes `promoted`, replacing the previously `promoted` one. If it fails, the `trial` Pipeline is deemed "failed". 

Observe not only the PipelineRollout Status but also the AnalysisRun Status:

```shell
kubectl get analysisrun --watch -o yaml
```

Assuming that Argo Rollouts and Prometheus are all functioning, the new Pipeline should succeed after a few minutes and you should see the Pipeline become `promoted` and the original Pipeline should get deleted. (For Pipelines, note that they are actually first drained (paused) and then deleted.)

#### Cause a Progressive Rollout failure

The simplest way to cause a Progressive Rollout failure is to use an image path which doesn't exist. 

```shell
kubectl edit pipelinerollout my-pipeline
```

Change one of the image paths to one which doesn't exist. 

```
        udf:
          container:
            ...
            image: quay.io/numaio/numaflow-go/map-cat:fakepath
            ...
```

This time, you'll see a `my-pipeline-2` get created, which is your new `trial`. If you observe the PipelineRollout Status, this time you'll see that it is assessed as failed after a few minutes. The Pipeline's Status at the time of failure is captured in the Status as well. 

There's also a label on the Pipeline to indicate the failure:

`numaplane.numaproj.io/progressive-result-state`: `failed`

The other thing that happens is that the new Pipeline scales down to `scale.min=0,scale.max=0` Pods and the `promoted` one scales back up to the scale min/max defined in the PipelineRollout spec. 

#### Cause a Progressive Rollout success

```shell
kubectl edit pipelinerollout my-pipeline
```

This time change the image path back to one which is valid (`quay.io/numaio/numaflow-go/map-cat:stable`), and also remove the environment variable added earlier - the combination of these two changes will cause a new Progressive Rollout (if we were only to do the former change, we would simply see `my-pipeline-2` deleted and that's it, since `my-pipeline-1` would already match the PipelineRollout spec).

`my-pipeline-2` is now being replaced, so it's marked:

`numaplane.numaproj.io/upgradeState`: `recyclable`

This means that it's marked for deletion; however, Pipelines must be drained before they're deleted to prevent data loss. Given that the Pipeline was not healthy, it is not able to drain by itself, which was the motivation for the feature known as "Force Draining" (link TBD).

You should see a `my-pipeline-3` get created and pass all assessments. After it succeeds, it will effectively replace `my-pipeline-1` (our `promoted` one) which also gets the label `recyclable` and is then drained and deleted. (Since this one is healthy, it can drain on its own.)

The "Force Draining" feature makes it so that `my-pipeline-2` takes on the healthy spec from `my-pipeline-3` and can then fully drain and then be deleted.

#### Cause a Progressive Rollout for InterstepBufferService

```shell
kubectl edit isbservicerollout my-isbsvc
```

Change the version number to 2.10.11 and save it.

When the InterstepBufferService spec is updated, it causes a new InterstepBufferService to get created (`my-isbsvc-1`), and so any PipelineRollout set to use it is also progressively rolled out to use the new one. You will see a `my-pipeline-4`. If you look at the spec, you'll see that it's mapped to `my-isbsvc-1`. Once again, you can observe the Status of the `PipelineRollout` and also the Status of the `ISBServiceRollout`. If all PipelineRollouts succeed (in this case just one), then the InterstepBufferService is also marked Successful. This will take a few minutes, but once complete you will just see `my-isbsvc-1` and `my-pipeline-4`.

Note that if you observe the PipelineRollout Status during the assessment, if there are any ephemeral failures of the Pipeline Status, those will be shown under `upgradingPipelineStatus.childStatus`. However, that is merely a capture of the last failure and not an indication by itself that the final result was Failure.

#### Create a MonoVertexRollout and update it

You should be able to adapt the same process above to MonoVertex to cause successful and failed Progressive Rollouts. You can find the sample spec at `config/samples/numaplane.numaproj.io_v1alpha1_monovertexrollout.yaml`. (In this case, of course there is no InterstepBufferService.)









