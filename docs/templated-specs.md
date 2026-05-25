# Templating

There are some use cases in which it's helpful to include the name of the child resource being deployed in the spec of that child resource. 

If this is needed, Numaplane provides the ability to add this to your spec:
    - `{{.pipeline-name}}` for Pipelines
    - `{{.monovertex-name}}` for MonoVertices
    - `{{.isbsvc-name}}` for InterstepBufferServices

Below is an example use case:

```
apiVersion: numaplane.numaproj.io/v1alpha1
kind: ISBServiceRollout
metadata:
  name: assessment-pipeline-isbsvc
spec:
  interStepBufferService:
    spec:
      jetstream:
        affinity:
          podAntiAffinity:
            preferredDuringSchedulingIgnoredDuringExecution:
            - podAffinityTerm:
                labelSelector:
                  matchLabels:
                    app.kubernetes.io/component: isbsvc
                    numaflow.numaproj.io/isbsvc-name: '{{.isbsvc-name}}'
                topologyKey: topology.kubernetes.io/zone
              weight: 100
        ...
```

Note the use of `{{.isbsvc-name}}`.

When Numaplane sees this, it evaluates the actual InterstepBufferService name (with integer suffix).

This feature also applies to [Riders](https://github.com/numaproj/numaplane/blob/main/docs/riders.md) that are defined in the Rollout's spec. Those can also be templated with those same keys to reference the Numaflow resource name. 