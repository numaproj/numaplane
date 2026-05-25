# Riders

What if you need to deploy a certain type of resource alongside each child Numaflow resource?

For example, you'd like to update your ConfigMap as part of your progressive rollout, such that each child has a corresponding version of that ConfigMap.

That was the motivation for the feature called `Rider`.

A Rider can be defined within the Rollout's spec:

```
apiVersion: numaplane.numaproj.io/v1alpha1
kind: MonoVertexRollout
metadata:
  name: my-mv
  namespace: example-namespace
spec: 
  riders:
    # this causes a unique ConfigMap Resource to be generated as a child of the MonoVertex
    - definition: 
        apiVersion: v1
        kind: ConfigMap
        metadata:
            name: source-and-sink-configmap
        data:
            key1: something
            key2: {{.monovertex-name}}

    # if this is set, then perform a Progressive Rollout when it changes
    progressive: true

   
  monoVertex:
    spec:
      ...
      source:
        udsource:
          container:
            image: my-source-image:stable
            volumeMounts:
              - name: source-cm-volume
                mountPath: /etc/config
      ...
      volumes:
        - name: source-cm-volume
          configMap:
            # {{.monovertex-name}} is automatically resolved to be the MonoVertex name
            name: source-and-sink-configmap-{{.monovertex-name}}

  ...
```

Note the use of the template `{{.monovertex-name}}` in the spec above. This is a feature to enable referencing the MonoVertex name, and it can be in either the MonoVertex definition or the Rider definition or both.

## Progressive rollout indicator

Note in the spec above the use of `progressive: true`. This tells Numaplane that if the Rider definition itself changes, it should result in a Progressive Rollout. However, in many cases this can be set false.

## Permitted Riders

The Numaplane Controller ConfigMap has a field called `permittedRiders`. Here you can set which Rider Kinds are permitted on your platform.

## HPA as Rider for MonoVertex

While Numaflow generally does its own horizontal autoscaling, it does have a feature that a user can use HPA for any Vertex types which cannot be autoscaled through the Numaflow platform itself. Numaplane supports HPA as a Rider for MonoVertex but it is not currently implemented for Pipeline.

Why is HPA a special case? Shouldn't the Rider mechanism be sufficient?:

The reason why HPA is a special case is because it dictates how many pods need to be running, which is something that Numaplane carefully controls during its Progressive Rollout. For MonoVertex, however, this is now handled. 


