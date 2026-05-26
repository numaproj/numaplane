# Deploying Numaflow Controller

For users deploying the Numaflow Controller as namespaced (i.e. each user namespace has its own), there is a Numaplane resource for it called `NumaflowControllerRollout`.

The definition for one of these is pretty simple:

```
apiVersion: numaplane.numaproj.io/v1alpha1
kind: NumaflowControllerRollout
metadata:
  name: numaflow-controller
  namespace: example-namespace
spec:
  controller:
    version: "1.5.2"
```

It just contains a version number. Numaplane looks for a ConfigMap which defines the spec corresponding to that version number. 

That ConfigMap should either live in the `numaplane-system` namespace or otherwise in the user's namespace (the latter definition takes precedence if it exists in both), and must:
- have the label: `numaplane.numaproj.io/config: numaflow-controller-definitions`
- define in its `controllerDefinitions` array a `version` key set to that version number

See the example ConfigMap below:

```
apiVersion: v1
kind: ConfigMap
metadata:
  name: numaflow-controller-definitions-1.5.2
  namespace: numaplane-system
  labels:
    "numaplane.numaproj.io/config": numaflow-controller-definitions
data:
  controller_definitions.yaml: |-
    controllerDefinitions:
      - version: "1.5.2"
        fullSpec: |-
            ....
```

You can choose to either have one ConfigMap which defines all versions or each ConfigMap can contain a single version - either way is fine. The files tend to be long, so having one ConfigMap per version may be easier. 

The [files](../tests/manifests/default/) which are used for the e2e test can serve as examples. (Ignore the templating in those definitions - that's no longer relevant.)