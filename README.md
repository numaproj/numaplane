# numaplane
Numaplane is a control plane for deploying and seamlessly upgrading [Numaflow](https://github.com/numaproj/numaflow) resources on Kubernetes.

## Overview

The primary goal of Numaplane is to make it easy to create and update Numaflow resources without the user needing to worry about:

- any resource breaking
- losing data
- incurring downtime

The way that Numaplane does this is to have its own set of Kubernetes Resource types (PipelineRollout, MonoVertexRollout, ISBServiceRollout, and NumaflowControllerRollout) which serve as wrappers around the Numaflow Resource types (Pipeline, MonoVertex, InterstepBufferService, Numaflow Controller Deployment). Because of this, Numaplane is able to dynamically create, update, and delete the underlying Numaflow resources as needed.

Numaplane is deployed one-per-cluster and manages Numaflow deployments cluster-wide on any namespace.

There are two strategies which can be used:
1. The primary strategy is Progressive rollout. This is essentially a single stage Canary rollout - Numaplane deploys the new resource while the original is still running and only promotes it if successful. This strategy applies to the deployment of Pipeline, Monovertex, and InterstepBufferService. It is used in conjunction with Argo Rollouts and Prometheus. 
2. The alternative strategy is known as Pause-and-Drain. This strategy applies to Pipeline and not Monovertex. Instead of deploying a second Pipeline in parallel, this simply pauses the Pipeline whenever it, its InterstepBufferService, or the Numaflow Controller in the same namespace are updated. This strategy only concerns itself with the issue of losing data. 

The Progressive Rollout strategy should be preferred in most cases. 


## Getting Started

### To build Numaplane image and run it on your local cluster with latest manifests 

`make start`


### To auto-generate code and manifests from Go

`make codegen`

### To deploy the default configuration of Numaplane to a cluster

`kubectl apply -f config/install.yaml`

For a more in-depth overview, please see [getting-started.md](https://github.com/numaproj/numaplane/blob/main/docs/getting-started.md).

## Docs

- Walk through on [creating your Numaplane Resources and deploying updates using Progressive Rollouts](https://github.com/numaproj/numaplane/blob/main/docs/getting-started.md)
- [Integrating with ArgoCD](https://github.com/numaproj/numaplane/blob/main/docs/argo-cd-integration.md) for a more robust user experience
- [Templating](https://github.com/numaproj/numaplane/blob/main/docs/templated-specs.md) Numaflow specs 
- [Deploying resources (Riders)](https://github.com/numaproj/numaplane/blob/main/docs/riders.md) alongside your child resource 


## How To Release

### Release Branch

Always create a release branch for the releases, for example branch `release-0.5` is for all the v0.5.x versions release.
If it's a new release branch, simply create a branch from `main`.

### Release Steps

1. Cherry-pick fixes to the release branch, skip this and the following two steps if it's the first release in the branch.
2. Run `make test` to make sure all test cases pass locally.
3. Push to remote branch, and make sure all the CI jobs pass.
4. Run `make prepare-release VERSION=v{x.y.z}` to update version in manifests, where `x.y.z` is the expected new version.
5. Follow the output of last step, to confirm if all the changes are expected, and then run `make release VERSION=v{x.y.z}`.
6. Follow the output, push a new tag to the release branch, GitHub actions will automatically build and publish the new release,
   this will take around 10 minutes.
7. Test the new release, make sure everything is running as expected, and then recreate a `stable` tag against the latest release.
   ```shell
   git tag -d stable
   git tag -a stable -m stable
   git push -d {your-remote} stable
   git push {your-remote} stable
   ```
8. Find the new release tag, and edit the release notes.


## License

Copyright 2023 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
