export const mockProps = {
  "tree": {
    "nodes": [
      {
        "version": "v1",
        "kind": "ConfigMap",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-config",
        "uid": "a1a0b2d2-1249-49e5-9e45-4a2d0c4bce2d",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "InterStepBufferService",
            "namespace": "demo-numarollotus",
            "name": "my-isbsvc",
            "uid": "4ea0c8a0-ec1a-43f6-aa4b-d9183288c7f1"
          }
        ],
        "resourceVersion": "2784960",
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "version": "v1",
        "kind": "ConfigMap",
        "namespace": "demo-numarollotus",
        "name": "numaflow-cmd-params-config",
        "uid": "3993fc8e-b3c7-4d9a-b9d7-c8b6e47a3ffe",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784891",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "version": "v1",
        "kind": "ConfigMap",
        "namespace": "demo-numarollotus",
        "name": "numaflow-controller-config",
        "uid": "735fbf9d-1507-4fcc-8c13-ac0187ad2703",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784894",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "version": "v1",
        "kind": "ConfigMap",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-local-user-config",
        "uid": "6e9f9f1d-ef8b-42c7-bcba-73ca72346999",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784892",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "version": "v1",
        "kind": "ConfigMap",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-rbac-config",
        "uid": "e52cbf84-1faa-4f2b-8463-cc4caef5568a",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784893",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "version": "v1",
        "kind": "Endpoints",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-svc",
        "uid": "d242bd66-c971-4012-a1ea-28b875122389",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js-svc",
            "uid": "901eac7c-6d8a-4211-a099-42aa1358af5a"
          }
        ],
        "resourceVersion": "2785320",
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "version": "v1",
        "kind": "Endpoints",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-cat-headless",
        "uid": "d68937b9-cc59-4958-83b0-fbda3a9ce058",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-cat-headless",
            "uid": "6a924c13-f2b0-4859-9387-1c9a3e243954"
          }
        ],
        "resourceVersion": "2785501",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Endpoints",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-daemon-svc",
        "uid": "da97164f-3fc4-46dd-835d-4c1680764fdf",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-daemon-svc",
            "uid": "133fab2b-78f8-4430-ad72-1e65c3eee98b"
          }
        ],
        "resourceVersion": "2785701",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Endpoints",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-in-headless",
        "uid": "20665c38-96a4-49a8-887d-8005408c7bc5",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-in-headless",
            "uid": "68bf2b29-9ae3-449e-80a1-1f1f615a1205"
          }
        ],
        "resourceVersion": "2785509",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Endpoints",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-out-headless",
        "uid": "97336155-82f2-4d9c-b887-7c51265221f7",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-out-headless",
            "uid": "d71318ca-0e8d-4617-8022-971bf3fb253c"
          }
        ],
        "resourceVersion": "2785495",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "PersistentVolumeClaim",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-vol-isbsvc-my-isbsvc-js-0",
        "uid": "20a1d428-9e0a-49aa-b1b0-536e5d425c52",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "resourceVersion": "2785173",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "version": "v1",
        "kind": "PersistentVolumeClaim",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-vol-isbsvc-my-isbsvc-js-1",
        "uid": "852ef4b9-96ee-4b36-bae8-bb43bf014f7c",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "resourceVersion": "2785170",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "PersistentVolumeClaim",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-vol-isbsvc-my-isbsvc-js-2",
        "uid": "97eb7019-3f3a-414f-afd9-06057fc60608",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "resourceVersion": "2785164",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-0",
        "uid": "f9006a45-4cea-4e2a-a334-76444056728d",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-48-143.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "3/3"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "isbsvc",
            "app.kubernetes.io/managed-by": "isbsvc-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az1",
            "controller-revision-hash": "isbsvc-my-isbsvc-js-694df4f759",
            "numaflow.numaproj.io/isbsvc-name": "my-isbsvc",
            "numaflow.numaproj.io/isbsvc-type": "jetstream",
            "statefulset.kubernetes.io/pod-name": "isbsvc-my-isbsvc-js-0"
          }
        },
        "resourceVersion": "2785504",
        "images": [
          "docker.intuit.com/docker-rmt/nats:2.10.11",
          "docker.intuit.com/oss-analytics/dataflow/service/nats-server-config-reloader:0.7.0-patched",
          "docker.intuit.com/oss-analytics/dataflow/service/prometheus-nats-exporter:0.9.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-1",
        "uid": "a9cfdbae-2b21-40d7-a5c9-ca111766b436",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-50-219.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "3/3"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "isbsvc",
            "app.kubernetes.io/managed-by": "isbsvc-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az2",
            "controller-revision-hash": "isbsvc-my-isbsvc-js-694df4f759",
            "numaflow.numaproj.io/isbsvc-name": "my-isbsvc",
            "numaflow.numaproj.io/isbsvc-type": "jetstream",
            "statefulset.kubernetes.io/pod-name": "isbsvc-my-isbsvc-js-1"
          }
        },
        "resourceVersion": "2785513",
        "images": [
          "docker.intuit.com/docker-rmt/nats:2.10.11",
          "docker.intuit.com/oss-analytics/dataflow/service/nats-server-config-reloader:0.7.0-patched",
          "docker.intuit.com/oss-analytics/dataflow/service/prometheus-nats-exporter:0.9.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-2",
        "uid": "32a2810f-ed3c-4235-8bd7-f07fdc892a7c",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-52-89.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "3/3"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "isbsvc",
            "app.kubernetes.io/managed-by": "isbsvc-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az3",
            "controller-revision-hash": "isbsvc-my-isbsvc-js-694df4f759",
            "numaflow.numaproj.io/isbsvc-name": "my-isbsvc",
            "numaflow.numaproj.io/isbsvc-type": "jetstream",
            "statefulset.kubernetes.io/pod-name": "isbsvc-my-isbsvc-js-2"
          }
        },
        "resourceVersion": "2785546",
        "images": [
          "docker.intuit.com/docker-rmt/nats:2.10.11",
          "docker.intuit.com/oss-analytics/dataflow/service/nats-server-config-reloader:0.7.0-patched",
          "docker.intuit.com/oss-analytics/dataflow/service/prometheus-nats-exporter:0.9.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-cat-0-vjsvw",
        "uid": "25f5d54e-bfb3-4562-9c7e-8931326462a6",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Vertex",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-cat",
            "uid": "391c92d7-7fbe-44d3-bef6-8917f8ba68f4"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-48-143.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "2/2"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "vertex",
            "app.kubernetes.io/managed-by": "vertex-controller",
            "app.kubernetes.io/name": "my-pipeline-cat",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az1",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "numaflow.numaproj.io/vertex-name": "cat"
          }
        },
        "resourceVersion": "2785499",
        "images": [
          "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-daemon-789d784877-nl2ss",
        "uid": "c27688c7-422c-4900-967a-8eb26eff7357",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "ReplicaSet",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-daemon-789d784877",
            "uid": "06b9b934-4903-4cda-a3cd-e496289f6c70"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-50-219.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "1/1"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "daemon",
            "app.kubernetes.io/managed-by": "pipeline-controller",
            "app.kubernetes.io/name": "my-pipeline-daemon",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az2",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "pod-template-hash": "789d784877"
          }
        },
        "resourceVersion": "2785698",
        "images": [
          "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-in-0-vzpt2",
        "uid": "63241ce4-b8b2-4981-90e3-590a2ea442b1",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Vertex",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-in",
            "uid": "1cf970e5-f1c7-4653-95e5-662841eb3d78"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-52-89.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "1/1"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "vertex",
            "app.kubernetes.io/managed-by": "vertex-controller",
            "app.kubernetes.io/name": "my-pipeline-in",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az3",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "numaflow.numaproj.io/vertex-name": "in"
          }
        },
        "resourceVersion": "2785507",
        "images": [
          "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-out-0-rrrgv",
        "uid": "9fb1bc2b-66eb-4244-a1e4-f6a7a21ee1a6",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Vertex",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-out",
            "uid": "bdd7b0e3-39ec-4fd4-8284-1fb239141a8e"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-48-143.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "1/1"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "vertex",
            "app.kubernetes.io/managed-by": "vertex-controller",
            "app.kubernetes.io/name": "my-pipeline-out",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az1",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "numaflow.numaproj.io/vertex-name": "out"
          }
        },
        "resourceVersion": "2785494",
        "images": [
          "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Pod",
        "namespace": "demo-numarollotus",
        "name": "numaflow-controller-6dbd5c6b5d-fnw5w",
        "uid": "6c7b81b5-ebfe-4fae-ad19-29fd3eb3a85d",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "ReplicaSet",
            "namespace": "demo-numarollotus",
            "name": "numaflow-controller-6dbd5c6b5d",
            "uid": "9581af52-1ed0-47b2-98e9-bb272450fa29"
          }
        ],
        "info": [
          {
            "name": "Status Reason",
            "value": "Running"
          },
          {
            "name": "Node",
            "value": "ip-10-74-52-89.us-west-2.compute.internal"
          },
          {
            "name": "Containers",
            "value": "1/1"
          }
        ],
        "networkingInfo": {
          "labels": {
            "app.kubernetes.io/component": "controller-manager",
            "app.kubernetes.io/name": "controller-manager",
            "app.kubernetes.io/part-of": "numaflow",
            "azId": "usw2-az3",
            "pod-template-hash": "6dbd5c6b5d"
          }
        },
        "resourceVersion": "2785110",
        "images": [
          "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
        ],
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:23Z"
      },
      {
        "version": "v1",
        "kind": "Secret",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-client-auth",
        "uid": "1c5e30ee-e7e3-4d1e-8404-24388d2b7de4",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "InterStepBufferService",
            "namespace": "demo-numarollotus",
            "name": "my-isbsvc",
            "uid": "4ea0c8a0-ec1a-43f6-aa4b-d9183288c7f1"
          }
        ],
        "resourceVersion": "2784959",
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "version": "v1",
        "kind": "Secret",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-server",
        "uid": "4d5a17e3-1e7d-444d-a087-9416c0b95a4d",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "InterStepBufferService",
            "namespace": "demo-numarollotus",
            "name": "my-isbsvc",
            "uid": "4ea0c8a0-ec1a-43f6-aa4b-d9183288c7f1"
          }
        ],
        "resourceVersion": "2784958",
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "version": "v1",
        "kind": "Secret",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-secrets",
        "uid": "fe942464-7552-458a-8b96-d0a2866fda64",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784890",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "version": "v1",
        "kind": "Service",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-svc",
        "uid": "901eac7c-6d8a-4211-a099-42aa1358af5a",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "InterStepBufferService",
            "namespace": "demo-numarollotus",
            "name": "my-isbsvc",
            "uid": "4ea0c8a0-ec1a-43f6-aa4b-d9183288c7f1"
          }
        ],
        "networkingInfo": {
          "targetLabels": {
            "app.kubernetes.io/component": "isbsvc",
            "app.kubernetes.io/managed-by": "isbsvc-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "numaflow.numaproj.io/isbsvc-name": "my-isbsvc",
            "numaflow.numaproj.io/isbsvc-type": "jetstream"
          }
        },
        "resourceVersion": "2784962",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "version": "v1",
        "kind": "Service",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-cat-headless",
        "uid": "6a924c13-f2b0-4859-9387-1c9a3e243954",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Vertex",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-cat",
            "uid": "391c92d7-7fbe-44d3-bef6-8917f8ba68f4"
          }
        ],
        "networkingInfo": {
          "targetLabels": {
            "app.kubernetes.io/component": "vertex",
            "app.kubernetes.io/managed-by": "vertex-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "numaflow.numaproj.io/vertex-name": "cat"
          }
        },
        "resourceVersion": "2785056",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Service",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-daemon-svc",
        "uid": "133fab2b-78f8-4430-ad72-1e65c3eee98b",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Pipeline",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline",
            "uid": "0bdc8a5b-c46a-4ecf-a111-bf7e268cbc4a"
          }
        ],
        "networkingInfo": {
          "targetLabels": {
            "app.kubernetes.io/component": "daemon",
            "app.kubernetes.io/managed-by": "pipeline-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline"
          }
        },
        "resourceVersion": "2785022",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Service",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-in-headless",
        "uid": "68bf2b29-9ae3-449e-80a1-1f1f615a1205",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Vertex",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-in",
            "uid": "1cf970e5-f1c7-4653-95e5-662841eb3d78"
          }
        ],
        "networkingInfo": {
          "targetLabels": {
            "app.kubernetes.io/component": "vertex",
            "app.kubernetes.io/managed-by": "vertex-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "numaflow.numaproj.io/vertex-name": "in"
          }
        },
        "resourceVersion": "2785026",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "Service",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-out-headless",
        "uid": "d71318ca-0e8d-4617-8022-971bf3fb253c",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Vertex",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-out",
            "uid": "bdd7b0e3-39ec-4fd4-8284-1fb239141a8e"
          }
        ],
        "networkingInfo": {
          "targetLabels": {
            "app.kubernetes.io/component": "vertex",
            "app.kubernetes.io/managed-by": "vertex-controller",
            "app.kubernetes.io/part-of": "numaflow",
            "numaflow.numaproj.io/pipeline-name": "my-pipeline",
            "numaflow.numaproj.io/vertex-name": "out"
          }
        },
        "resourceVersion": "2785009",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "version": "v1",
        "kind": "ServiceAccount",
        "namespace": "demo-numarollotus",
        "name": "numaflow-sa",
        "uid": "83ccf0ec-6938-48d5-aed8-79738eb6a538",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784888",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "version": "v1",
        "kind": "ServiceAccount",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-sa",
        "uid": "919a6358-6821-4469-9d51-a546463524d3",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784889",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "apps",
        "version": "v1",
        "kind": "ControllerRevision",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-694df4f759",
        "uid": "1e92a419-ecab-4506-bd18-409253211d10",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "StatefulSet",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js",
            "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e"
          }
        ],
        "info": [
          {
            "name": "Revision",
            "value": "Rev:1"
          }
        ],
        "resourceVersion": "2784970",
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "group": "apps",
        "version": "v1",
        "kind": "Deployment",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-daemon",
        "uid": "45014e3d-22c2-4b42-b31a-020a4d0f77e9",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Pipeline",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline",
            "uid": "0bdc8a5b-c46a-4ecf-a111-bf7e268cbc4a"
          }
        ],
        "info": [
          {
            "name": "Revision",
            "value": "Rev:1"
          }
        ],
        "resourceVersion": "2785702",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "apps",
        "version": "v1",
        "kind": "Deployment",
        "namespace": "demo-numarollotus",
        "name": "numaflow-controller",
        "uid": "15409404-2836-43b0-a481-61d9fb4810e5",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "info": [
          {
            "name": "Revision",
            "value": "Rev:1"
          }
        ],
        "resourceVersion": "2785112",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "apps",
        "version": "v1",
        "kind": "ReplicaSet",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-daemon-789d784877",
        "uid": "06b9b934-4903-4cda-a3cd-e496289f6c70",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "Deployment",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-daemon",
            "uid": "45014e3d-22c2-4b42-b31a-020a4d0f77e9"
          }
        ],
        "info": [
          {
            "name": "Revision",
            "value": "Rev:1"
          }
        ],
        "resourceVersion": "2785699",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "apps",
        "version": "v1",
        "kind": "ReplicaSet",
        "namespace": "demo-numarollotus",
        "name": "numaflow-controller-6dbd5c6b5d",
        "uid": "9581af52-1ed0-47b2-98e9-bb272450fa29",
        "parentRefs": [
          {
            "group": "apps",
            "kind": "Deployment",
            "namespace": "demo-numarollotus",
            "name": "numaflow-controller",
            "uid": "15409404-2836-43b0-a481-61d9fb4810e5"
          }
        ],
        "info": [
          {
            "name": "Revision",
            "value": "Rev:1"
          }
        ],
        "resourceVersion": "2785111",
        "health": {
          "status": "Healthy"
        },
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "apps",
        "version": "v1",
        "kind": "StatefulSet",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js",
        "uid": "e09f9908-3062-4d1f-9e47-d691a3c19b3e",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "InterStepBufferService",
            "namespace": "demo-numarollotus",
            "name": "my-isbsvc",
            "uid": "4ea0c8a0-ec1a-43f6-aa4b-d9183288c7f1"
          }
        ],
        "resourceVersion": "2785548",
        "health": {
          "status": "Healthy",
          "message": "partitioned roll out complete: 3 new pods have been updated..."
        },
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "group": "discovery.k8s.io",
        "version": "v1",
        "kind": "EndpointSlice",
        "namespace": "demo-numarollotus",
        "name": "isbsvc-my-isbsvc-js-svc-2f6p9",
        "uid": "84c41d87-8dc6-4c95-9f59-a1051efd5472",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "isbsvc-my-isbsvc-js-svc",
            "uid": "901eac7c-6d8a-4211-a099-42aa1358af5a"
          }
        ],
        "resourceVersion": "2785547",
        "createdAt": "2024-06-07T21:27:24Z"
      },
      {
        "group": "discovery.k8s.io",
        "version": "v1",
        "kind": "EndpointSlice",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-cat-headless-p6zkc",
        "uid": "58efdcaa-9098-4607-bec9-f82104c4ce72",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-cat-headless",
            "uid": "6a924c13-f2b0-4859-9387-1c9a3e243954"
          }
        ],
        "resourceVersion": "2785500",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "discovery.k8s.io",
        "version": "v1",
        "kind": "EndpointSlice",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-daemon-svc-t8pmq",
        "uid": "15425581-bce2-40c0-9832-d6705640ada7",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-daemon-svc",
            "uid": "133fab2b-78f8-4430-ad72-1e65c3eee98b"
          }
        ],
        "resourceVersion": "2785700",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "discovery.k8s.io",
        "version": "v1",
        "kind": "EndpointSlice",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-in-headless-csdw4",
        "uid": "53298902-2ed0-4ba7-8263-c8b3d8138ae6",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-in-headless",
            "uid": "68bf2b29-9ae3-449e-80a1-1f1f615a1205"
          }
        ],
        "resourceVersion": "2785508",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "discovery.k8s.io",
        "version": "v1",
        "kind": "EndpointSlice",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-out-headless-gthld",
        "uid": "effb082c-9927-4353-b3d0-355acc5d8390",
        "parentRefs": [
          {
            "kind": "Service",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline-out-headless",
            "uid": "d71318ca-0e8d-4617-8022-971bf3fb253c"
          }
        ],
        "resourceVersion": "2785496",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "numaflow.numaproj.io",
        "version": "v1alpha1",
        "kind": "InterStepBufferService",
        "namespace": "demo-numarollotus",
        "name": "my-isbsvc",
        "uid": "4ea0c8a0-ec1a-43f6-aa4b-d9183288c7f1",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "ISBServiceRollout",
            "namespace": "demo-numarollotus",
            "name": "my-isbsvc",
            "uid": "dab68efe-de38-4bd5-be14-21de651fd858"
          }
        ],
        "resourceVersion": "2784976",
        "createdAt": "2024-06-07T21:27:21Z"
      },
      {
        "group": "numaflow.numaproj.io",
        "version": "v1alpha1",
        "kind": "Pipeline",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline",
        "uid": "0bdc8a5b-c46a-4ecf-a111-bf7e268cbc4a",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "PipelineRollout",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline",
            "uid": "c8639f83-4ab3-4f16-8995-fdd3574057f6"
          }
        ],
        "resourceVersion": "2785037",
        "createdAt": "2024-06-07T21:27:21Z"
      },
      {
        "group": "numaflow.numaproj.io",
        "version": "v1alpha1",
        "kind": "Vertex",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-cat",
        "uid": "391c92d7-7fbe-44d3-bef6-8917f8ba68f4",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Pipeline",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline",
            "uid": "0bdc8a5b-c46a-4ecf-a111-bf7e268cbc4a"
          }
        ],
        "resourceVersion": "2785071",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "numaflow.numaproj.io",
        "version": "v1alpha1",
        "kind": "Vertex",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-in",
        "uid": "1cf970e5-f1c7-4653-95e5-662841eb3d78",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Pipeline",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline",
            "uid": "0bdc8a5b-c46a-4ecf-a111-bf7e268cbc4a"
          }
        ],
        "resourceVersion": "2785049",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "numaflow.numaproj.io",
        "version": "v1alpha1",
        "kind": "Vertex",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline-out",
        "uid": "bdd7b0e3-39ec-4fd4-8284-1fb239141a8e",
        "parentRefs": [
          {
            "group": "numaflow.numaproj.io",
            "kind": "Pipeline",
            "namespace": "demo-numarollotus",
            "name": "my-pipeline",
            "uid": "0bdc8a5b-c46a-4ecf-a111-bf7e268cbc4a"
          }
        ],
        "resourceVersion": "2785019",
        "createdAt": "2024-06-07T21:27:25Z"
      },
      {
        "group": "numaplane.numaproj.io",
        "version": "v1alpha1",
        "kind": "ISBServiceRollout",
        "namespace": "demo-numarollotus",
        "name": "my-isbsvc",
        "uid": "dab68efe-de38-4bd5-be14-21de651fd858",
        "resourceVersion": "2785005",
        "createdAt": "2024-06-07T21:27:21Z"
      },
      {
        "group": "numaplane.numaproj.io",
        "version": "v1alpha1",
        "kind": "NumaflowControllerRollout",
        "namespace": "demo-numarollotus",
        "name": "my-numaflow-controller",
        "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2",
        "resourceVersion": "2784911",
        "createdAt": "2024-06-07T21:27:21Z"
      },
      {
        "group": "numaplane.numaproj.io",
        "version": "v1alpha1",
        "kind": "PipelineRollout",
        "namespace": "demo-numarollotus",
        "name": "my-pipeline",
        "uid": "c8639f83-4ab3-4f16-8995-fdd3574057f6",
        "resourceVersion": "2785064",
        "createdAt": "2024-06-07T21:27:21Z"
      },
      {
        "group": "rbac.authorization.k8s.io",
        "version": "v1",
        "kind": "Role",
        "namespace": "demo-numarollotus",
        "name": "numaflow-role",
        "uid": "db897d37-1e42-42fe-8887-bb9fd93f05c3",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784899",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "rbac.authorization.k8s.io",
        "version": "v1",
        "kind": "Role",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-role",
        "uid": "c708fe2c-1b5a-4397-b963-c4e0ac3f870d",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784898",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "rbac.authorization.k8s.io",
        "version": "v1",
        "kind": "Role",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-secrets-role",
        "uid": "fceec4f3-8d0d-48bc-9fac-8aeda12f5528",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784900",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "rbac.authorization.k8s.io",
        "version": "v1",
        "kind": "RoleBinding",
        "namespace": "demo-numarollotus",
        "name": "numaflow-role-binding",
        "uid": "6bf900e1-665a-495a-9132-b6596aac3485",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784905",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "rbac.authorization.k8s.io",
        "version": "v1",
        "kind": "RoleBinding",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-binding",
        "uid": "336295e5-ce0f-4df8-8326-da7c052295f2",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784906",
        "createdAt": "2024-06-07T21:27:22Z"
      },
      {
        "group": "rbac.authorization.k8s.io",
        "version": "v1",
        "kind": "RoleBinding",
        "namespace": "demo-numarollotus",
        "name": "numaflow-server-secrets-binding",
        "uid": "4aa37596-ad19-4889-8a50-46eddc635791",
        "parentRefs": [
          {
            "group": "numaplane.numaproj.io",
            "kind": "NumaflowControllerRollout",
            "namespace": "demo-numarollotus",
            "name": "my-numaflow-controller",
            "uid": "9d4742e7-c439-4a0d-a14d-c504f5713ae2"
          }
        ],
        "resourceVersion": "2784904",
        "createdAt": "2024-06-07T21:27:22Z"
      }
    ],
    "hosts": [
      {
        "name": "ip-10-74-48-143.us-west-2.compute.internal",
        "resourcesInfo": [
          {
            "resourceName": "cpu",
            "requestedByApp": 200,
            "requestedByNeighbors": 915,
            "capacity": 4000
          },
          {
            "resourceName": "memory",
            "requestedByApp": 268435456000,
            "requestedByNeighbors": 1101882624000,
            "capacity": 16393338880000
          }
        ],
        "systemInfo": {
          "machineID": "dc64f1760b55468991c7af8c1bba33d5",
          "systemUUID": "ec280584-80c4-a506-56d3-bcd4d64c3ce3",
          "bootID": "3a470ddf-55c8-47e4-b00c-4671f57537f2",
          "kernelVersion": "5.10.217-205.860.amzn2.x86_64",
          "osImage": "Amazon Linux 2",
          "containerRuntimeVersion": "containerd://1.7.11",
          "kubeletVersion": "v1.27.9-eks-5e0fdde",
          "kubeProxyVersion": "v1.27.9-eks-5e0fdde",
          "operatingSystem": "linux",
          "architecture": "amd64"
        }
      },
      {
        "name": "ip-10-74-50-219.us-west-2.compute.internal",
        "resourcesInfo": [
          {
            "resourceName": "cpu",
            "requestedByApp": 100,
            "requestedByNeighbors": 1075,
            "capacity": 4000
          },
          {
            "resourceName": "memory",
            "requestedByApp": 134217728000,
            "requestedByNeighbors": 1705862400000,
            "capacity": 16393338880000
          }
        ],
        "systemInfo": {
          "machineID": "dc64f1760b55468991c7af8c1bba33d5",
          "systemUUID": "ec217674-9632-5174-126b-4008cd2ff86e",
          "bootID": "4c611423-fee1-4e8d-9ca3-f1a67a1cf895",
          "kernelVersion": "5.10.217-205.860.amzn2.x86_64",
          "osImage": "Amazon Linux 2",
          "containerRuntimeVersion": "containerd://1.7.11",
          "kubeletVersion": "v1.27.9-eks-5e0fdde",
          "kubeProxyVersion": "v1.27.9-eks-5e0fdde",
          "operatingSystem": "linux",
          "architecture": "amd64"
        }
      },
      {
        "name": "ip-10-74-52-89.us-west-2.compute.internal",
        "resourcesInfo": [
          {
            "resourceName": "cpu",
            "requestedByApp": 200,
            "requestedByNeighbors": 865,
            "capacity": 4000
          },
          {
            "resourceName": "memory",
            "requestedByApp": 343932928000,
            "requestedByNeighbors": 1445815552000,
            "capacity": 16393338880000
          }
        ],
        "systemInfo": {
          "machineID": "dc64f1760b55468991c7af8c1bba33d5",
          "systemUUID": "ec20702b-439c-b87e-3fb0-c32b8d51e28f",
          "bootID": "6b520e8c-6746-4b14-b826-eed12103cadf",
          "kernelVersion": "5.10.217-205.860.amzn2.x86_64",
          "osImage": "Amazon Linux 2",
          "containerRuntimeVersion": "containerd://1.7.11",
          "kubeletVersion": "v1.27.9-eks-5e0fdde",
          "kubeProxyVersion": "v1.27.9-eks-5e0fdde",
          "operatingSystem": "linux",
          "architecture": "amd64"
        }
      }
    ]
  },
  "resource": {
    "apiVersion": "numaplane.numaproj.io/v1alpha1",
    "kind": "ISBServiceRollout",
    "metadata": {
      "annotations": {
        "kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"numaplane.numaproj.io/v1alpha1\",\"kind\":\"ISBServiceRollout\",\"metadata\":{\"annotations\":{},\"labels\":{\"app.kubernetes.io/instance\":\"numarollouts\"},\"name\":\"my-isbsvc\",\"namespace\":\"demo-numarollotus\"},\"spec\":{\"interStepBufferService\":{\"jetstream\":{\"persistence\":{\"volumeSize\":\"1Gi\"},\"version\":\"2.10.11\"}}}}\n"
      },
      "creationTimestamp": "2024-06-07T21:27:21Z",
      "finalizers": [
        "numaplane-controller"
      ],
      "generation": 1,
      "labels": {
        "app.kubernetes.io/instance": "numarollouts"
      },
      "managedFields": [
        {
          "apiVersion": "numaplane.numaproj.io/v1alpha1",
          "fieldsType": "FieldsV1",
          "fieldsV1": {
            "f:metadata": {
              "f:annotations": {
                ".": {},
                "f:kubectl.kubernetes.io/last-applied-configuration": {}
              },
              "f:labels": {
                ".": {},
                "f:app.kubernetes.io/instance": {}
              }
            },
            "f:spec": {
              ".": {},
              "f:interStepBufferService": {
                ".": {},
                "f:jetstream": {
                  ".": {},
                  "f:persistence": {
                    ".": {},
                    "f:volumeSize": {}
                  },
                  "f:version": {}
                }
              }
            }
          },
          "manager": "argocd-controller",
          "operation": "Update",
          "time": "2024-06-07T21:27:21Z"
        },
        {
          "apiVersion": "numaplane.numaproj.io/v1alpha1",
          "fieldsType": "FieldsV1",
          "fieldsV1": {
            "f:metadata": {
              "f:finalizers": {
                ".": {},
                "v:\"numaplane-controller\"": {}
              }
            }
          },
          "manager": "manager",
          "operation": "Update",
          "time": "2024-06-07T21:27:21Z"
        },
        {
          "apiVersion": "numaplane.numaproj.io/v1alpha1",
          "fieldsType": "FieldsV1",
          "fieldsV1": {
            "f:status": {
              ".": {},
              "f:conditions": {},
              "f:phase": {}
            }
          },
          "manager": "manager",
          "operation": "Update",
          "subresource": "status",
          "time": "2024-06-07T21:27:25Z"
        }
      ],
      "name": "my-isbsvc",
      "namespace": "demo-numarollotus",
      "resourceVersion": "2785005",
      "uid": "dab68efe-de38-4bd5-be14-21de651fd858"
    },
    "spec": {
      "interStepBufferService": {
        "jetstream": {
          "persistence": {
            "volumeSize": "1Gi"
          },
          "version": "2.10.11"
        }
      }
    },
    "status": {
      "conditions": [
        {
          "lastTransitionTime": "2024-06-07T21:27:25Z",
          "message": "Successful",
          "reason": "Successful",
          "status": "True",
          "type": "ChildResourcesHealthy"
        },
        {
          "lastTransitionTime": "2024-06-07T21:27:25Z",
          "message": "Successful",
          "reason": "Successful",
          "status": "True",
          "type": "Configured"
        }
      ],
      "phase": "Running"
    }
  },
  "application": {
    "apiVersion": "argoproj.io/v1alpha1",
    "kind": "Application",
    "spec": {
      "project": "default",
      "source": {
        "repoURL": "https://github.com/sarabala1979/numarollouts.git",
        "path": "manifests",
        "targetRevision": "HEAD"
      },
      "destination": {
        "server": "https://kubernetes.default.svc",
        "namespace": "demo-numarollotus"
      }
    },
    "status": {
      "resources": [
        {
          "group": "numaplane.numaproj.io",
          "version": "v1alpha1",
          "kind": "ISBServiceRollout",
          "namespace": "demo-numarollotus",
          "name": "my-isbsvc",
          "status": "Synced"
        },
        {
          "group": "numaplane.numaproj.io",
          "version": "v1alpha1",
          "kind": "NumaflowControllerRollout",
          "namespace": "demo-numarollotus",
          "name": "my-numaflow-controller",
          "status": "Synced"
        },
        {
          "group": "numaplane.numaproj.io",
          "version": "v1alpha1",
          "kind": "PipelineRollout",
          "namespace": "demo-numarollotus",
          "name": "my-pipeline",
          "status": "Synced"
        }
      ],
      "summary": {
        "images": [
          "docker.intuit.com/docker-rmt/nats:2.10.11",
          "docker.intuit.com/oss-analytics/dataflow/service/nats-server-config-reloader:0.7.0-patched",
          "docker.intuit.com/oss-analytics/dataflow/service/prometheus-nats-exporter:0.9.1",
          "docker.intuit.com/quay-rmt/numaproj/numaflow:v1.2.1"
        ]
      },
      "sync": {
        "status": "Synced",
        "comparedTo": {
          "source": {
            "repoURL": "https://github.com/sarabala1979/numarollouts.git",
            "path": "manifests",
            "targetRevision": "HEAD"
          },
          "destination": {
            "server": "https://kubernetes.default.svc",
            "namespace": "demo-numarollotus"
          }
        },
        "revision": "2a91416f6afdbd548c3554db9874d3f37c957f69"
      },
      "health": {
        "status": "Healthy"
      },
      "history": [
        {
          "revision": "2a91416f6afdbd548c3554db9874d3f37c957f69",
          "deployedAt": "2024-06-07T21:27:21Z",
          "id": 0,
          "source": {
            "repoURL": "https://github.com/sarabala1979/numarollouts.git",
            "path": "manifests",
            "targetRevision": "HEAD"
          },
          "deployStartedAt": "2024-06-07T21:27:21Z"
        }
      ],
      "reconciledAt": "2024-06-25T14:36:49Z",
      "operationState": {
        "operation": {
          "sync": {
            "revision": "2a91416f6afdbd548c3554db9874d3f37c957f69",
            "syncStrategy": {
              "hook": {}
            }
          },
          "initiatedBy": {
            "username": "admin"
          },
          "retry": {}
        },
        "phase": "Succeeded",
        "message": "successfully synced (all tasks run)",
        "syncResult": {
          "resources": [
            {
              "group": "numaplane.numaproj.io",
              "version": "v1alpha1",
              "kind": "ISBServiceRollout",
              "namespace": "demo-numarollotus",
              "name": "my-isbsvc",
              "status": "Synced",
              "message": "isbservicerollout.numaplane.numaproj.io/my-isbsvc created",
              "hookPhase": "Running",
              "syncPhase": "Sync"
            },
            {
              "group": "numaplane.numaproj.io",
              "version": "v1alpha1",
              "kind": "NumaflowControllerRollout",
              "namespace": "demo-numarollotus",
              "name": "my-numaflow-controller",
              "status": "Synced",
              "message": "numaflowcontrollerrollout.numaplane.numaproj.io/my-numaflow-controller created",
              "hookPhase": "Running",
              "syncPhase": "Sync"
            },
            {
              "group": "numaplane.numaproj.io",
              "version": "v1alpha1",
              "kind": "PipelineRollout",
              "namespace": "demo-numarollotus",
              "name": "my-pipeline",
              "status": "Synced",
              "message": "pipelinerollout.numaplane.numaproj.io/my-pipeline created",
              "hookPhase": "Running",
              "syncPhase": "Sync"
            }
          ],
          "revision": "2a91416f6afdbd548c3554db9874d3f37c957f69",
          "source": {
            "repoURL": "https://github.com/sarabala1979/numarollouts.git",
            "path": "manifests",
            "targetRevision": "HEAD"
          }
        },
        "startedAt": "2024-06-07T21:27:21Z",
        "finishedAt": "2024-06-07T21:27:21Z"
      },
      "sourceType": "Kustomize"
    },
    "metadata": {
      "name": "numarollouts",
      "namespace": "argocd",
      "uid": "cf3a628c-2e6b-4ab6-94c1-99543a338f7d",
      "resourceVersion": "18213014",
      "generation": 8518,
      "creationTimestamp": "2024-06-07T21:25:54Z",
      "managedFields": [
        {
          "manager": "argocd-server",
          "operation": "Update",
          "apiVersion": "argoproj.io/v1alpha1",
          "time": "2024-06-07T21:27:21Z",
          "fieldsType": "FieldsV1",
          "fieldsV1": {
            "f:spec": {
              ".": {},
              "f:destination": {
                ".": {},
                "f:namespace": {},
                "f:server": {}
              },
              "f:project": {},
              "f:source": {
                ".": {},
                "f:path": {},
                "f:repoURL": {},
                "f:targetRevision": {}
              }
            },
            "f:status": {
              ".": {},
              "f:health": {},
              "f:summary": {},
              "f:sync": {
                ".": {},
                "f:comparedTo": {
                  ".": {},
                  "f:destination": {},
                  "f:source": {}
                }
              }
            }
          }
        },
        {
          "manager": "argocd-application-controller",
          "operation": "Update",
          "apiVersion": "argoproj.io/v1alpha1",
          "time": "2024-06-25T14:36:49Z",
          "fieldsType": "FieldsV1",
          "fieldsV1": {
            "f:status": {
              "f:health": {
                "f:status": {}
              },
              "f:history": {},
              "f:operationState": {
                ".": {},
                "f:finishedAt": {},
                "f:message": {},
                "f:operation": {
                  ".": {},
                  "f:initiatedBy": {
                    ".": {},
                    "f:username": {}
                  },
                  "f:retry": {},
                  "f:sync": {
                    ".": {},
                    "f:revision": {},
                    "f:syncStrategy": {
                      ".": {},
                      "f:hook": {}
                    }
                  }
                },
                "f:phase": {},
                "f:startedAt": {},
                "f:syncResult": {
                  ".": {},
                  "f:resources": {},
                  "f:revision": {},
                  "f:source": {
                    ".": {},
                    "f:path": {},
                    "f:repoURL": {},
                    "f:targetRevision": {}
                  }
                }
              },
              "f:reconciledAt": {},
              "f:resources": {},
              "f:sourceType": {},
              "f:summary": {
                "f:images": {}
              },
              "f:sync": {
                "f:comparedTo": {
                  "f:destination": {
                    "f:namespace": {},
                    "f:server": {}
                  },
                  "f:source": {
                    "f:path": {},
                    "f:repoURL": {},
                    "f:targetRevision": {}
                  }
                },
                "f:revision": {},
                "f:status": {}
              }
            }
          }
        }
      ]
    }
  }
}