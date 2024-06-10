export interface ArgoPropType {
  tree: Tree;
  resource: Resource;
  application: Application;
}
interface Application {
  apiVersion: string;
  kind: string;
  spec: Spec2;
  status: Status2;
  metadata: Metadata2;
}
interface Metadata2 {
  name: string;
  namespace: string;
  uid: string;
  resourceVersion: string;
  generation: number;
  creationTimestamp: string;
  managedFields: ManagedField2[];
}
interface ManagedField2 {
  manager: string;
  operation: string;
  apiVersion: string;
  time: string;
  fieldsType: string;
  fieldsV1: FieldsV12;
}
interface FieldsV12 {
  'f:spec'?: Fspec2;
  'f:status': Fstatus2;
}
interface Fstatus2 {
  '.'?: _;
  'f:health': Fhealth;
  'f:summary': Fsummary;
  'f:sync': Fsync;
  'f:history'?: _;
  'f:operationState'?: FoperationState;
  'f:reconciledAt'?: _;
  'f:resources'?: _;
  'f:sourceType'?: _;
}
interface FoperationState {
  '.': _;
  'f:finishedAt': _;
  'f:message': _;
  'f:operation': Foperation;
  'f:phase': _;
  'f:startedAt': _;
  'f:syncResult': FsyncResult;
}
interface FsyncResult {
  '.': _;
  'f:resources': _;
  'f:revision': _;
  'f:source': Fsource;
}
interface Foperation {
  '.': _;
  'f:initiatedBy': FinitiatedBy;
  'f:retry': _;
  'f:sync': Fsync2;
}
interface Fsync2 {
  '.': _;
  'f:revision': _;
  'f:syncStrategy': FsyncStrategy;
}
interface FsyncStrategy {
  '.': _;
  'f:hook': _;
}
interface FinitiatedBy {
  '.': _;
  'f:username': _;
}
interface Fsync {
  '.'?: _;
  'f:comparedTo': FcomparedTo;
  'f:revision'?: _;
  'f:status'?: _;
}
interface FcomparedTo {
  '.'?: _;
  'f:destination': Fdestination2;
  'f:source': Fsource2;
}
interface Fsource2 {
  'f:path'?: _;
  'f:repoURL'?: _;
  'f:targetRevision'?: _;
}
interface Fdestination2 {
  'f:namespace'?: _;
  'f:server'?: _;
}
interface Fsummary {
  'f:images'?: _;
}
interface Fhealth {
  'f:status'?: _;
}
interface Fspec2 {
  '.': _;
  'f:destination': Fdestination;
  'f:project': _;
  'f:source': Fsource;
}
interface Fsource {
  '.': _;
  'f:path': _;
  'f:repoURL': _;
  'f:targetRevision': _;
}
interface Fdestination {
  '.': _;
  'f:namespace': _;
  'f:server': _;
}
interface Status2 {
  resources: Resource2[];
  summary: Summary;
  sync: Sync;
  health: Health2;
  history: History[];
  reconciledAt: string;
  operationState: OperationState;
  sourceType: string;
}
interface OperationState {
  operation: Operation;
  phase: string;
  message: string;
  syncResult: SyncResult;
  startedAt: string;
  finishedAt: string;
}
interface SyncResult {
  resources: Resource3[];
  revision: string;
  source: Source2;
}
interface Resource3 {
  group: string;
  version: string;
  kind: string;
  namespace: string;
  name: string;
  status: string;
  message: string;
  hookPhase: string;
  syncPhase: string;
}
interface Operation {
  sync: Sync2;
  initiatedBy: InitiatedBy;
  retry: _;
}
interface InitiatedBy {
  username: string;
}
interface Sync2 {
  revision: string;
  syncStrategy: SyncStrategy;
}
interface SyncStrategy {
  hook: _;
}
interface History {
  revision: string;
  deployedAt: string;
  id: number;
  source: Source2;
  deployStartedAt: string;
}
interface Health2 {
  status: string;
}
interface Sync {
  status: string;
  comparedTo: ComparedTo;
  revision: string;
}
interface ComparedTo {
  source: Source2;
  destination: Destination;
}
interface Summary {
  images: string[];
}
interface Resource2 {
  group: string;
  version: string;
  kind: string;
  namespace: string;
  name: string;
  status: string;
}
interface Spec2 {
  project: string;
  source: Source2;
  destination: Destination;
}
interface Destination {
  server: string;
  namespace: string;
}
interface Source2 {
  repoURL: string;
  path: string;
  targetRevision: string;
}
export interface Resource {
  apiVersion: string;
  kind: string;
  metadata: Metadata;
  spec: Spec;
  status: Status;
}
interface Status {
  conditions: Condition[];
  phase: string;
}
interface Condition {
  lastTransitionTime: string;
  message: string;
  reason: string;
  status: string;
  type: string;
}
interface Spec {
  pipeline: Pipeline;
}
interface Pipeline {
  edges: Edge[];
  interStepBufferServiceName: string;
  vertices: Vertex[];
}
interface Vertex {
  name: string;
  source?: Source;
  udf?: Udf;
  sink?: Sink;
}
interface Sink {
  log: _;
}
interface Udf {
  builtin: Builtin;
}
interface Builtin {
  name: string;
}
interface Source {
  generator: Generator;
}
interface Generator {
  duration: string;
  rpu: number;
}
interface Edge {
  from: string;
  to: string;
}
interface Metadata {
  annotations: Annotations;
  creationTimestamp: string;
  finalizers: string[];
  generation: number;
  labels: Labels2;
  managedFields: ManagedField[];
  name: string;
  namespace: string;
  resourceVersion: string;
  uid: string;
}
interface ManagedField {
  apiVersion: string;
  fieldsType: string;
  fieldsV1: FieldsV1;
  manager: string;
  operation: string;
  time: string;
  subresource?: string;
}
interface FieldsV1 {
  'f:metadata'?: Fmetadata;
  'f:spec'?: Fspec;
  'f:status'?: Fstatus;
}
interface Fstatus {
  '.': _;
  'f:conditions': _;
  'f:phase': _;
}
interface Fspec {
  '.': _;
  'f:pipeline': Fpipeline;
}
interface Fpipeline {
  '.': _;
  'f:edges': _;
  'f:interStepBufferServiceName': _;
  'f:vertices': _;
}
interface Fmetadata {
  'f:annotations'?: Fannotations;
  'f:labels'?: Flabels;
  'f:finalizers'?: Ffinalizers;
}
interface Ffinalizers {
  '.': _;
  'v:"numaplane-controller"': _;
}
interface Flabels {
  '.': _;
  'f:app.kubernetes.io/instance': _;
}
interface Fannotations {
  '.': _;
  'f:kubectl.kubernetes.io/last-applied-configuration': _;
}
interface _ {
}
interface Labels2 {
  'app.kubernetes.io/instance': string;
}
interface Annotations {
  'kubectl.kubernetes.io/last-applied-configuration': string;
}
interface Tree {
  nodes: Node[];
  hosts: Host[];
}
interface Host {
  name: string;
  resourcesInfo: ResourcesInfo[];
  systemInfo: SystemInfo;
}
interface SystemInfo {
  machineID: string;
  systemUUID: string;
  bootID: string;
  kernelVersion: string;
  osImage: string;
  containerRuntimeVersion: string;
  kubeletVersion: string;
  kubeProxyVersion: string;
  operatingSystem: string;
  architecture: string;
}
interface ResourcesInfo {
  resourceName: string;
  requestedByApp: number;
  requestedByNeighbors: number;
  capacity: number;
}
export interface Node {
  version: string;
  kind: string;
  namespace: string;
  name: string;
  uid: string;
  parentRefs?: ParentRef[];
  resourceVersion: string;
  createdAt: string;
  health?: Health;
  info?: Info[];
  networkingInfo?: NetworkingInfo;
  images?: string[];
  group?: string;
}
interface NetworkingInfo {
  labels?: Labels;
  targetLabels?: TargetLabels;
}
interface TargetLabels {
  'app.kubernetes.io/component': string;
  'app.kubernetes.io/managed-by': string;
  'app.kubernetes.io/part-of': string;
  'numaflow.numaproj.io/isbsvc-name'?: string;
  'numaflow.numaproj.io/isbsvc-type'?: string;
  'numaflow.numaproj.io/pipeline-name'?: string;
  'numaflow.numaproj.io/vertex-name'?: string;
}
interface Labels {
  'app.kubernetes.io/component': string;
  'app.kubernetes.io/managed-by'?: string;
  'app.kubernetes.io/part-of': string;
  azId: string;
  'controller-revision-hash'?: string;
  'numaflow.numaproj.io/isbsvc-name'?: string;
  'numaflow.numaproj.io/isbsvc-type'?: string;
  'statefulset.kubernetes.io/pod-name'?: string;
  'app.kubernetes.io/name'?: string;
  'numaflow.numaproj.io/pipeline-name'?: string;
  'numaflow.numaproj.io/vertex-name'?: string;
  'pod-template-hash'?: string;
}
interface Info {
  name: string;
  value: string;
}
interface Health {
  status: string;
  message?: string;
}
interface ParentRef {
  group?: string;
  kind: string;
  namespace: string;
  name: string;
  uid: string;
}