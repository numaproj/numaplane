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
  '.'?: Fvpcamazonawscompodips;
  'f:health': Fhealth;
  'f:summary': Fsummary;
  'f:sync': Fsync;
  'f:controllerNamespace'?: Fvpcamazonawscompodips;
  'f:history'?: Fvpcamazonawscompodips;
  'f:operationState'?: FoperationState;
  'f:reconciledAt'?: Fvpcamazonawscompodips;
  'f:resources'?: Fvpcamazonawscompodips;
  'f:sourceType'?: Fvpcamazonawscompodips;
}
interface FoperationState {
  '.': Fvpcamazonawscompodips;
  'f:finishedAt': Fvpcamazonawscompodips;
  'f:message': Fvpcamazonawscompodips;
  'f:operation': Foperation;
  'f:phase': Fvpcamazonawscompodips;
  'f:startedAt': Fvpcamazonawscompodips;
  'f:syncResult': FsyncResult;
}
interface FsyncResult {
  '.': Fvpcamazonawscompodips;
  'f:resources': Fvpcamazonawscompodips;
  'f:revision': Fvpcamazonawscompodips;
  'f:source': Fsource;
}
interface Foperation {
  '.': Fvpcamazonawscompodips;
  'f:initiatedBy': FinitiatedBy;
  'f:retry': Fvpcamazonawscompodips;
  'f:sync': Fsync2;
}
interface Fsync2 {
  '.': Fvpcamazonawscompodips;
  'f:revision': Fvpcamazonawscompodips;
  'f:syncStrategy': FsyncStrategy;
}
interface FsyncStrategy {
  '.': Fvpcamazonawscompodips;
  'f:hook': Fvpcamazonawscompodips;
}
interface FinitiatedBy {
  '.': Fvpcamazonawscompodips;
  'f:username': Fvpcamazonawscompodips;
}
interface Fsync {
  '.'?: Fvpcamazonawscompodips;
  'f:comparedTo': FcomparedTo;
  'f:revision'?: Fvpcamazonawscompodips;
  'f:status'?: Fvpcamazonawscompodips;
}
interface FcomparedTo {
  '.'?: Fvpcamazonawscompodips;
  'f:destination': Fdestination2;
  'f:source': Fsource2;
}
interface Fsource2 {
  'f:path'?: Fvpcamazonawscompodips;
  'f:repoURL'?: Fvpcamazonawscompodips;
  'f:targetRevision'?: Fvpcamazonawscompodips;
}
interface Fdestination2 {
  'f:namespace'?: Fvpcamazonawscompodips;
  'f:server'?: Fvpcamazonawscompodips;
}
interface Fsummary {
  'f:externalURLs'?: Fvpcamazonawscompodips;
  'f:images'?: Fvpcamazonawscompodips;
}
interface Fhealth {
  'f:status'?: Fvpcamazonawscompodips;
}
interface Fspec2 {
  '.': Fvpcamazonawscompodips;
  'f:destination': Fdestination;
  'f:project': Fvpcamazonawscompodips;
  'f:source': Fsource;
}
interface Fsource {
  '.': Fvpcamazonawscompodips;
  'f:path': Fvpcamazonawscompodips;
  'f:repoURL': Fvpcamazonawscompodips;
  'f:targetRevision': Fvpcamazonawscompodips;
}
interface Fdestination {
  '.': Fvpcamazonawscompodips;
  'f:namespace': Fvpcamazonawscompodips;
  'f:server': Fvpcamazonawscompodips;
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
  controllerNamespace: string;
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
  retry: Fvpcamazonawscompodips;
}
interface InitiatedBy {
  username: string;
}
interface Sync2 {
  revision: string;
  syncStrategy: SyncStrategy;
}
interface SyncStrategy {
  hook: Fvpcamazonawscompodips;
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
  externalURLs: string[];
  images: string[];
}
interface Resource2 {
  version: string;
  kind: string;
  namespace: string;
  name: string;
  status: string;
  health?: Health;
  group?: string;
  syncWave?: number;
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
interface Resource {
  apiVersion: string;
  kind: string;
  metadata: Metadata;
  spec: Spec;
  status: Status;
}
interface Status {
  conditions: Condition[];
  containerStatuses: ContainerStatus[];
  hostIP: string;
  phase: string;
  podIP: string;
  podIPs: PodIP[];
  qosClass: string;
  startTime: string;
}
interface PodIP {
  ip: string;
}
interface ContainerStatus {
  containerID: string;
  image: string;
  imageID: string;
  lastState: Fvpcamazonawscompodips;
  name: string;
  ready: boolean;
  restartCount: number;
  started: boolean;
  state: State;
}
interface State {
  running: Running;
}
interface Running {
  startedAt: string;
}
interface Condition {
  lastProbeTime?: any;
  lastTransitionTime: string;
  status: string;
  type: string;
}
interface Spec {
  containers: Container[];
  dnsPolicy: string;
  enableServiceLinks: boolean;
  nodeName: string;
  nodeSelector: NodeSelector;
  preemptionPolicy: string;
  priority: number;
  restartPolicy: string;
  schedulerName: string;
  securityContext: Fvpcamazonawscompodips;
  serviceAccount: string;
  serviceAccountName: string;
  terminationGracePeriodSeconds: number;
  tolerations: Toleration[];
  volumes: Volume[];
}
interface Volume {
  name: string;
  projected: Projected;
}
interface Projected {
  defaultMode: number;
  sources: Source[];
}
interface Source {
  serviceAccountToken?: ServiceAccountToken;
  configMap?: ConfigMap;
  downwardAPI?: DownwardAPI;
}
interface DownwardAPI {
  items: Item2[];
}
interface Item2 {
  fieldRef: FieldRef;
  path: string;
}
interface FieldRef {
  apiVersion: string;
  fieldPath: string;
}
interface ConfigMap {
  items: Item[];
  name: string;
}
interface Item {
  key: string;
  path: string;
}
interface ServiceAccountToken {
  expirationSeconds: number;
  path: string;
}
interface Toleration {
  effect?: string;
  key: string;
  operator?: string;
  tolerationSeconds?: number;
}
interface NodeSelector {
  'node.kubernetes.io/instancegroup': string;
}
interface Container {
  env: Info[];
  image: string;
  imagePullPolicy: string;
  name: string;
  resources: Resources;
  terminationMessagePath: string;
  terminationMessagePolicy: string;
  volumeMounts: VolumeMount[];
}
interface VolumeMount {
  mountPath: string;
  name: string;
  readOnly: boolean;
}
interface Resources {
  limits: Limits;
  requests: Requests;
}
interface Requests {
  cpu: string;
  'ephemeral-storage': string;
  memory: string;
}
interface Limits {
  'ephemeral-storage': string;
  memory: string;
}
interface Metadata {
  annotations: Annotations;
  creationTimestamp: string;
  generateName: string;
  labels: Labels2;
  managedFields: ManagedField[];
  name: string;
  namespace: string;
  ownerReferences: OwnerReference[];
  resourceVersion: string;
  uid: string;
}
interface OwnerReference {
  apiVersion: string;
  blockOwnerDeletion: boolean;
  controller: boolean;
  kind: string;
  name: string;
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
  'f:conditions': Fconditions;
  'f:containerStatuses': Fvpcamazonawscompodips;
  'f:hostIP': Fvpcamazonawscompodips;
  'f:phase': Fvpcamazonawscompodips;
  'f:podIP': Fvpcamazonawscompodips;
  'f:podIPs': FpodIPs;
  'f:startTime': Fvpcamazonawscompodips;
}
interface FpodIPs {
  '.': Fvpcamazonawscompodips;
  'k:{"ip":"10.195.40.168"}': Kip1019540168;
}
interface Kip1019540168 {
  '.': Fvpcamazonawscompodips;
  'f:ip': Fvpcamazonawscompodips;
}
interface Fconditions {
  'k:{"type":"ContainersReady"}': KtypeContainersReady;
  'k:{"type":"Initialized"}': KtypeContainersReady;
  'k:{"type":"Ready"}': KtypeContainersReady;
}
interface KtypeContainersReady {
  '.': Fvpcamazonawscompodips;
  'f:lastProbeTime': Fvpcamazonawscompodips;
  'f:lastTransitionTime': Fvpcamazonawscompodips;
  'f:status': Fvpcamazonawscompodips;
  'f:type': Fvpcamazonawscompodips;
}
interface Fspec {
  'f:containers': Fcontainers;
  'f:dnsPolicy': Fvpcamazonawscompodips;
  'f:enableServiceLinks': Fvpcamazonawscompodips;
  'f:restartPolicy': Fvpcamazonawscompodips;
  'f:schedulerName': Fvpcamazonawscompodips;
  'f:securityContext': Fvpcamazonawscompodips;
  'f:terminationGracePeriodSeconds': Fvpcamazonawscompodips;
}
interface Fcontainers {
  'k:{"name":"app"}': Knameapp;
}
interface Knameapp {
  '.': Fvpcamazonawscompodips;
  'f:env': Fenv;
  'f:image': Fvpcamazonawscompodips;
  'f:imagePullPolicy': Fvpcamazonawscompodips;
  'f:name': Fvpcamazonawscompodips;
  'f:resources': Fvpcamazonawscompodips;
  'f:terminationMessagePath': Fvpcamazonawscompodips;
  'f:terminationMessagePolicy': Fvpcamazonawscompodips;
}
interface Fenv {
  '.': Fvpcamazonawscompodips;
  'k:{"name":"SPRING_PROFILE"}': KnameSPRINGPROFILE;
}
interface KnameSPRINGPROFILE {
  '.': Fvpcamazonawscompodips;
  'f:name': Fvpcamazonawscompodips;
  'f:value': Fvpcamazonawscompodips;
}
interface Fmetadata {
  'f:annotations': Fannotations;
  'f:generateName'?: Fvpcamazonawscompodips;
  'f:labels'?: Flabels;
  'f:ownerReferences'?: FownerReferences;
}
interface FownerReferences {
  '.': Fvpcamazonawscompodips;
  'k:{"uid":"75fbf6e3-bfee-4160-99e2-f25ca936c90b"}': Fvpcamazonawscompodips;
}
interface Flabels {
  '.': Fvpcamazonawscompodips;
  'f:app': Fvpcamazonawscompodips;
  'f:applications.argoproj.io/app-name': Fvpcamazonawscompodips;
  'f:pod-template-hash': Fvpcamazonawscompodips;
}
interface Fannotations {
  'f:vpc.amazonaws.com/pod-ips'?: Fvpcamazonawscompodips;
  '.'?: Fvpcamazonawscompodips;
  'f:iam.amazonaws.com/role'?: Fvpcamazonawscompodips;
}
interface Fvpcamazonawscompodips {
}
interface Labels2 {
  app: string;
  'applications.argoproj.io/app-name': string;
  assetId: string;
  azId: string;
  'pod-template-hash': string;
}
interface Annotations {
  'iam.amazonaws.com/role': string;
  'kubernetes.io/limit-ranger': string;
  'vpc.amazonaws.com/pod-ips': string;
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
interface Node {
  version: string;
  kind: string;
  namespace: string;
  name: string;
  uid: string;
  parentRefs?: ParentRef[];
  resourceVersion: string;
  createdAt: string;
  info?: Info[];
  networkingInfo?: NetworkingInfo;
  images?: string[];
  health?: Health;
  group?: string;
}
interface Health {
  status: string;
  message?: string;
}
interface NetworkingInfo {
  labels?: Labels;
  targetLabels?: TargetLabels;
  targetRefs?: TargetRef[];
  ingress?: Ingress[];
  externalURLs?: string[];
}
interface Ingress {
  hostname: string;
}
interface TargetRef {
  kind: string;
  namespace: string;
  name: string;
}
interface TargetLabels {
  app: string;
  'rollouts-pod-template-hash'?: string;
}
interface Labels {
  app: string;
  assetAlias?: string;
  assetId: string;
  azId: string;
  env?: string;
  'istio-injected'?: string;
  l1?: string;
  l2?: string;
  policyId?: string;
  role?: string;
  'rollouts-pod-template-hash'?: string;
  'scaleops.sh/pod-owner-identifier'?: string;
  'security.istio.io/tlsMode'?: string;
  'service.istio.io/canonical-name'?: string;
  'service.istio.io/canonical-revision'?: string;
  'applications.argoproj.io/app-name'?: string;
  'pod-template-hash'?: string;
}
interface Info {
  name: string;
  value: string;
}
interface ParentRef {
  kind: string;
  namespace: string;
  name: string;
  uid: string;
  group?: string;
}