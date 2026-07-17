export type EventType =
  | "model_release"
  | "developer_tool_release"
  | "research_or_benchmark"
  | "official_product_release"
  | "security_incident";

export type LifecycleState =
  | "insufficient_data"
  | "detected"
  | "emerging"
  | "accelerating"
  | "established"
  | "cooling"
  | "dormant"
  | "noise";

export type StructureLabel =
  | "cross_platform_confirmed"
  | "adoption_confirmed"
  | "response_confirmed"
  | "platform_concentrated"
  | "coordination_risk"
  | "attention_behavior_gap"
  | "expected_behavior_lag"
  | "official_source_led"
  | "low_source_diversity"
  | "reactivated";

export type EvidenceState = "observed" | "missing" | "not_applicable" | "untrusted";

export interface Evidence {
  id: string;
  source: string;
  platform: string;
  title: string;
  url: string;
  publishedAt: string;
  kind: "discussion" | "behavior" | "official" | "research";
  excerpt: string;
  provenanceLevel?: "self_authenticating" | "provider_verified" | "unverified_discovery";
}

export interface MetricPoint {
  at: string;
  attention: number;
  behavior: number;
}

export interface RadarEvent {
  id: string;
  narrativeId?: string | null;
  narrativeTitle?: string | null;
  clusterVersion?: number;
  supersededBy?: string[];
  title: string;
  titleEn: string;
  eventType: EventType;
  classificationStatus?: "supported" | "unsupported";
  unsupportedReason?: string | null;
  state: LifecycleState;
  labels: StructureLabel[];
  attention: number;
  behavior: number;
  diversity: number;
  authority: number;
  coordinationRisk: number;
  coverage: number;
  uncertainty: number;
  evidenceStrength: "low" | "medium" | "high";
  discussionEvidenceState?: EvidenceState;
  behaviorEvidenceState?: EvidenceState;
  evidenceScore: number;
  velocity: number;
  gapResidual: number;
  firstSeen: string;
  updatedAt: string;
  independentSources: number;
  platforms: string[];
  signalFamilies?: Array<"discussion" | "behavior" | "official" | "research">;
  evidenceCount?: number;
  newEvidenceCount?: number;
  queuePriorityScore?: number;
  queuePriorityReasons?: string[];
  reviewAnchorAt?: string | null;
  driver: string;
  coverageNote: string;
  timeline: MetricPoint[];
  evidence: Evidence[];
}

export interface EventAssessment {
  eventId: string;
  evidenceMask: Record<string, EvidenceState>;
  observedFeatureWeight: number;
  expectedFeatureWeight: number;
  baselineMaturity: number;
  clusterConfidence: number;
  uncertainty: number;
  missingEvidence: Array<{ feature: string; state: EvidenceState; reason: string; impact: string }>;
  decisionReason: { summary: string; drivers: string[]; cautions: string[] };
}

export interface EventMember {
  id: string;
  title: string;
  source: string;
  platform: string;
  publishedAt: string;
}

export interface LineageEdge {
  operationId: string;
  parentEventId: string;
  childEventId: string;
  effectiveAt: string;
  revertedAt?: string | null;
}

export interface ClusterOperation {
  id: string;
  operation: "merge" | "split";
  status: "queued" | "running" | "completed" | "failed" | "reverted";
  resultEventIds?: string[];
  completedAt?: string;
  error?: string;
}

export interface EventLineage {
  eventId: string;
  clusterVersion: number;
  supersededBy: string[];
  parents: LineageEdge[];
  children: LineageEdge[];
  currentObservationCount: number;
  pendingOperations: ClusterOperation[];
}

export interface CoverageBudget {
  currency: "CNY";
  spent: number;
  limit: number;
  remaining: number;
  connectorLimits?: Array<{ scope: string; spent: number; limit: number; remaining: number; hardPaused: boolean }>;
  signalFamilyLimits?: Array<{ scope: string; spent: number; limit: number; remaining: number; hardPaused: boolean }>;
}

export interface ConnectorStatus {
  id: string;
  name: string;
  family: string;
  status: "healthy" | "degraded" | "paused";
  latencyMinutes: number;
  observations24h: number;
  coverage: number;
  lastSuccess: string;
  note: string;
  quotaUsed?: number | null;
  quotaLimit?: number | null;
  costRmbMonth?: number | null;
  rightsStatus?: "active" | "pending" | "experimental" | "blocked";
}

export interface RadarPayload {
  generatedAt: string;
  dataMode: "live" | "recorded_demo";
  window: string;
  events: RadarEvent[];
  connectors: ConnectorStatus[];
  totalEvents: number;
  limit: number;
  hasMore: boolean;
}

export type SourceStatus = "candidate" | "active" | "paused" | "blocked";

export interface SourceProfile {
  id: string;
  displayName: string;
  platform: string;
  language: "zh" | "en" | "other";
  status: SourceStatus;
  validObservations: number;
  earlyHits: number;
  confirmedHits: number;
  originality: number;
  domainFocus: number;
  authority: number;
  marketingMatrixOverlap: number;
  qualityCalibrated: boolean;
  discoveryReasons: string[];
  accountIds: string[];
  entityIds: string[];
  createdAt: string;
  firstObservedAt: string;
  lastObservedAt: string;
  activatedAt?: string | null;
  candidateScore: number | null;
  scoreEvidenceStatus: "eligible" | "insufficient";
  promotionEligible: boolean;
  rankEligible: boolean;
  blockedReasons: string[];
  historyDays: number;
  scoreVersion: string;
}

export interface SourceCatalogResponse {
  generatedAt: string;
  items: SourceProfile[];
  total: number;
  offset: number;
  limit: number;
  hasMore: boolean;
  counts: Record<SourceStatus, number>;
  active: number;
  activeCapacity: number;
  candidateCapacity: number;
  systemCapacity: number;
  sourceScorePolicy: {
    version: string;
    digest: string;
    status: string;
    frozenAt: string;
    timezone: string;
    rankingEnabled: boolean;
    autoPromotionEnabled: boolean;
    minimumValidObservations: number;
    minimumHistoryDays: number;
    minimumPromotionScore: number;
    dailyGrowthRate: number;
    dailyGrowthRounding: "floor";
    allowAutomaticBootstrap: boolean;
  };
}
