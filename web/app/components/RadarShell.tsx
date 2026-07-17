"use client";

import * as Dialog from "@radix-ui/react-dialog";
import * as Tooltip from "@radix-ui/react-tooltip";
import { Activity, Bell, BookOpenCheck, ChevronRight, CircleGauge, Database, Filter, GitBranch, LayoutList, Menu, Radar, Search, Send, ShieldCheck, UsersRound, X } from "lucide-react";
import type { FormEvent } from "react";
import { lazy, Suspense, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";
import { demoPayload } from "../lib/demo-data";
import type { ConnectorStatus, CoverageBudget, EventAssessment, EventLineage, EventMember, LifecycleState, RadarEvent, RadarPayload, SourceCatalogResponse, SourceStatus, StructureLabel } from "../lib/types";
import { QueueTable } from "./QueueTable";

const RadarChart = lazy(() => import("./RadarChart").then((module) => ({ default: module.RadarChart })));

type View = "queue" | "radar" | "sources" | "coverage" | "method";
type Filters = { state: "all" | LifecycleState; eventType: "all" | RadarEvent["eventType"]; evidence: "all" | RadarEvent["evidenceStrength"] };
type InteractionPayload = {
  kind: "detail_opened" | "evidence_opened" | "triage_submitted" | "review_segment_closed" | "review_heartbeat" | "watch_toggled";
  idempotencyKey: string;
  sessionId: string;
  eventId: string;
  metadata: Record<string, string | number | boolean>;
};
const interactionOutboxKey = "signal-ai-interaction-outbox-v1";

function readInteractionOutbox(): InteractionPayload[] {
  try {
    const value = JSON.parse(window.localStorage.getItem(interactionOutboxKey) ?? "[]");
    return Array.isArray(value) ? value : [];
  } catch {
    return [];
  }
}

function writeInteractionOutbox(rows: InteractionPayload[]) {
  window.localStorage.setItem(interactionOutboxKey, JSON.stringify(rows));
}

async function flushInteractionOutbox(base: string) {
  for (const payload of readInteractionOutbox()) {
    const response = await fetch(`${base}/api/v1/interactions`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload),
    });
    if (!response.ok) throw new Error(`interaction persistence failed (${response.status})`);
    writeInteractionOutbox(readInteractionOutbox().filter((row) => row.idempotencyKey !== payload.idempotencyKey));
  }
}
const windowName: Record<string, string> = { "1H": "一小时", "6H": "六小时", "24H": "二十四小时", "7D": "七天" };

const stateName: Record<LifecycleState, string> = {
  insufficient_data: "数据不足", detected: "已发现", emerging: "萌发", accelerating: "加速",
  established: "已建立", cooling: "降温", dormant: "休眠", noise: "噪声",
};
const labelName: Record<StructureLabel, string> = {
  cross_platform_confirmed: "覆盖生态内跨平台确认", adoption_confirmed: "采用已确认", platform_concentrated: "单平台集中",
  response_confirmed: "修复响应已确认",
  coordination_risk: "协同发布风险", attention_behavior_gap: "讨论 / 行为剪刀差", expected_behavior_lag: "预期行为滞后",
  official_source_led: "官方信源首发", low_source_diversity: "信源多样性低", reactivated: "再次活跃",
};
const timeAgo = (iso: string) => {
  const minutes = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 60_000));
  if (minutes < 1) return "刚刚";
  if (minutes < 60) return `${minutes} 分钟前`;
  return `${Math.round(minutes / 60)} 小时前`;
};
const discussionObserved = (event: RadarEvent) => event.discussionEvidenceState === undefined
  ? event.evidence.some((item) => item.kind === "discussion")
  : event.discussionEvidenceState === "observed";
const behaviorObserved = (event: RadarEvent) => event.behaviorEvidenceState === undefined
  ? event.evidence.some((item) => item.kind === "behavior")
  : event.behaviorEvidenceState === "observed";

const lagBoundary: Record<RadarEvent["eventType"], string> = {
  model_release: "模型发布的下载、衍生项目和集成通常在 0–24 小时内出现；缺失时只降低采用证据，不直接否定事件。",
  developer_tool_release: "开发工具的安装、仓库活动和 Issue 通常在 0–24 小时内出现。",
  research_or_benchmark: "研究复现和引用常滞后 24 小时至 7 天，早期不得按营销剪刀差直接裁决。",
  official_product_release: "产品采用信号依赖可合法取得的活跃或集成指标；无行为源时只判断注意力。",
  security_incident: "安全事件的修复版本、Issue 和缓解采用可能在 0–48 小时内出现。",
};

function Score({ label, value, tone = "blue" }: { label: string; value: number | null; tone?: "blue" | "green" | "amber" | "red" }) {
  return <div className="score"><span>{label}</span><b className={`tone-${tone}`}>{value === null ? "N/A" : value}</b><i><em className={`tone-bg-${tone}`} style={{ width: `${value ?? 0}%` }} /></i></div>;
}

function ClusterEditDialog({ event, peers }: { event: RadarEvent; peers: RadarEvent[] }) {
  const [open, setOpen] = useState(false);
  const [mode, setMode] = useState<"merge" | "split">("merge");
  const [status, setStatus] = useState<"idle" | "saving" | "queued" | "error">("idle");
  const [members, setMembers] = useState<EventMember[]>([]);
  const [memberStatus, setMemberStatus] = useState<"idle" | "loading" | "ready" | "error">("idle");
  useEffect(() => {
    if (!open || mode !== "split") return;
    const controller = new AbortController();
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    void fetch(`${base}/api/v1/events/${encodeURIComponent(event.id)}/members`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("members unavailable")))
      .then((data: { items: EventMember[] }) => { setMembers(data.items); setMemberStatus("ready"); })
      .catch(() => { if (!controller.signal.aborted) setMemberStatus("error"); });
    return () => controller.abort();
  }, [event.id, mode, open]);
  const submit = async (formEvent: FormEvent<HTMLFormElement>) => {
    formEvent.preventDefault();
    setStatus("saving");
    const form = new FormData(formEvent.currentTarget);
    const payload = mode === "merge"
      ? { targetEventId: String(form.get("targetEventId") || ""), reason: String(form.get("reason") || "") }
      : { observationIds: form.getAll("observationIds").map(String), reason: String(form.get("reason") || "") };
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    try {
      const response = await fetch(`${base}/api/v1/events/${encodeURIComponent(event.id)}/${mode}`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
      if (!response.ok) throw new Error("cluster edit rejected");
      setStatus("queued");
    } catch {
      setStatus("error");
    }
  };
  return <Dialog.Root open={open} onOpenChange={(value) => { setOpen(value); if (!value) setStatus("idle"); else if (mode === "split") setMemberStatus("loading"); }}>
    <Dialog.Trigger asChild><button><GitBranch size={13} /> 合并 / 拆分</button></Dialog.Trigger>
    <Dialog.Portal><Dialog.Overlay className="dialog-overlay" /><Dialog.Content className="dialog-content">
      <Dialog.Title>修订事件聚类</Dialog.Title>
      <Dialog.Description>提交后进入可审计异步队列；原事件和历史评分不会被覆盖。</Dialog.Description>
      <form onSubmit={submit}>
        <label>操作<select value={mode} onChange={(input) => { const nextMode = input.target.value as "merge" | "split"; setMode(nextMode); setMemberStatus(nextMode === "split" ? "loading" : "idle"); setStatus("idle"); }}><option value="merge">合并到新事件</option><option value="split">拆出所选证据</option></select></label>
        {mode === "merge" ? <label>目标事件<select name="targetEventId" required><option value="">请选择</option>{peers.filter((peer) => peer.id !== event.id).map((peer) => <option value={peer.id} key={peer.id}>{peer.title}</option>)}</select></label>
          : <label>拆分成员<select name="observationIds" multiple required disabled={memberStatus !== "ready" || members.length < 2} size={Math.min(8, Math.max(3, members.length))}>{members.map((item) => <option value={item.id} key={item.id}>{item.title} · {item.platform} · {item.source}</option>)}</select><small>{memberStatus === "loading" ? "正在读取事件成员…" : memberStatus === "error" ? "成员读取失败，未允许提交。" : members.length < 2 ? "至少需要两个真实 Observation 成员才能拆分。" : "按住 Ctrl / Command 可多选；提交的是完整 Observation ID，执行前会再次校验。"}</small></label>}
        <label>修订依据<textarea name="reason" minLength={3} maxLength={2000} required placeholder="说明共同实体、版本冲突、官方 URL 或误归并证据" /></label>
        {status === "queued" && <p className="form-status success" role="status">命令已排队；执行后谱系会生成新 Event ID。</p>}
        {status === "error" && <p className="form-status error" role="alert">提交失败；请检查成员、权限或事件版本。</p>}
        <div className="dialog-actions"><Dialog.Close asChild><button type="button" className="button-secondary">关闭</button></Dialog.Close><button className="button-primary" type="submit" disabled={status === "saving"}>{status === "saving" ? "提交中…" : "提交修订"}</button></div>
      </form>
      <Dialog.Close className="dialog-close" aria-label="关闭"><X size={17} /></Dialog.Close>
    </Dialog.Content></Dialog.Portal>
  </Dialog.Root>;
}

type DecisionContext = { queueEligibilityKey: string | null; alertDeliveryKey: string | null; capturedAt: string };

function DetailPanel({ event, peers, assessment, decisionContext, lineage, windowSize, watched, onWatchChange, onLineageRefresh, onInteraction, onClose }: { event: RadarEvent; peers: RadarEvent[]; assessment: EventAssessment | null; decisionContext: DecisionContext | null; lineage: EventLineage | null; windowSize: string; watched: boolean; onWatchChange: (eventId: string, watched: boolean) => void; onLineageRefresh: () => void; onInteraction: (kind: "detail_opened" | "evidence_opened" | "triage_submitted" | "review_segment_closed" | "review_heartbeat" | "watch_toggled", eventId: string, metadata?: Record<string, string | number | boolean>) => Promise<void>; onClose: () => void }) {
  const latest = event.timeline[event.timeline.length - 1] ?? { attention: event.attention, behavior: event.behavior };
  const visibleVerifiedEvidence = event.evidence.filter((item) => item.provenanceLevel !== "unverified_discovery");
  const unverifiedEvidenceCount = event.evidence.length - visibleVerifiedEvidence.length;
  const totalVerifiedEvidence = Math.max(event.evidenceCount ?? 0, visibleVerifiedEvidence.length);
  const hasDiscussion = assessment ? assessment.evidenceMask.discussion === "observed" : discussionObserved(event);
  const hasBehavior = assessment ? assessment.evidenceMask.behavior === "observed" : behaviorObserved(event);
  const counterSignals = [
    event.labels.includes("attention_behavior_gap") ? `行为趋势未跟上讨论，残差 ${event.gapResidual > 0 ? "+" : ""}${event.gapResidual}` : null,
    event.labels.includes("platform_concentrated") ? "增长主要集中在单一平台，尚不能视为覆盖生态内扩散" : null,
    event.labels.includes("coordination_risk") ? `协同发布风险 ${event.coordinationRisk}/100，独立讨论者可能被高估` : null,
    event.labels.includes("low_source_diversity") ? `独立信源仅 ${event.independentSources} 个，来源多样性不足` : null,
    ...((assessment?.missingEvidence ?? []).map((gap) => `${gap.reason}；${gap.impact}`)),
  ].filter((item): item is string => Boolean(item));
  const [actionStatus, setActionStatus] = useState("");
  const [watchNote, setWatchNote] = useState("从研判详情关注");
  const [waitingExternal, setWaitingExternal] = useState(false);
  const timing = useRef({ lastTick: 0, lastActivity: 0, activeSeconds: 0, externalWaitSeconds: 0, waitingExternal: false, segmentId: "", sequence: 0 });
  const openInteractionReady = useRef<Promise<boolean>>(Promise.resolve(true));
  const reviewAttemptId = useRef("");
  const submittedAttemptId = useRef("");
  const collectReviewTiming = useCallback(() => {
    const now = window.performance.now();
    const previous = timing.current.lastTick || now;
    // Cap a single tick so suspended tabs and sleeping devices cannot inflate
    // foreground work when the browser resumes. Explicit external wait is wall
    // time, so it keeps accruing while the analyst is in another app/tab.
    const rawElapsedSeconds = Math.max(0, (now - previous) / 1000);
    const foregroundElapsedSeconds = Math.min(5, rawElapsedSeconds);
    if (timing.current.waitingExternal) timing.current.externalWaitSeconds += rawElapsedSeconds;
    else if (document.visibilityState === "visible" && document.hasFocus() && now - timing.current.lastActivity <= 60_000) {
      timing.current.activeSeconds += foregroundElapsedSeconds;
    }
    timing.current.lastTick = now;
    return {
      activeSeconds: Math.round(timing.current.activeSeconds * 10) / 10,
      externalWaitSeconds: Math.round(timing.current.externalWaitSeconds * 10) / 10,
    };
  }, []);

  const sendReviewHeartbeat = useCallback(async () => {
    const queueEligibilityKey = decisionContext?.queueEligibilityKey;
    const attemptId = reviewAttemptId.current;
    const current = timing.current;
    if (!queueEligibilityKey || !attemptId || !current.segmentId) return;
    collectReviewTiming();
    const now = window.performance.now();
    const state = current.waitingExternal
      ? "external_wait"
      : document.visibilityState === "visible" && document.hasFocus() && now - current.lastActivity <= 60_000
        ? "active" : "idle";
    const sequence = current.sequence++;
    await onInteraction("review_heartbeat", event.id, {
      reviewAttemptId: attemptId, queueEligibilityKey, segmentId: current.segmentId,
      sequence, state, measurementVersion: "server-heartbeat-v2", idleTimeoutSeconds: 60, tickCapSeconds: 5,
    });
  }, [collectReviewTiming, decisionContext?.queueEligibilityKey, event.id, onInteraction]);

  useEffect(() => {
    const queueEligibilityKey = decisionContext?.queueEligibilityKey;
    if (!queueEligibilityKey) {
      openInteractionReady.current = Promise.resolve(false);
      return;
    }
    const now = window.performance.now();
    const segmentId = window.crypto.randomUUID();
    timing.current = { lastTick: now, lastActivity: now, activeSeconds: 0, externalWaitSeconds: 0, waitingExternal: false, segmentId, sequence: 0 };
    const attemptId = window.crypto.randomUUID();
    reviewAttemptId.current = attemptId;
    submittedAttemptId.current = "";
    openInteractionReady.current = onInteraction("detail_opened", event.id, {
      window: windowSize, reviewAttemptId: attemptId, queueEligibilityKey, segmentId,
      measurementVersion: "server-heartbeat-v2",
    })
      .then(async () => { await sendReviewHeartbeat(); return true; })
      .catch(() => false);
    const markActivity = () => {
      collectReviewTiming();
      timing.current.lastActivity = window.performance.now();
    };
    const accountTransition = () => collectReviewTiming();
    const interval = window.setInterval(() => { void sendReviewHeartbeat().catch(() => undefined); }, 5000);
    window.addEventListener("pointerdown", markActivity, { passive: true });
    window.addEventListener("keydown", markActivity);
    window.addEventListener("focus", accountTransition);
    window.addEventListener("blur", accountTransition);
    document.addEventListener("visibilitychange", accountTransition);
    return () => {
      collectReviewTiming();
      if (submittedAttemptId.current !== attemptId) {
        void sendReviewHeartbeat().catch(() => undefined).finally(() => onInteraction("review_segment_closed", event.id, {
          reviewAttemptId: attemptId, queueEligibilityKey, segmentId,
          measurementVersion: "server-heartbeat-v2", idleTimeoutSeconds: 60, tickCapSeconds: 5,
        }).catch(() => undefined));
      }
      window.clearInterval(interval);
      window.removeEventListener("pointerdown", markActivity);
      window.removeEventListener("keydown", markActivity);
      window.removeEventListener("focus", accountTransition);
      window.removeEventListener("blur", accountTransition);
      document.removeEventListener("visibilitychange", accountTransition);
    };
  }, [collectReviewTiming, decisionContext?.queueEligibilityKey, event.id, onInteraction, sendReviewHeartbeat, windowSize]);

  const toggleExternalWait = () => {
    collectReviewTiming();
    timing.current.waitingExternal = !timing.current.waitingExternal;
    timing.current.lastActivity = window.performance.now();
    setWaitingExternal(timing.current.waitingExternal);
  };

  const submitAction = async (action: "watch" | "confirm" | "reject" | "observe") => {
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    const endpoint = action === "watch" ? "/api/v1/watchlists" : "/api/v1/feedback";
    const body = action === "watch"
      ? { eventId: event.id, note: watchNote }
      : {
          eventId: event.id, action,
          queueEligibilityKey: decisionContext?.queueEligibilityKey,
          alertDeliveryKey: decisionContext?.alertDeliveryKey,
          reason: action === "confirm" ? "分析师接受当前研判" : action === "reject" ? "分析师拒绝当前研判" : "证据尚未闭合，继续观察",
        };
    try {
      if (action !== "watch" && !decisionContext?.queueEligibilityKey) {
        setActionStatus("提交失败，详情缺少队列版本；请刷新后重试");
        return;
      }
      const removingWatch = action === "watch" && watched;
      // Freeze analyst foreground time at the decision boundary. Network latency
      // while the feedback receipt is being persisted is not review work.
      if (action !== "watch") collectReviewTiming();
      if (action !== "watch" && !(await openInteractionReady.current)) {
        setActionStatus("无法保存详情打开时间；已进入本地重试队列，请联网后重试研判");
        return;
      }
      if (action !== "watch") await sendReviewHeartbeat();
      const response = await fetch(removingWatch ? `${base}${endpoint}/${encodeURIComponent(event.id)}` : `${base}${endpoint}`, removingWatch
        ? { method: "DELETE" }
        : { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      if (!response.ok) throw new Error("request failed");
      const receipt = await response.json() as { id: string };
      if (action === "watch") onWatchChange(event.id, !watched);
      if (action !== "watch") submittedAttemptId.current = reviewAttemptId.current;
      try {
        await onInteraction(
          action === "watch" ? "watch_toggled" : "triage_submitted",
          event.id,
          action === "watch"
            ? { watched: !watched }
            : {
                action,
              feedbackId: receipt.id,
              reviewAttemptId: reviewAttemptId.current,
              queueEligibilityKey: decisionContext?.queueEligibilityKey ?? "",
                segmentId: timing.current.segmentId,
                measurementVersion: "server-heartbeat-v2",
                idleTimeoutSeconds: 60,
                tickCapSeconds: 5,
              },
        );
      } catch {
        setActionStatus("研判已保存；计时记录已进入本地重试队列");
        return;
      }
      setActionStatus(action === "watch" ? (watched ? "已取消关注" : "已关注") : action === "confirm" ? "已接受" : action === "reject" ? "已拒绝" : "已标记观察");
    } catch {
      setActionStatus("提交失败，未保存");
    }
  };
  const revertOperation = async (operationId: string) => {
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    setActionStatus("正在撤销聚类修订…");
    try {
      const response = await fetch(`${base}/api/v1/cluster-operations/${encodeURIComponent(operationId)}/revert`, { method: "POST" });
      if (!response.ok) throw new Error("revert failed");
      setActionStatus("聚类修订已撤销");
      onLineageRefresh();
    } catch {
      setActionStatus("提交失败，未保存");
    }
  };
  return (
    <aside className="detail-panel" aria-label="事件研判详情">
      <div className="detail-head">
        <div><div className="eyebrow">EVENT / {event.eventType.replaceAll("_", " ")}</div><h2>{event.title}</h2><p>{event.titleEn}</p></div>
        <button className="icon-button close-detail" onClick={onClose} aria-label="关闭详情"><X size={18} /></button>
      </div>
      <div className="detail-state-row">
        <span className={`state-pill state-${event.state}`}>{stateName[event.state]}</span>
        <span className="detail-updated">更新于 {timeAgo(event.updatedAt)}</span>
      </div>
      <div className="label-row">{event.labels.map((label) => <span className="structure-label" key={label}>{labelName[label]}</span>)}</div>
      <div className="detail-actions">
        <button className={watched ? "action-watched" : ""} onClick={() => submitAction("watch")}>{watched ? "取消关注" : "关注事件"}</button>
        <button className="action-confirm" disabled={!decisionContext?.queueEligibilityKey} onClick={() => submitAction("confirm")}>接受</button>
        <button disabled={!decisionContext?.queueEligibilityKey} onClick={() => submitAction("observe")}>需观察</button>
        <button disabled={!decisionContext?.queueEligibilityKey} onClick={() => submitAction("reject")}>拒绝</button>
        <button type="button" aria-pressed={waitingExternal} onClick={toggleExternalWait}>{waitingExternal ? "结束外部等待" : "等待外部数据"}</button>
        <ClusterEditDialog event={event} peers={peers} />
        {actionStatus && <span className={actionStatus.startsWith("提交失败") ? "action-error" : ""} role="status">{actionStatus}</span>}
      </div>
      {!watched && <label className="watch-note">关注备注<input value={watchNote} maxLength={500} onChange={(input) => setWatchNote(input.target.value)} /></label>}
      <section className="context-strip"><div><span>NARRATIVE</span><b>{event.narrativeTitle ?? "未关联长期主题"}</b><small>{event.narrativeId ? `主题 ID · ${event.narrativeId}；当前 Event 仍独立评分。` : "当前 Event 独立评分，不推断模型家族或公司级趋势。"}</small></div><div><span>24H 结果</span><b>N/A</b><small>等待冻结结果标注集</small></div><div><span>7D 结果</span><b>N/A</b><small>不得用未来结果回写时点判断</small></div></section>
      <section className="verdict-card">
        <div className="section-kicker"><BookOpenCheck size={14} /> 判定摘要</div>
        <p>{event.driver}</p>
        <div className="coverage-note"><ShieldCheck size={14} /><span>{event.coverageNote}</span></div>
      </section>
      {assessment && <section className="verdict-card assessment-card">
        <div className="section-kicker"><ShieldCheck size={14} /> 证据完整性</div>
        <p>已观测权重 {Math.round(assessment.observedFeatureWeight * 100)}% · 基线成熟度 {Math.round(assessment.baselineMaturity * 100)}% · 聚类置信度 {Math.round(assessment.clusterConfidence * 100)}% · 不确定性 {Math.round(assessment.uncertainty)}%</p>
        <div className="label-row">{Object.entries(assessment.evidenceMask).map(([feature, state]) => <span className="structure-label" key={feature}>{feature}: {state === "observed" ? "已观测" : state === "not_applicable" ? "不适用" : state === "untrusted" ? "未复核" : "缺失"}</span>)}</div>
        {assessment.missingEvidence.length > 0 && <ul>{assessment.missingEvidence.map((gap) => <li key={gap.feature}>{gap.reason}；{gap.impact}</li>)}</ul>}
      </section>}
      <section className="counter-card">
        <div className="section-kicker"><ShieldCheck size={14} /> 反向信号与可信边界</div>
        {counterSignals.length > 0 ? <ul>{counterSignals.map((signal) => <li key={signal}>{signal}</li>)}</ul> : <p>当前可用结构指标未形成明确反向信号；这不等于互联网中不存在反证。</p>}
        <p><b>正常时滞：</b>{lagBoundary[event.eventType]}</p>
        <p><b>方法限制：</b>只基于已接入且有权使用的信号家族；缺失平台、连接器故障和未知所有权都会提高不确定性。</p>
      </section>
      <section className="metric-grid">
        <Score label="讨论度" value={hasDiscussion ? event.attention : null} />
        <Score label="行为趋势" value={hasBehavior ? event.behavior : null} tone="green" />
        <Score label="信源多样性" value={event.diversity} />
        <Score label="权威性" value={event.authority} tone="green" />
        <Score label="协同风险" value={event.coordinationRisk} tone={event.coordinationRisk > 60 ? "red" : "amber"} />
        <Score label="覆盖置信度" value={event.coverage} tone="amber" />
      </section>
      <section className="trajectory-card">
        <div className="section-title"><span>{windowName[windowSize] ?? windowSize}信号轨迹</span><span className="mono">A {hasDiscussion ? latest.attention : "N/A"} / B {hasBehavior ? latest.behavior : "N/A"}</span></div>
        <div className="trajectory-bars">
          {event.timeline.map((p, index) => <div className="bar-pair" key={p.at} title={new Date(p.at).toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit" })}>
            {hasDiscussion && <i style={{ height: `${p.attention}%` }} />}{hasBehavior && <em style={{ height: `${p.behavior}%` }} />}<small>{index === 0 || index === event.timeline.length - 1 ? new Date(p.at).getHours().toString().padStart(2, "0") : ""}</small>
          </div>)}
        </div>
        <div className="legend">{hasDiscussion && <span><i className="legend-attention" />讨论</span>}{hasBehavior && <span><i className="legend-behavior" />行为</span>}{hasBehavior && <span className="mono">残差 {event.gapResidual > 0 ? "+" : ""}{event.gapResidual}</span>}</div>
      </section>
      <section className="state-history">
        <div className="section-title"><span>状态时间线</span><span className="mono">CLUSTER V{event.clusterVersion ?? 1}</span></div>
        <ol><li><i />首次发现 <time>{new Date(event.firstSeen).toLocaleString("zh-CN")}</time></li><li><i />当前状态：{stateName[event.state]} <time>{new Date(event.updatedAt).toLocaleString("zh-CN")}</time></li></ol>
        {(event.supersededBy?.length ?? 0) > 0 && <p>该事件已被新事件取代：{event.supersededBy?.join("、")}</p>}
        {lineage && <div className="lineage-summary">
          <p>成员 {lineage.currentObservationCount} · 父边 {lineage.parents.length} · 子边 {lineage.children.length}</p>
          {lineage.parents.map((edge) => <p key={`${edge.operationId}:${edge.parentEventId}`}>来自 {edge.parentEventId} · 操作 {edge.operationId} {edge.revertedAt ? "（已撤销）" : ""}</p>)}
          {lineage.children.map((edge) => <p key={`${edge.operationId}:${edge.childEventId}`}>后继 {edge.childEventId} · 操作 {edge.operationId} {edge.revertedAt ? "（已撤销）" : ""}</p>)}
          {lineage.pendingOperations.map((operation) => <div className="lineage-operation" key={operation.id}><span>{operation.operation === "merge" ? "合并" : "拆分"} · {operation.status} · {operation.id}</span>{operation.status === "completed" && <button type="button" onClick={() => revertOperation(operation.id)}>撤销</button>}</div>)}
        </div>}
      </section>
      <section className="propagation-card">
        <div className="section-title"><span>跨平台传播顺序</span><span>{event.platforms.length} 个平台</span></div>
        <div className="propagation-flow">{event.platforms.map((platform, index) => <div className="propagation-step" key={platform}><i>{String(index + 1).padStart(2, "0")}</i><b>{platform}</b>{index < event.platforms.length - 1 && <ChevronRight size={13} />}</div>)}</div>
      </section>
      <section className="evidence-section">
        <div className="section-title"><span>证据链</span><span>已复核 {visibleVerifiedEvidence.length} / {totalVerifiedEvidence} 项{unverifiedEvidenceCount > 0 ? ` · 未复核候选 ${unverifiedEvidenceCount}` : ""}</span></div>
        <div className="evidence-list">{event.evidence.map((item, index) => (
          <a className="evidence-item" href={item.url} target="_blank" rel="noreferrer" key={item.id} onClick={() => { void onInteraction("evidence_opened", event.id, { evidenceId: item.id, platform: item.platform }); }}>
            <span className={`evidence-index evidence-${item.kind}`}>{String(index + 1).padStart(2, "0")}</span>
            <div><b>{item.title}</b><p>{item.excerpt}</p><small>{item.platform} · {item.source} · {timeAgo(item.publishedAt)}{item.provenanceLevel === "unverified_discovery" && <span className="evidence-provenance-unverified"> · 未复核候选，不参与评分</span>}</small></div>
            <ChevronRight size={15} />
          </a>
        ))}</div>
      </section>
    </aside>
  );
}

function AlertRuleDialog() {
  const [open, setOpen] = useState(false);
  const [status, setStatus] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setStatus("saving");
    const form = new FormData(event.currentTarget);
    const payload = {
      name: String(form.get("name") || "AI 热点确认"),
      minimumAttention: Number(form.get("attention") || 70),
      minimumEvidenceStrength: Number(form.get("evidence") || 65),
      webhookUrl: String(form.get("webhook") || "") || null,
    };
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    try {
      const response = await fetch(`${base}/api/v1/alert-rules`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) });
      if (!response.ok) throw new Error("save failed");
      setStatus("saved");
    } catch {
      setStatus("error");
    }
  };
  return <Dialog.Root open={open} onOpenChange={(value) => { setOpen(value); if (!value) setStatus("idle"); }}>
    <Dialog.Trigger asChild><button className="alert-button"><Bell size={15} /> 新建预警</button></Dialog.Trigger>
    <Dialog.Portal><Dialog.Overlay className="dialog-overlay" /><Dialog.Content className="dialog-content">
      <Dialog.Title>创建事件预警</Dialog.Title>
      <Dialog.Description>事件进入加速阶段且证据强度达到阈值时，通过签名 Webhook 通知。</Dialog.Description>
      <form onSubmit={submit}>
        <label>规则名称<input name="name" defaultValue="AI 热点确认" /></label>
        <div className="dialog-grid"><label>最低讨论度<input name="attention" type="number" defaultValue="70" /></label><label>最低证据分（内部阈值）<input name="evidence" type="number" defaultValue="65" /></label></div>
        <label>Webhook URL<input name="webhook" placeholder="https://hooks.example.com/…" /></label>
        {status === "saved" && <p className="form-status success" role="status">规则已保存</p>}
        {status === "error" && <p className="form-status error" role="alert">保存失败，未创建规则</p>}
        <div className="dialog-actions"><Dialog.Close asChild><button type="button" className="button-secondary">取消</button></Dialog.Close><button className="button-primary" type="submit" disabled={status === "saving"}><Send size={14} /> {status === "saving" ? "保存中…" : "保存规则"}</button></div>
      </form>
      <Dialog.Close className="dialog-close" aria-label="关闭"><X size={17} /></Dialog.Close>
    </Dialog.Content></Dialog.Portal>
  </Dialog.Root>;
}

function FilterDialog({ filters, onChange }: { filters: Filters; onChange: (next: Filters) => void }) {
  const active = Object.values(filters).filter((value) => value !== "all").length;
  return <Dialog.Root>
    <Dialog.Trigger asChild><button className="filter-button"><Filter size={14} /> 筛选 {active > 0 && <span>{active}</span>}</button></Dialog.Trigger>
    <Dialog.Portal><Dialog.Overlay className="dialog-overlay" /><Dialog.Content className="dialog-content filter-dialog">
      <Dialog.Title>筛选研判队列</Dialog.Title>
      <Dialog.Description>筛选只影响当前工作台，不改变评分或告警规则。</Dialog.Description>
      <label>生命周期<select value={filters.state} onChange={(event) => onChange({ ...filters, state: event.target.value as Filters["state"] })}><option value="all">全部阶段</option>{Object.entries(stateName).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
      <label>事件类型<select value={filters.eventType} onChange={(event) => onChange({ ...filters, eventType: event.target.value as Filters["eventType"] })}><option value="all">全部类型</option><option value="model_release">模型发布</option><option value="developer_tool_release">开发工具</option><option value="research_or_benchmark">研究 / 基准</option><option value="official_product_release">产品发布</option><option value="security_incident">安全事件</option></select></label>
      <label>证据强度<select value={filters.evidence} onChange={(event) => onChange({ ...filters, evidence: event.target.value as Filters["evidence"] })}><option value="all">全部强度</option><option value="high">高</option><option value="medium">中</option><option value="low">低</option></select></label>
      <div className="dialog-actions"><button type="button" className="button-secondary" onClick={() => onChange({ state: "all", eventType: "all", evidence: "all" })}>清空</button><Dialog.Close asChild><button className="button-primary" type="button">应用筛选</button></Dialog.Close></div>
      <Dialog.Close className="dialog-close" aria-label="关闭"><X size={17} /></Dialog.Close>
    </Dialog.Content></Dialog.Portal>
  </Dialog.Root>;
}

const sourceStatusName: Record<SourceStatus, string> = {
  candidate: "候选", active: "活跃", paused: "暂停", blocked: "阻断",
};

function SourceView({ catalog, loading, query, status, onQueryChange, onStatusChange, onPageChange }: {
  catalog: SourceCatalogResponse | null; loading: boolean; query: string; status: "all" | SourceStatus;
  onQueryChange: (value: string) => void; onStatusChange: (value: "all" | SourceStatus) => void;
  onPageChange: (offset: number) => void;
}) {
  const rows = catalog?.items ?? [];
  return (
    <div className="content-view source-view">
      <div className="view-heading"><div><div className="eyebrow">SOURCE GOVERNANCE</div><h1>热点信源治理</h1><p>自动发现候选，证据充分后再校准与晋级；缺失质量特征不等于低分。</p></div></div>
      {!catalog && <div className="boundary-card boundary-warning"><ShieldCheck size={22} /><div><h3>{loading ? "正在读取信源登记表" : "信源目录尚不可用"}</h3><p>{loading ? "正在从 API 获取候选生命周期与策略证明。" : "录制演示不会伪造真实信源排名；启动 API 后才能查看候选登记表。"}</p></div></div>}
      {catalog && <>
        <section className="source-policy-card">
          <div><span>SOURCESCORE</span><b>{catalog.sourceScorePolicy.rankingEnabled ? "排行已启用" : "排行关闭"}</b><small>{catalog.sourceScorePolicy.version}</small></div>
          <p>{catalog.sourceScorePolicy.rankingEnabled ? "已通过历史结果集与反馈回路校准。" : `等待真实历史结果集校准；候选分不参与排序。最低 ${catalog.sourceScorePolicy.minimumValidObservations} 次有效观测、${catalog.sourceScorePolicy.minimumHistoryDays} 天历史；${catalog.sourceScorePolicy.timezone} 单日晋级按 floor 取整且不超过 ${(catalog.sourceScorePolicy.dailyGrowthRate * 100).toFixed(0)}%，首批 active 种子需人工审核。`}</p>
          <div className="policy-flags"><span className={catalog.sourceScorePolicy.autoPromotionEnabled ? "flag-on" : "flag-off"}>自动晋级 {catalog.sourceScorePolicy.autoPromotionEnabled ? "ON" : "OFF"}</span><span>策略摘要 {catalog.sourceScorePolicy.digest.slice(0, 18)}…</span></div>
        </section>
        <div className="coverage-summary source-summary"><div><b>{catalog.counts.active ?? 0}/{catalog.activeCapacity}</b><span>活跃信源 / 容量</span></div><div><b>{catalog.counts.candidate ?? 0}/{catalog.candidateCapacity}</b><span>候选池 / 容量</span></div><div><b>{catalog.total}</b><span>当前查询匹配数</span></div><div><b>{catalog.systemCapacity}</b><span>系统容量上限</span></div></div>
        <section className="source-directory">
          <div className="queue-toolbar"><div className="search-box"><Search size={15} /><input maxLength={200} value={query} onChange={(event) => onQueryChange(event.target.value)} placeholder="搜索信源、平台、账号或实体…" aria-label="搜索信源" /></div><select aria-label="信源状态" value={status} onChange={(event) => onStatusChange(event.target.value as "all" | SourceStatus)}><option value="all">全部状态</option>{Object.entries(sourceStatusName).map(([value, label]) => <option value={value} key={value}>{label}</option>)}</select><div className="queue-count">{catalog.total === 0 ? "0" : `${catalog.offset + 1}–${catalog.offset + rows.length}`} / {catalog.total}</div></div>
          <div className="table-scroll"><table className="source-table"><thead><tr><th>信源</th><th>生命周期</th><th>证据历史</th><th>SourceScore</th><th>发现依据与阻断项</th></tr></thead><tbody>{rows.map((source) => <tr key={source.id}>
            <td><b>{source.displayName}</b><small>{source.platform} · {source.language.toUpperCase()}</small><em>{source.id}</em></td>
            <td><span className={`source-status source-${source.status}`}>{sourceStatusName[source.status]}</span><small>最近观测 {timeAgo(source.lastObservedAt)}</small></td>
            <td><b className="mono">{source.validObservations}</b><small>有效观测 · {source.historyDays} 天历史</small><em>{source.accountIds.length} 账号 / {source.entityIds.length} 实体</em></td>
            <td><b className="mono">{source.candidateScore === null ? "N/A" : source.candidateScore.toFixed(2)}</b><small>{source.scoreEvidenceStatus === "eligible" ? "质量证据充分" : "质量证据不足"}</small><em>{source.rankEligible ? "可参与排序" : "不参与排序"}</em></td>
            <td><p>{source.discoveryReasons.join(" · ") || "尚无发现说明"}</p><small>{source.blockedReasons.join("；") || "没有晋级阻断项"}</small></td>
          </tr>)}</tbody></table>{rows.length === 0 && <p className="empty-directory">没有匹配的信源。</p>}</div>
          <div className="source-pagination"><button type="button" disabled={catalog.offset === 0} onClick={() => onPageChange(Math.max(0, catalog.offset - catalog.limit))}>上一页</button><span className="mono">PAGE {Math.floor(catalog.offset / catalog.limit) + 1}</span><button type="button" disabled={!catalog.hasMore} onClick={() => onPageChange(catalog.offset + catalog.limit)}>下一页</button></div>
        </section>
      </>}
    </div>
  );
}

function CoverageView({ connectors, budget }: { connectors: ConnectorStatus[]; budget: CoverageBudget | null }) {
  const healthy = connectors.filter((c) => c.status === "healthy").length;
  const observations = connectors.reduce((sum, c) => sum + c.observations24h, 0);
  const families = new Set(connectors.filter((c) => c.status !== "paused").map((c) => c.family)).size;
  const latencies = connectors.filter((c) => c.status !== "paused").map((c) => c.latencyMinutes).sort((a, b) => a - b);
  const p95Latency = latencies.length ? latencies[Math.ceil(latencies.length * .95) - 1] : null;
  return (
    <div className="content-view coverage-view">
      <div className="view-heading"><div><div className="eyebrow">DATA PLANE</div><h1>覆盖与连接器</h1><p>数据缺口会显式降低证据强度，不会被解释为热度下降。</p></div></div>
      <div className="coverage-summary"><div><b>{healthy}/{connectors.length}</b><span>健康连接器</span></div><div><b>{observations.toLocaleString()}</b><span>24H 观测</span></div><div><b>{families}</b><span>独立信号家族</span></div><div><b>{p95Latency === null ? "N/A" : `${p95Latency}m`}</b><span>P95 采集轮次耗时</span></div><div><b>{budget ? `¥${budget.spent.toFixed(0)} / ¥${budget.limit.toFixed(0)}` : "N/A"}</b><span>{budget ? `本月剩余 ¥${budget.remaining.toFixed(0)}` : "预算接口未连接"}</span></div></div>
      <div className="connector-grid">{connectors.map((connector) => (
        <article className="connector-card" key={connector.id}>
          <div className="connector-head"><div className="connector-icon"><Database size={18} /></div><div><h3>{connector.name}</h3><span>{connector.family}</span></div><span className={`connector-status connector-${connector.status}`}>{connector.status}</span></div>
          <div className="connector-metrics"><div><span>覆盖</span><b>{connector.coverage}%</b></div><div><span>延迟</span><b>{connector.status === "paused" ? "—" : `${connector.latencyMinutes}m`}</b></div><div><span>24H</span><b>{connector.observations24h.toLocaleString()}</b></div></div>
          <div className="connector-ops"><span>配额 {connector.quotaLimit ? `${connector.quotaUsed ?? 0} / ${connector.quotaLimit}` : "N/A"}</span><span>本月成本 {connector.costRmbMonth == null ? "N/A" : `¥${connector.costRmbMonth}`}</span><span>权利 {connector.rightsStatus === "blocked" ? "关闭" : connector.rightsStatus === "experimental" ? "实验" : "可用"}</span></div>
          <div className="connector-progress"><i style={{ width: `${connector.coverage}%` }} /></div>
          <p>{connector.note}</p><small>最近成功 · {timeAgo(connector.lastSuccess)}</small>
        </article>
      ))}</div>
      <div className="boundary-card boundary-warning"><ShieldCheck size={22} /><div><h3>中文讨论覆盖不足</h3><p>当前中文侧主要依赖官方站点、合法 RSS 与媒体源，尚无稳定、获授权的中文社会化讨论家族。因此系统不声称具备完整中英文破圈比较能力。</p></div></div>
      <div className="boundary-card"><ShieldCheck size={22} /><div><h3>可信边界</h3><p>X、Bilibili 等实验源不构成强判定的必要条件；连接器停机时冻结对应指标并提高不确定性。未授权的受限平台默认关闭。</p></div></div>
    </div>
  );
}

function MethodView() {
  return (
    <div className="content-view method-view">
      <div className="view-heading"><div><div className="eyebrow">DECISION SYSTEM</div><h1>方法与可信边界</h1><p>生命周期负责“正在发生什么”，结构标签负责“为什么会这样”。</p></div></div>
      <div className="method-grid">
        <section><span className="method-number">01</span><h3>讨论度 Attention</h3><p>以独立讨论者、讨论速度、加速度和跨平台迁移构成；转发与同文案会降权，不把曝光量直接当作讨论。</p></section>
        <section><span className="method-number">02</span><h3>行为趋势 Behavior</h3><p>按事件类型选择有效行为：模型看下载与衍生，工具看安装与仓库，研究看复现与引用，安全事件看补丁采用。</p></section>
        <section><span className="method-number">03</span><h3>证据强度</h3><p>Coverage、来源多样性、权威性和可核验性共同决定结论力度。覆盖低于 40 时只显示“数据不足”。</p></section>
        <section><span className="method-number">04</span><h3>剪刀差残差</h3><p>比较真实行为与该事件类型的预期行为，而非生硬比较两个原始分数；论文类的正常行为滞后会单独标记。</p></section>
      </div>
      <section className="state-machine"><div className="section-kicker"><GitBranch size={15} /> 生命周期状态机</div><div className="state-flow"><span>发现</span><i /><span>萌发</span><i /><span>加速</span><i /><span>已建立</span><i /><span>降温</span><i /><span>休眠</span></div><p>状态至少连续两个评分周期确认；普通告警四小时冷却。任何结论均保存输入时间窗、评分版本、阈值版本与驱动因素。</p></section>
      <section className="formula-card"><div><small>ROBUST BASELINE</small><code>z = (x − median) / (1.4826 × MAD + ε)</code></div><div><small>EVIDENCE QUALITY</small><code>E = .30C + .20R + .20B + .15K + .15S</code></div><div><small>GAP RESIDUAL</small><code>G = observedGap − expectedGap(type, age, mix)</code></div></section>
    </div>
  );
}

export function RadarShell() {
  const [payload, setPayload] = useState<RadarPayload>(demoPayload);
  const [dataMode, setDataMode] = useState<"live" | "stale" | "demo">("demo");
  const [view, setView] = useState<View>("queue");
  const [selectedId, setSelectedId] = useState(demoPayload.events[0].id);
  const [detailOpen, setDetailOpen] = useState(true);
  const [detailAssessment, setDetailAssessment] = useState<EventAssessment | null>(null);
  const [detailDecisionContext, setDetailDecisionContext] = useState<DecisionContext | null>(null);
  const [detailLineage, setDetailLineage] = useState<EventLineage | null>(null);
  const [coverageBudget, setCoverageBudget] = useState<CoverageBudget | null>(null);
  const [sourceCatalog, setSourceCatalog] = useState<SourceCatalogResponse | null>(null);
  const [sourceCatalogFailed, setSourceCatalogFailed] = useState(false);
  const [sourceQuery, setSourceQuery] = useState("");
  const deferredSourceQuery = useDeferredValue(sourceQuery);
  const [sourceStatus, setSourceStatus] = useState<"all" | SourceStatus>("all");
  const [sourceOffset, setSourceOffset] = useState(0);
  const [query, setQuery] = useState("");
  const [windowSize, setWindowSize] = useState("6H");
  const [mobileNav, setMobileNav] = useState(false);
  const [mobileDetail, setMobileDetail] = useState(false);
  const [filters, setFilters] = useState<Filters>({ state: "all", eventType: "all", evidence: "all" });
  const [watchedIds, setWatchedIds] = useState<Set<string>>(new Set());
  const [showWatched, setShowWatched] = useState(false);
  const searchInput = useRef<HTMLInputElement>(null);
  const interactionSession = useRef("");
  const recordInteraction = useCallback(async (kind: "detail_opened" | "evidence_opened" | "triage_submitted" | "review_segment_closed" | "review_heartbeat" | "watch_toggled", eventId: string, metadata: Record<string, string | number | boolean> = {}) => {
    if (!interactionSession.current) {
      const existing = window.sessionStorage.getItem("signal-ai-review-session");
      interactionSession.current = existing || window.crypto.randomUUID();
      if (!existing) window.sessionStorage.setItem("signal-ai-review-session", interactionSession.current);
    }
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    const payload: InteractionPayload = {
      kind, idempotencyKey: window.crypto.randomUUID(), sessionId: interactionSession.current, eventId, metadata,
    };
    const pending = readInteractionOutbox();
    pending.push(payload);
    writeInteractionOutbox(pending);
    await flushInteractionOutbox(base);
  }, []);

  useEffect(() => {
    const existing = window.sessionStorage.getItem("signal-ai-review-session");
    interactionSession.current = existing || window.crypto.randomUUID();
    if (!existing) window.sessionStorage.setItem("signal-ai-review-session", interactionSession.current);
  }, []);

  useEffect(() => {
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    const retry = () => { void flushInteractionOutbox(base).catch(() => undefined); };
    retry();
    window.addEventListener("online", retry);
    return () => window.removeEventListener("online", retry);
  }, []);

  useEffect(() => {
    const media = window.matchMedia("(max-width: 760px)");
    const sync = () => setMobileDetail(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  useEffect(() => {
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    let disposed = false;
    let hasLiveData = false;
    const load = async () => {
      const controller = new AbortController();
      const timeout = window.setTimeout(() => controller.abort(), 2500);
      try {
        const response = await fetch(`${base}/api/v1/radar?window=${windowSize.toLowerCase()}`, { signal: controller.signal });
        if (!response.ok) throw new Error("API unavailable");
        const data = await response.json() as RadarPayload;
        if (!disposed && Array.isArray(data.events)) {
          hasLiveData = data.dataMode === "live";
          setPayload(data);
          setSelectedId((current) => data.events.some((item) => item.id === current) ? current : (data.events[0]?.id ?? ""));
          setDataMode(data.dataMode === "recorded_demo" ? "demo" : "live");
        }
      } catch {
        if (!disposed) setDataMode(hasLiveData ? "stale" : "demo");
      } finally {
        window.clearTimeout(timeout);
      }
    };
    void load();
    const poll = window.setInterval(load, 60_000);
    const stream = new EventSource(`${base}/api/v1/stream`);
    stream.addEventListener("radar", () => void load());
    stream.onerror = () => { if (hasLiveData) setDataMode("stale"); };
    return () => { disposed = true; window.clearInterval(poll); stream.close(); };
  }, [windowSize]);

  useEffect(() => {
    if (dataMode !== "live") return;
    const controller = new AbortController();
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    void fetch(`${base}/api/v1/watchlists`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("watchlists unavailable")))
      .then((data: { items: Array<{ eventId: string }> }) => setWatchedIds(new Set(data.items.map((item) => item.eventId))))
      .catch(() => undefined);
    void fetch(`${base}/api/v1/coverage`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("coverage unavailable")))
      .then((data: { budget: CoverageBudget }) => setCoverageBudget(data.budget))
      .catch(() => undefined);
    return () => controller.abort();
  }, [dataMode]);

  useEffect(() => {
    if (view !== "sources") return;
    const controller = new AbortController();
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    const parameters = new URLSearchParams({ limit: "500", offset: String(sourceOffset) });
    if (deferredSourceQuery) parameters.set("query", deferredSourceQuery);
    if (sourceStatus !== "all") parameters.set("status", sourceStatus);
    void fetch(`${base}/api/v1/sources?${parameters}`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("sources unavailable")))
      .then((data: SourceCatalogResponse) => { setSourceCatalog(data); setSourceCatalogFailed(false); })
      .catch(() => { if (!controller.signal.aborted) { setSourceCatalog(null); setSourceCatalogFailed(true); } });
    return () => controller.abort();
  }, [deferredSourceQuery, sourceOffset, sourceStatus, view]);

  useEffect(() => {
    if (!selectedId || dataMode !== "live") {
      return;
    }
    const controller = new AbortController();
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    void fetch(`${base}/api/v1/events/${encodeURIComponent(selectedId)}`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("detail unavailable")))
      .then((data: { assessment: EventAssessment; decisionContext: DecisionContext }) => {
        setDetailAssessment(data.assessment);
        setDetailDecisionContext(data.decisionContext);
      })
      .catch(() => { if (!controller.signal.aborted) { setDetailAssessment(null); setDetailDecisionContext(null); } });
    void fetch(`${base}/api/v1/events/${encodeURIComponent(selectedId)}/lineage`, { signal: controller.signal })
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("lineage unavailable")))
      .then((data: EventLineage) => setDetailLineage(data))
      .catch(() => { if (!controller.signal.aborted) setDetailLineage(null); });
    return () => controller.abort();
  }, [selectedId, dataMode, windowSize, recordInteraction]);

  useEffect(() => {
    const shortcut = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement | null;
      if (event.key === "/" && !target?.matches("input, textarea, select, [contenteditable='true']")) {
        event.preventDefault();
        setView("queue");
        window.setTimeout(() => searchInput.current?.focus(), 0);
      }
    };
    window.addEventListener("keydown", shortcut);
    return () => window.removeEventListener("keydown", shortcut);
  }, []);

  const events = useMemo(() => payload.events.filter((event) => {
    const matchesQuery = `${event.title} ${event.titleEn} ${event.platforms.join(" ")}`.toLowerCase().includes(query.toLowerCase());
    return matchesQuery && (!showWatched || watchedIds.has(event.id)) && (filters.state === "all" || event.state === filters.state) && (filters.eventType === "all" || event.eventType === filters.eventType) && (filters.evidence === "all" || event.evidenceStrength === filters.evidence);
  }), [payload.events, query, filters, showWatched, watchedIds]);
  const radarEvents = useMemo(() => events.filter((event) =>
    discussionObserved(event) && behaviorObserved(event)
  ), [events]);
  const selected = payload.events.find((event) => event.id === selectedId) ?? payload.events[0];
  const strongLifecycle = payload.events.filter((e) => e.state === "accelerating" || e.state === "established").length;
  const gap = payload.events.filter((e) => e.labels.includes("attention_behavior_gap")).length;
  const concentrated = payload.events.filter((e) => e.labels.includes("platform_concentrated")).length;
  const generatedAtMs = new Date(payload.generatedAt).getTime();
  const recentlyUpdated = payload.events.filter((event) => generatedAtMs - new Date(event.updatedAt).getTime() <= 3_600_000).length;
  const selectEvent = useCallback((id: string) => { setSelectedId(id); setDetailAssessment(null); setDetailDecisionContext(null); setDetailLineage(null); setDetailOpen(true); }, []);
  const refreshLineage = useCallback(() => {
    if (!selectedId || dataMode !== "live") return;
    const base = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8017";
    void fetch(`${base}/api/v1/events/${encodeURIComponent(selectedId)}/lineage`)
      .then((response) => response.ok ? response.json() : Promise.reject(new Error("lineage unavailable")))
      .then((data: EventLineage) => setDetailLineage(data))
      .catch(() => setDetailLineage(null));
  }, [dataMode, selectedId]);
  const updateWatched = useCallback((eventId: string, watched: boolean) => setWatchedIds((current) => {
    const next = new Set(current);
    if (watched) next.add(eventId); else next.delete(eventId);
    return next;
  }), []);

  return (
    <Tooltip.Provider delayDuration={250}>
      <div className="app-shell">
        <header className="topbar">
          <button className="mobile-menu icon-button" onClick={() => setMobileNav((v) => !v)} aria-label="打开导航"><Menu size={19} /></button>
          <div className="brand"><span className="brand-mark"><Radar size={19} /></span><b>SIGNAL<span>{"//"}</span>AI</b><small>开发者与研究生态信号 Beta</small></div>
          <div className="system-status"><i className={dataMode === "live" ? "pulse-live" : "pulse-demo"} /><span>{dataMode === "live" ? "LIVE PIPELINE" : dataMode === "stale" ? "POLLING / STALE" : "RECORDED DEMO"}</span><em>·</em><span>更新 {timeAgo(payload.generatedAt)}</span></div>
          <div className="top-actions">
            <Tooltip.Root><Tooltip.Trigger asChild><button className="icon-button" aria-label="搜索" onClick={() => { setView("queue"); window.setTimeout(() => searchInput.current?.focus(), 0); }}><Search size={17} /></button></Tooltip.Trigger><Tooltip.Portal><Tooltip.Content className="tooltip">快捷搜索 <kbd>/</kbd><Tooltip.Arrow className="tooltip-arrow" /></Tooltip.Content></Tooltip.Portal></Tooltip.Root>
            <AlertRuleDialog />
            <div className="avatar">AN</div>
          </div>
        </header>

        <aside className={`sidebar ${mobileNav ? "open" : ""}`}>
          <nav aria-label="主导航">
            {([
              ["queue", LayoutList, "研判队列"], ["radar", CircleGauge, "信号雷达"],
              ["sources", UsersRound, "信源治理"], ["coverage", Database, "数据覆盖"],
              ["method", BookOpenCheck, "方法与边界"],
            ] as const).map(([id, Icon, label]) => <button key={id} className={view === id ? "active" : ""} onClick={() => { setView(id); setMobileNav(false); }}><Icon size={17} /><span>{label}</span>{id === "queue" && <em>{events.length}</em>}</button>)}
          </nav>
          <div className="sidebar-section"><span>工作区</span><button><Activity size={16} /><span>AI 行业雷达</span><i className="workspace-dot" /></button></div>
          <div className="sidebar-foot"><div><b>V1 · RC2</b><span>评分引擎 0.6.0</span></div><small>预算守卫已启用 · 实时支出见覆盖页</small><i><em style={{ width: "0%" }} /></i></div>
        </aside>

        <main className={`main ${detailOpen && (view === "queue" || view === "radar") ? "with-detail" : ""}`}>
          {view === "queue" && <div className="content-view queue-view">
            <div className="view-heading"><div><div className="eyebrow">REVIEW QUEUE / {windowSize}</div><h1>AI 热点研判队列</h1><p>优先处理高速度、高证据、状态刚发生变化的事件。</p></div><div className="window-switch">{["1H", "6H", "24H", "7D"].map((item) => <button className={windowSize === item ? "active" : ""} onClick={() => setWindowSize(item)} key={item}>{item}</button>)}</div></div>
            <div className="summary-grid">
              <div className="summary-card"><span>待研判事件</span><b>{payload.events.length.toString().padStart(2, "0")}</b><small><i className="dot-blue" /> {recentlyUpdated} 个事件在 1 小时内更新</small></div>
              <div className="summary-card"><span>加速 / 已建立</span><b className="tone-green">{strongLifecycle.toString().padStart(2, "0")}</b><small>满足对应事件类型的阶段门槛</small></div>
              <div className="summary-card"><span>剪刀差风险</span><b className="tone-red">{gap.toString().padStart(2, "0")}</b><small>含协同发布风险</small></div>
              <div className="summary-card"><span>单平台集中</span><b className="tone-amber">{concentrated.toString().padStart(2, "0")}</b><small>尚未跨平台迁移</small></div>
            </div>
            <section className="queue-section">
              <div className="queue-toolbar"><div className="search-box"><Search size={15} /><input ref={searchInput} value={query} onChange={(e) => setQuery(e.target.value)} placeholder="搜索事件、平台或信源…" aria-label="搜索事件" /></div><button className={`filter-button watch-toggle ${showWatched ? "active" : ""}`} aria-pressed={showWatched} onClick={() => setShowWatched((value) => !value)}><Bell size={14} />只看关注 <span>{watchedIds.size}</span></button><FilterDialog filters={filters} onChange={setFilters} /><div className="queue-count">显示 {events.length} / {payload.events.length}</div></div>
              <QueueTable events={events} selectedId={selectedId} windowSize={windowSize} onSelect={selectEvent} />
            </section>
          </div>}
          {view === "radar" && <div className="content-view radar-view"><div className="view-heading"><div><div className="eyebrow">SIGNAL MAP / {windowSize}</div><h1>讨论 × 行为信号雷达</h1><p>坐标只用于适配事件类型；圆点大小表示证据强度。</p></div><div className="window-switch">{["1H", "6H", "24H", "7D"].map((item) => <button className={windowSize === item ? "active" : ""} onClick={() => setWindowSize(item)} key={item}>{item}</button>)}</div></div><section className="radar-card"><div className="radar-legend"><span><i className="state-accelerating-dot" />加速</span><span><i className="state-emerging-dot" />萌发</span><span><i className="state-detected-dot" />已发现</span></div><Suspense fallback={<div className="radar-chart" role="status">正在加载雷达图…</div>}><RadarChart events={radarEvents} onSelect={selectEvent} /></Suspense><details className="radar-data"><summary>查看图表数据表</summary><div className="table-scroll"><table><thead><tr><th>事件</th><th>讨论</th><th>行为</th><th>证据</th></tr></thead><tbody>{radarEvents.map((event) => <tr key={event.id}><td>{event.title}</td><td>{event.attention}</td><td>{event.behavior}</td><td>{({ low: "低", medium: "中", high: "高" })[event.evidenceStrength]}</td></tr>)}</tbody></table></div></details></section><div className="radar-insight"><Activity size={18} /><div><b>当前结构</b><p>{strongLifecycle} 个事件处于加速或已建立阶段，{gap} 个事件存在注意力与实际行为的显著偏离。</p></div></div></div>}
          {view === "sources" && <SourceView catalog={sourceCatalog} loading={!sourceCatalogFailed} query={sourceQuery} status={sourceStatus} onQueryChange={(value) => { setSourceQuery(value); setSourceOffset(0); }} onStatusChange={(value) => { setSourceStatus(value); setSourceOffset(0); }} onPageChange={setSourceOffset} />}
          {view === "coverage" && <CoverageView connectors={payload.connectors} budget={coverageBudget} />}
          {view === "method" && <MethodView />}
        </main>
        {detailOpen && selected && (view === "queue" || view === "radar") && (mobileDetail ?
          <Dialog.Root open={detailOpen} onOpenChange={setDetailOpen}>
            <Dialog.Portal>
              <Dialog.Overlay className="dialog-overlay mobile-detail-overlay" />
              <Dialog.Content className="mobile-detail-dialog" aria-describedby={undefined}>
                <Dialog.Title className="sr-only">{selected.title}事件研判详情</Dialog.Title>
                <DetailPanel key={`${selected.id}:${windowSize}`} event={selected} peers={payload.events} assessment={detailAssessment?.eventId === selected.id ? detailAssessment : null} decisionContext={detailDecisionContext} lineage={detailLineage?.eventId === selected.id ? detailLineage : null} windowSize={windowSize} watched={watchedIds.has(selected.id)} onWatchChange={updateWatched} onLineageRefresh={refreshLineage} onInteraction={recordInteraction} onClose={() => setDetailOpen(false)} />
              </Dialog.Content>
            </Dialog.Portal>
          </Dialog.Root> :
          <DetailPanel key={`${selected.id}:${windowSize}`} event={selected} peers={payload.events} assessment={detailAssessment?.eventId === selected.id ? detailAssessment : null} decisionContext={detailDecisionContext} lineage={detailLineage?.eventId === selected.id ? detailLineage : null} windowSize={windowSize} watched={watchedIds.has(selected.id)} onWatchChange={updateWatched} onLineageRefresh={refreshLineage} onInteraction={recordInteraction} onClose={() => setDetailOpen(false)} />)}
      </div>
    </Tooltip.Provider>
  );
}
