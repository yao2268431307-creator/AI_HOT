"use client";

import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import type { RadarEvent } from "../lib/types";

type SortKey = "state" | "evidence" | "velocity" | "newEvidence" | "behavior" | "attention";
type SortDirection = "desc" | "asc";
type QueueSort = { key: SortKey; direction: SortDirection } | null;

const stateName: Record<string, string> = {
  accelerating: "加速",
  established: "已建立",
  emerging: "萌发",
  detected: "已发现",
  cooling: "降温",
  dormant: "休眠",
  noise: "噪声",
  insufficient_data: "数据不足",
};

const typeName: Record<string, string> = {
  model_release: "模型发布",
  developer_tool_release: "开发工具",
  research_or_benchmark: "研究 / 基准",
  official_product_release: "产品发布",
  security_incident: "安全事件",
};

const labelName: Record<string, string> = {
  cross_platform_confirmed: "生态内跨平台", adoption_confirmed: "采用确认", platform_concentrated: "单平台集中",
  coordination_risk: "协同风险", attention_behavior_gap: "剪刀差", expected_behavior_lag: "正常时滞",
  official_source_led: "官方首发", low_source_diversity: "多样性低", reactivated: "再次活跃",
};

const stateRank: Record<string, number> = {
  established: 7, accelerating: 6, emerging: 5, detected: 4,
  cooling: 3, dormant: 2, noise: 1, insufficient_data: 0,
};
const evidenceRank = { low: 0, medium: 1, high: 2 } as const;
const timeAgo = (iso: string) => {
  const minutes = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 60_000));
  if (minutes < 60) return `${Math.max(1, minutes)} 分钟前`;
  if (minutes < 1440) return `${Math.round(minutes / 60)} 小时前`;
  return `${Math.round(minutes / 1440)} 天前`;
};

const discussionObserved = (event: RadarEvent) => event.discussionEvidenceState === undefined
  ? event.evidence.some((item) => item.kind === "discussion")
  : event.discussionEvidenceState === "observed";
const behaviorObserved = (event: RadarEvent) => event.behaviorEvidenceState === undefined
  ? event.evidence.some((item) => item.kind === "behavior")
  : event.behaviorEvidenceState === "observed";

const sortValue = (event: RadarEvent, key: SortKey): number | null => {
  if (key === "state") return stateRank[event.state] ?? 0;
  if (key === "evidence") return evidenceRank[event.evidenceStrength] * 101 + event.evidenceScore;
  if (key === "velocity") return event.velocity;
  if (key === "newEvidence") return event.newEvidenceCount ?? 0;
  if (key === "behavior") return behaviorObserved(event) ? event.behavior : null;
  return discussionObserved(event) ? event.attention : null;
};

function SortHeader({ label, sortKey, sort, onToggle }: { label: string; sortKey: SortKey; sort: QueueSort; onToggle: (key: SortKey) => void }) {
  const active = sort?.key === sortKey;
  const direction = active ? sort.direction : null;
  const current = direction === "desc" ? "高到低" : direction === "asc" ? "低到高" : "未排序";
  const next = direction === "desc" ? "低到高" : "高到低";
  return <button type="button" className={`sort-header ${active ? "active" : ""}`} onClick={() => onToggle(sortKey)} aria-label={`${label}当前${current}，点击切换为${next}`} title={`${label}：${current}`}>
    <span>{label}</span>{active && <small>{current}</small>}{direction === "desc" ? <ArrowDown size={12} aria-hidden="true" /> : direction === "asc" ? <ArrowUp size={12} aria-hidden="true" /> : <ChevronsUpDown size={12} aria-hidden="true" />}
  </button>;
}

function Sparkline({ event, windowSize }: { event: RadarEvent; windowSize: string }) {
  const width = 78;
  const height = 28;
  const path = (key: "attention" | "behavior") => event.timeline.map((p, index) => {
    const x = event.timeline.length === 1 ? 0 : (index / (event.timeline.length - 1)) * width;
    const y = height - (p[key] / 100) * height;
    return `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const hasDiscussion = discussionObserved(event);
  const hasBehavior = behaviorObserved(event);
  return (
    <svg className="sparkline" viewBox={`0 0 ${width} ${height}`} aria-label={`${windowSize} 可用信号走势`}>
      {hasDiscussion && <path d={path("attention")} className="spark-attention" />}
      {hasBehavior && <path d={path("behavior")} className="spark-behavior" />}
    </svg>
  );
}

export function QueueTable({ events, selectedId, windowSize, mode, scopeKey, onSelect }: { events: RadarEvent[]; selectedId: string; windowSize: string; mode: "latest" | "priority"; scopeKey: string; onSelect: (id: string) => void }) {
  const pageSize = 15;
  const pageCount = Math.max(1, Math.ceil(events.length / pageSize));
  const [page, setPage] = useState(0);
  const [sort, setSort] = useState<QueueSort>(null);
  useEffect(() => {
    setPage((current) => Math.min(current, pageCount - 1));
  }, [pageCount]);
  useEffect(() => setPage(0), [scopeKey]);
  const toggleSort = useCallback((key: SortKey) => {
    setSort((current) => current?.key === key
      ? { key, direction: current.direction === "desc" ? "asc" : "desc" }
      : { key, direction: "desc" });
    setPage(0);
  }, []);
  const sortedEvents = useMemo(() => {
    if (!sort) return events;
    return events.map((event, index) => ({ event, index, value: sortValue(event, sort.key) }))
      .sort((a, b) => {
        if (a.value === null && b.value === null) return a.index - b.index;
        if (a.value === null) return 1;
        if (b.value === null) return -1;
        const difference = a.value - b.value;
        return difference === 0 ? a.index - b.index : sort.direction === "desc" ? -difference : difference;
      })
      .map(({ event }) => event);
  }, [events, sort]);
  const visibleEvents = useMemo(
    () => sortedEvents.slice(page * pageSize, (page + 1) * pageSize),
    [page, sortedEvents],
  );
  const columns = useMemo<ColumnDef<RadarEvent>[]>(() => [
    {
      id: "event",
      header: "研判对象",
      cell: ({ row }) => (
        <button type="button" className="event-cell event-select" aria-current={row.original.id === selectedId} onClick={() => onSelect(row.original.id)}>
          <div className="event-title">{mode === "latest" && row.original.latestEvidenceAt && Date.now() - new Date(row.original.latestEvidenceAt).getTime() <= 20 * 60_000 && <span className="new-arrival-badge">新到</span>}{row.original.title}</div>
          <div className="event-meta"><span>{typeName[row.original.eventType]}</span><span>{row.original.independentSources} 个独立信源</span><span>首见 {timeAgo(row.original.firstSeen)}</span><span>{mode === "latest" ? "采集" : "变化"} {timeAgo(mode === "latest" ? (row.original.latestEvidenceAt ?? row.original.updatedAt) : row.original.updatedAt)}</span></div>
          <div className="row-labels">{row.original.classificationStatus === "unsupported" && <span>未支持分类</span>}{row.original.labels.slice(0, 3).map((label) => <span key={label}>{labelName[label]}</span>)}</div>
        </button>
      ),
    },
    { id: "state", header: () => <SortHeader label="阶段" sortKey="state" sort={sort} onToggle={toggleSort} />, cell: ({ row }) => <span className={`state-pill state-${row.original.state}`}>{stateName[row.original.state]}</span> },
    { id: "attention", header: () => <SortHeader label="讨论" sortKey="attention" sort={sort} onToggle={toggleSort} />, cell: ({ row }) => <span className="metric-value metric-discussion">{discussionObserved(row.original) ? row.original.attention : "N/A"}</span> },
    { id: "behavior", header: () => <SortHeader label="行为" sortKey="behavior" sort={sort} onToggle={toggleSort} />, cell: ({ row }) => <span className="metric-value metric-behavior">{behaviorObserved(row.original) ? row.original.behavior : "N/A"}</span> },
    { id: "trend", header: `${windowSize} 轨迹`, cell: ({ row }) => <Sparkline event={row.original} windowSize={windowSize} /> },
    { id: "evidence", header: () => <SortHeader label="证据" sortKey="evidence" sort={sort} onToggle={toggleSort} />, cell: ({ row }) => {
      const missing = [row.original.discussionEvidenceState === "missing" ? "讨论" : "", row.original.behaviorEvidenceState === "missing" ? "行为" : ""].filter(Boolean);
      return <div className="confidence"><span>{({ low: "低", medium: "中", high: "高" })[row.original.evidenceStrength]} · {row.original.evidenceCount ?? row.original.evidence.length} 项</span><i><em style={{ width: `${row.original.evidenceScore}%` }} /></i><small>{missing.length ? `缺 ${missing.join("、")}` : "关键轴已覆盖"}</small></div>;
    } },
    { id: "newEvidence", header: () => <SortHeader label="新增证据" sortKey="newEvidence" sort={sort} onToggle={toggleSort} />, cell: ({ row }) => <div className="priority-reason"><b>+{row.original.newEvidenceCount ?? 0}</b><small>{row.original.queuePriorityReasons?.[0] ?? "常规复核"}</small></div> },
    { id: "velocity", header: () => <SortHeader label="速度" sortKey="velocity" sort={sort} onToggle={toggleSort} />, cell: ({ row }) => <span className="velocity">{row.original.velocity > 0 ? "+" : ""}{row.original.velocity}</span> },
  ], [mode, onSelect, selectedId, sort, toggleSort, windowSize]);
  // TanStack Table intentionally returns callable table state; it is safe here because
  // the instance remains local and no returned function crosses a memoized boundary.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({ data: visibleEvents, columns, getCoreRowModel: getCoreRowModel() });

  return (
    <div className="queue-table-frame">
      <div className="table-scroll">
        <table className="queue-table">
          <thead>{table.getHeaderGroups().map((group) => <tr key={group.id}>{group.headers.map((header) => <th key={header.id} aria-sort={sort?.key === header.column.id ? (sort.direction === "asc" ? "ascending" : "descending") : undefined}>{flexRender(header.column.columnDef.header, header.getContext())}</th>)}</tr>)}</thead>
          <tbody>{table.getRowModel().rows.map((row) => (
            <tr key={row.id} className={row.original.id === selectedId ? "selected" : ""}>
              {row.getVisibleCells().map((cell) => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
            </tr>
          ))}{table.getRowModel().rows.length === 0 && <tr><td colSpan={columns.length} className="empty-directory">没有符合当前筛选条件的事件。</td></tr>}</tbody>
        </table>
      </div>
      {events.length > pageSize && <nav className="queue-pagination" aria-label="研判队列分页">
        <span>第 {page + 1} / {pageCount} 页</span>
        <div>
          <button type="button" disabled={page === 0} onClick={() => setPage((current) => current - 1)}>上一页</button>
          <button type="button" disabled={page >= pageCount - 1} onClick={() => setPage((current) => current + 1)}>下一页</button>
        </div>
      </nav>}
    </div>
  );
}
