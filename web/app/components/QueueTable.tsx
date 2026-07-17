"use client";

import { flexRender, getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import { useMemo } from "react";
import type { RadarEvent } from "../lib/types";

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

const timeAgo = (iso: string) => {
  const minutes = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 60_000));
  if (minutes < 60) return `${Math.max(1, minutes)} 分钟前`;
  if (minutes < 1440) return `${Math.round(minutes / 60)} 小时前`;
  return `${Math.round(minutes / 1440)} 天前`;
};

function Sparkline({ event, windowSize }: { event: RadarEvent; windowSize: string }) {
  const width = 78;
  const height = 28;
  const path = (key: "attention" | "behavior") => event.timeline.map((p, index) => {
    const x = event.timeline.length === 1 ? 0 : (index / (event.timeline.length - 1)) * width;
    const y = height - (p[key] / 100) * height;
    return `${index ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`;
  }).join(" ");
  const hasDiscussion = event.discussionEvidenceState === undefined ? event.evidence.some((item) => item.kind === "discussion") : event.discussionEvidenceState === "observed";
  const hasBehavior = event.behaviorEvidenceState === undefined ? event.evidence.some((item) => item.kind === "behavior") : event.behaviorEvidenceState === "observed";
  return (
    <svg className="sparkline" viewBox={`0 0 ${width} ${height}`} aria-label={`${windowSize} 可用信号走势`}>
      {hasDiscussion && <path d={path("attention")} className="spark-attention" />}
      {hasBehavior && <path d={path("behavior")} className="spark-behavior" />}
    </svg>
  );
}

export function QueueTable({ events, selectedId, windowSize, onSelect }: { events: RadarEvent[]; selectedId: string; windowSize: string; onSelect: (id: string) => void }) {
  const columns = useMemo<ColumnDef<RadarEvent>[]>(() => [
    {
      id: "event",
      header: "研判对象",
      cell: ({ row }) => (
        <button type="button" className="event-cell event-select" aria-current={row.original.id === selectedId} onClick={() => onSelect(row.original.id)}>
          <div className="event-title">{row.original.title}</div>
          <div className="event-meta"><span>{typeName[row.original.eventType]}</span><span>{row.original.independentSources} 个独立信源</span><span>首见 {timeAgo(row.original.firstSeen)}</span><span>变化 {timeAgo(row.original.updatedAt)}</span></div>
          <div className="row-labels">{row.original.classificationStatus === "unsupported" && <span>未支持分类</span>}{row.original.labels.slice(0, 3).map((label) => <span key={label}>{labelName[label]}</span>)}</div>
        </button>
      ),
    },
    { accessorKey: "state", header: "阶段", cell: ({ row }) => <span className={`state-pill state-${row.original.state}`}>{stateName[row.original.state]}</span> },
    { id: "signal", header: "讨论 / 行为", cell: ({ row }) => {
      const hasDiscussion = row.original.discussionEvidenceState === undefined ? row.original.evidence.some((item) => item.kind === "discussion") : row.original.discussionEvidenceState === "observed";
      const hasBehavior = row.original.behaviorEvidenceState === undefined ? row.original.evidence.some((item) => item.kind === "behavior") : row.original.behaviorEvidenceState === "observed";
      return <div className="metric-pair"><b>{hasDiscussion ? row.original.attention : "N/A"}</b><span>/</span><b>{hasBehavior ? row.original.behavior : "N/A"}</b></div>;
    } },
    { id: "trend", header: `${windowSize} 轨迹`, cell: ({ row }) => <Sparkline event={row.original} windowSize={windowSize} /> },
    { accessorKey: "evidenceStrength", header: "证据 / 缺口", cell: ({ row }) => {
      const missing = [row.original.discussionEvidenceState === "missing" ? "讨论" : "", row.original.behaviorEvidenceState === "missing" ? "行为" : ""].filter(Boolean);
      return <div className="confidence"><span>{({ low: "低", medium: "中", high: "高" })[row.original.evidenceStrength]} · {row.original.evidenceCount ?? row.original.evidence.length} 项</span><i><em style={{ width: `${row.original.evidenceScore}%` }} /></i><small>{missing.length ? `缺 ${missing.join("、")}` : "关键轴已覆盖"}</small></div>;
    } },
    { id: "priority", header: "新增证据 / 优先理由", cell: ({ row }) => <div className="priority-reason"><b>+{row.original.newEvidenceCount ?? 0}</b><small>{row.original.queuePriorityReasons?.[0] ?? "常规复核"}</small></div> },
    { accessorKey: "velocity", header: "速度", cell: ({ row }) => <span className="velocity">{row.original.velocity > 0 ? "+" : ""}{row.original.velocity}</span> },
  ], [onSelect, selectedId, windowSize]);
  // TanStack Table intentionally returns callable table state; it is safe here because
  // the instance remains local and no returned function crosses a memoized boundary.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({ data: events, columns, getCoreRowModel: getCoreRowModel() });

  return (
    <div className="table-scroll">
      <table className="queue-table">
        <thead>{table.getHeaderGroups().map((group) => <tr key={group.id}>{group.headers.map((header) => <th key={header.id}>{flexRender(header.column.columnDef.header, header.getContext())}</th>)}</tr>)}</thead>
        <tbody>{table.getRowModel().rows.map((row) => (
          <tr key={row.id} className={row.original.id === selectedId ? "selected" : ""}>
            {row.getVisibleCells().map((cell) => <td key={cell.id}>{flexRender(cell.column.columnDef.cell, cell.getContext())}</td>)}
          </tr>
        ))}{table.getRowModel().rows.length === 0 && <tr><td colSpan={columns.length} className="empty-directory">没有符合当前筛选条件的事件。</td></tr>}</tbody>
      </table>
    </div>
  );
}
