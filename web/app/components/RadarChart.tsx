"use client";

import { ScatterChart } from "echarts/charts";
import { GridComponent, MarkAreaComponent, TooltipComponent } from "echarts/components";
import { init, use as registerModules } from "echarts/core";
import { CanvasRenderer } from "echarts/renderers";
import { useEffect, useRef } from "react";
import type { RadarEvent } from "../lib/types";

registerModules([ScatterChart, GridComponent, MarkAreaComponent, TooltipComponent, CanvasRenderer]);

const stateColor: Record<string, string> = {
  accelerating: "#16866f",
  established: "#2855d9",
  emerging: "#b7791f",
  detected: "#6d5bd0",
  cooling: "#7a8391",
  insufficient_data: "#9aa1ad",
  dormant: "#9aa1ad",
  noise: "#b2b7c0",
};

export function RadarChart({ events, onSelect }: { events: RadarEvent[]; onSelect: (id: string) => void }) {
  const host = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!host.current) return;
    const chart = init(host.current, undefined, { renderer: "canvas" });
    const reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const points = events.map((event) => ({
      value: [event.attention, event.behavior, event.evidenceScore, event.id, event.evidenceStrength],
      name: event.title,
      itemStyle: { color: stateColor[event.state] ?? "#41b8ff" },
    }));
    chart.setOption({
      animation: !reducedMotion,
      animationDuration: reducedMotion ? 0 : 180,
      animationEasing: "cubicOut",
      grid: { left: 54, right: 22, top: 26, bottom: 46 },
      tooltip: {
        trigger: "item",
        renderMode: "richText",
        borderColor: "#d8dce3",
        backgroundColor: "rgba(255, 255, 255, .98)",
        textStyle: { color: "#20242c", fontSize: 12 },
        extraCssText: "box-shadow: 0 12px 32px rgba(28, 39, 58, .12); border-radius: 8px;",
        formatter: (p: { name?: string; value?: unknown }) => {
          const v = p.value as [number, number, number, string, "low" | "medium" | "high"];
          const tier = { low: "低", medium: "中", high: "高" }[v[4]];
          return `${p.name ?? ""}\n讨论 ${v[0]} · 行为 ${v[1]}\n证据强度 ${tier}`;
        },
      },
      xAxis: {
        min: 0,
        max: 100,
        name: "讨论度  ATTENTION →",
        nameLocation: "middle",
        nameGap: 31,
        axisLine: { lineStyle: { color: "#c9ced7" } },
        splitLine: { lineStyle: { color: "rgba(74, 84, 101, .10)" } },
        axisLabel: { color: "#7a8391" },
        nameTextStyle: { color: "#596271", fontSize: 10, fontFamily: "monospace" },
      },
      yAxis: {
        min: 0,
        max: 100,
        name: "行为趋势  BEHAVIOR ↑",
        nameGap: 22,
        axisLine: { show: true, lineStyle: { color: "#c9ced7" } },
        splitLine: { lineStyle: { color: "rgba(74, 84, 101, .10)" } },
        axisLabel: { color: "#7a8391" },
        nameTextStyle: { color: "#596271", fontSize: 10, fontFamily: "monospace" },
      },
      series: [{
        type: "scatter",
        data: points,
        symbolSize: (value: number[]) => 11 + value[2] * 0.18,
        emphasis: { scale: 1.08, itemStyle: { shadowBlur: 10, shadowColor: "rgba(40,85,217,.20)" } },
        markArea: {
          silent: true,
          label: { color: "rgba(80, 89, 103, .58)", fontSize: 10 },
          itemStyle: { color: "rgba(40,85,217,.025)" },
          data: [
            [{ name: "低讨论 / 高行为", xAxis: 0, yAxis: 55 }, { xAxis: 55, yAxis: 100 }],
            [{ name: "同步增长", xAxis: 55, yAxis: 55 }, { xAxis: 100, yAxis: 100 }],
            [{ name: "低信号", xAxis: 0, yAxis: 0 }, { xAxis: 55, yAxis: 55 }],
            [{ name: "讨论 / 行为剪刀差", xAxis: 55, yAxis: 0 }, { xAxis: 100, yAxis: 55 }],
          ],
        },
      }],
    });
    chart.on("click", (params) => {
      const value = params.value as [number, number, number, string] | undefined;
      if (value?.[3]) onSelect(value[3]);
    });
    const resize = () => chart.resize();
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
      chart.dispose();
    };
  }, [events, onSelect]);

  return <div ref={host} className="radar-chart" role="img" aria-label="AI 事件讨论度与行为趋势二维雷达图" />;
}
