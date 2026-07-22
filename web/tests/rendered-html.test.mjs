import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function render() {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);
  return worker.fetch(new Request("http://localhost/", { headers: { accept: "text/html" } }), {
    ASSETS: { fetch: async () => new Response("Not found", { status: 404 }) },
  }, { waitUntil() {}, passThroughOnException() {} });
}

test("server-renders the intelligence workspace", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);
  const html = await response.text();
  assert.match(html, /<title>SIGNAL\/\/AI · 热点研判台<\/title>/i);
  assert.match(html, /AI 热点研判队列/);
  assert.match(html, /研判队列/);
  assert.match(html, /信源治理/);
  assert.match(html, /RECORDED DEMO/);
  assert.match(html, /覆盖置信度/);
  assert.match(html, /开发者与研究生态信号 Beta/);
  assert.match(html, /加速 \/ 已建立/);
  assert.match(html, /只看关注/);
});

test("ships the required product views and responsive safety", async () => {
  const [shell, table, css, data] = await Promise.all([
    readFile(new URL("../app/components/RadarShell.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/components/QueueTable.tsx", import.meta.url), "utf8"),
    readFile(new URL("../app/globals.css", import.meta.url), "utf8"),
    readFile(new URL("../app/lib/demo-data.ts", import.meta.url), "utf8"),
  ]);
  for (const label of ["研判队列", "信号雷达", "信源治理", "数据覆盖", "方法与边界"]) assert.match(shell, new RegExp(label));
  assert.match(shell, /NEXT_PUBLIC_API_URL/);
  assert.match(shell, /data\.dataMode === "recorded_demo"/);
  assert.match(shell, /windowSize\.toLowerCase\(\)/);
  assert.match(shell, /windowName\[windowSize\]/);
  assert.match(table, /header: `\$\{windowSize\} 轨迹`/);
  assert.match(shell, /detailAssessment/);
  assert.match(shell, /\/api\/v1\/watchlists/);
  assert.match(shell, /只看关注/);
  assert.match(shell, /ClusterEditDialog/);
  assert.match(shell, /中文讨论覆盖不足/);
  assert.match(shell, /SourceScore 排行等待真实结果集校准|等待真实历史结果集校准/);
  assert.match(shell, /candidateScore === null \? "N\/A"/);
  assert.match(shell, /new URLSearchParams\(\{ limit: "500", offset:/);
  assert.match(shell, /source-pagination/);
  assert.match(shell, /mobileDetail[\s\S]*Dialog\.Content/);
  assert.match(shell, /discussionEvidenceState === undefined/);
  assert.match(shell, /未复核候选，不参与评分/);
  assert.match(shell, /provenanceLevel !== "unverified_discovery"/);
  assert.match(table, /behaviorEvidenceState === undefined/);
  assert.doesNotMatch(shell, /¥1,184|2 个状态刚切换/);
  assert.match(shell, /prefers-reduced-motion|detail-panel/);
  assert.match(css, /prefers-reduced-motion:\s*reduce/);
  assert.match(css, /@media \(max-width: 760px\)/);
  assert.match(data, /attention_behavior_gap/);
  assert.match(data, /platform_concentrated/);
  assert.match(data, /expected_behavior_lag/);
});
