import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page, type Route } from "@playwright/test";
import { demoPayload } from "../../app/lib/demo-data";

const now = new Date().toISOString();
const receipt = (operation: string) => ({
  id: crypto.randomUUID(), status: "completed", operation, createdAt: now,
});

async function mockApi(page: Page, options: { radarStatus?: number } = {}) {
  await page.addInitScript(() => {
    class StableEventSource {
      static readonly CONNECTING = 0;
      static readonly OPEN = 1;
      static readonly CLOSED = 2;
      readonly CONNECTING = 0;
      readonly OPEN = 1;
      readonly CLOSED = 2;
      readonly readyState = 1;
      readonly url: string;
      readonly withCredentials = false;
      onopen = null;
      onmessage = null;
      onerror = null;
      constructor(url: string | URL) { this.url = String(url); }
      addEventListener() {}
      removeEventListener() {}
      dispatchEvent() { return true; }
      close() {}
    }
    Object.defineProperty(window, "EventSource", { value: StableEventSource, configurable: true });
  });
  await page.route("http://127.0.0.1:8017/**", async (route: Route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    if (path === "/api/v1/radar") {
      if (options.radarStatus) {
        await route.fulfill({ status: options.radarStatus, json: { detail: "radar unavailable" } });
        return;
      }
      const sort = url.searchParams.get("sort") === "priority" ? "priority" : "latest";
      await route.fulfill({ json: { ...demoPayload, generatedAt: now, dataMode: "live", sort } });
      return;
    }
    if (path === "/api/v1/stream") {
      await route.fulfill({ status: 200, contentType: "text/event-stream", body: ": ready\n\n" });
      return;
    }
    if (path === "/api/v1/watchlists" && request.method() === "GET") {
      await route.fulfill({ json: { items: [] } });
      return;
    }
    if (path === "/api/v1/coverage") {
      await route.fulfill({ json: { budget: { currency: "CNY", spent: 0, limit: 2000, remaining: 2000 } } });
      return;
    }
    if (/\/api\/v1\/events\/[^/]+\/lineage$/.test(path)) {
      const eventId = decodeURIComponent(path.split("/")[4]);
      await route.fulfill({ json: { eventId, clusterVersion: 1, supersededBy: [], parents: [], children: [], currentObservationCount: 3, pendingOperations: [] } });
      return;
    }
    if (/\/api\/v1\/events\/[^/]+$/.test(path)) {
      await route.fulfill({ json: {
        assessment: null,
        decisionContext: { queueEligibilityKey: "queue:test:1", alertDeliveryKey: "alert:test:1", capturedAt: now },
      } });
      return;
    }
    const operation = path.includes("watchlists") ? "watchlist.create"
      : path.includes("feedback") ? "feedback.create"
        : path.includes("interactions") ? "interaction.create" : "test.create";
    await route.fulfill({ json: receipt(operation) });
  });
}

async function assertNoWcagAaViolations(page: Page) {
  const result = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  expect(result.violations, JSON.stringify(result.violations, null, 2)).toEqual([]);
}

test("desktop keyboard flow and chart data alternative pass WCAG AA scan", async ({ page }) => {
  await mockApi(page);
  await page.goto("/");
  await expect(page.getByText("LIVE PIPELINE")).toBeVisible();

  await page.keyboard.press("/");
  await expect(page.getByLabel("搜索事件")).toBeFocused();
  const firstRow = page.locator(".event-select").first();
  await firstRow.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByLabel("事件研判详情")).toBeVisible();

  await page.getByRole("button", { name: "信号雷达" }).click();
  await page.getByText("查看图表数据表").click();
  await expect(page.getByRole("table").last()).toContainText("讨论");
  await assertNoWcagAaViolations(page);
});

test("review queue separates latest arrivals from priority triage", async ({ page }) => {
  await mockApi(page);
  await page.goto("/");
  await expect(page.getByText("LIVE PIPELINE")).toBeVisible();
  const latest = page.getByRole("button", { name: "最新进入" });
  const priority = page.getByRole("button", { name: "研判优先" });
  await expect(latest).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText("新到不等于热点")).toBeVisible();
  await priority.click();
  await expect(priority).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText("优先级不等于最终结论")).toBeVisible();
});

test("radar failure shows an honest offline state without demo rows", async ({ page }) => {
  await mockApi(page, { radarStatus: 503 });
  await page.goto("/");
  await expect(page.getByText("DATA OFFLINE")).toBeVisible();
  await expect(page.getByText("真实数据暂不可用", { exact: true })).toBeVisible();
  await expect(page.locator(".event-select")).toHaveCount(0);
  await expect(page.getByText("RECORDED DEMO")).toHaveCount(0);
});

test("review queue sorts the full result set from table headers", async ({ page }) => {
  await mockApi(page);
  await page.goto("/");
  await expect(page.getByText("LIVE PIPELINE")).toBeVisible();
  for (const label of ["阶段", "证据", "速度", "新增证据", "行为", "讨论"]) {
    await expect(page.locator("thead").getByRole("button", { name: new RegExp(`^${label}当前`) })).toBeVisible();
  }
  const discussionSort = page.locator("thead").getByRole("button", { name: /^讨论当前/ });
  await discussionSort.click();
  await expect(discussionSort).toHaveAccessibleName("讨论当前高到低，点击切换为低到高");
  await expect(page.locator(".event-title").first()).toContainText("开放权重多模态模型");
  await discussionSort.click();
  await expect(discussionSort).toHaveAccessibleName("讨论当前低到高，点击切换为高到低");
  await expect(page.locator(".event-title").first()).toContainText("单平台 AI 视频演示");
});

test("mobile user can filter, watch and accept with dialog focus containment", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await mockApi(page);
  await page.goto("/");

  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeHidden();
  await expect(page.getByText("LIVE", { exact: true })).toBeVisible();

  await page.getByLabel("打开导航").click();
  await expect(page.getByLabel("关闭导航")).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(page.getByLabel("打开导航")).toBeFocused();
  await expect(page.locator("#mobile-navigation")).toHaveAttribute("aria-hidden", "true");
  await page.getByLabel("打开导航").click();
  await page.getByRole("button", { name: "数据覆盖" }).click();
  await expect(page.getByRole("heading", { name: "覆盖与连接器" })).toBeVisible();
  await page.getByLabel("打开导航").click();
  await page.getByRole("button", { name: /研判队列/ }).click();

  await page.getByRole("button", { name: /筛选/ }).click();
  await page.getByRole("dialog").getByLabel("事件类型").selectOption("security_incident");
  await page.getByRole("button", { name: "应用筛选" }).click();
  await expect(page.locator(".event-select")).toHaveCount(1);

  const firstRow = page.locator(".event-select").first();
  await firstRow.focus();
  await page.keyboard.press("Enter");
  await expect(dialog).toBeVisible();
  await expect(page.getByRole("button", { name: "接受", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "关注事件" }).click();
  await expect(page.getByRole("button", { name: "取消关注" })).toBeVisible();
  await page.getByRole("button", { name: "接受", exact: true }).click();
  await expect(page.getByRole("status")).toContainText("已接受");
  await assertNoWcagAaViolations(page);
});
