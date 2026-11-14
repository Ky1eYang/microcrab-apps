#!/usr/bin/env node
/** Real-process browser acceptance. Parent starts browser_fixture.py and builds
 * artifacts beforehand. This script never intercepts requests or starts servers.
 * Diagnostics deliberately omit URLs, exceptions, request bodies and credentials.
 */
import { readFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repository = fileURLToPath(new URL("../../", import.meta.url));
const screenshot = resolve(repository, "target/tmp/v013-browser-smoke.png");
let browser;
let page;
let stage = "arguments";
let spaReady = false;
let failure = false;
const pageErrors = [];
const checked = [];
const unavailable = [];
const sockets = [];
let websocketCount = 0;
let socketClosedCount = 0;
let credentialQueryCount = 0;

try {
  const args = process.argv.slice(2);
  if (args.length !== 2 || args[0] !== "--ready-file" || !args[1]) {
    throw new Error("ready-file argument required");
  }
  stage = "ready-metadata";
  const metadata = JSON.parse(await readFile(resolve(args[1]), "utf8"));
  const origin = new URL(metadata.url);
  const login = new URL(metadata.login_url);
  if (
    !["http:", "https:"].includes(origin.protocol) ||
    !["127.0.0.1", "localhost", "[::1]"].includes(origin.hostname) ||
    origin.username || origin.password || origin.search || origin.hash ||
    login.origin !== origin.origin || login.pathname !== "/api/auth/login" ||
    login.search || login.hash || login.username || login.password ||
    typeof metadata.project_id !== "string" || !metadata.project_id ||
    typeof metadata.root !== "string" || !metadata.root
  ) {
    throw new Error("invalid public fixture metadata");
  }
  const { chromium, expect: baseExpect } = await import(pathToFileURL(resolve(
    repository, "node_modules/@playwright/test/index.mjs",
  )).href);
  const expect = baseExpect.configure({ timeout: 20_000 });
  stage = "chromium-launch";
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1050 } });
  page = await context.newPage();
  page.setDefaultTimeout(20_000);
  page.setDefaultNavigationTimeout(30_000);
  page.on("pageerror", error => {
    // Only a short allowlisted class name is retained; messages can contain URLs.
    if (pageErrors.length < 12) pageErrors.push(
      ["Error", "TypeError", "ReferenceError", "SyntaxError", "RangeError"].includes(error.name)
        ? error.name : "PageError",
    );
  });
  page.on("websocket", socket => {
    websocketCount++;
    sockets.push(socket);
    if (new URL(socket.url()).search) credentialQueryCount++;
    socket.on("close", () => { socketClosedCount++; });
  });

  stage = "actual-oidc-login";
  // Follow the real issuer redirects and callback; never inspect or print URLs.
  await page.goto(login.href, { waitUntil: "domcontentloaded" });
  await page.waitForURL(url => url.origin === origin.origin && url.pathname === "/");
  const project = page.getByLabel("已分配项目");
  await expect(project).toBeVisible();
  await project.selectOption(metadata.project_id);
  await expect(project).toHaveValue(metadata.project_id);
  await expect(page.getByLabel("运行模式")).toHaveValue("plan");
  spaReady = true;
  checked.push("actual_oidc_cookie_login", "assigned_project", "initial_plan_mode");

  stage = "create-submit-tool-and-canonical-answer";
  const message = page.getByLabel("消息", { exact: true });
  await expect(message).toBeEnabled();
  await message.fill("lifecycle-one");
  await page.getByTitle("发送 (Enter)").click();
  await page.waitForURL(url => url.origin === origin.origin && url.hash.startsWith("#session/"));
  await expect(page.locator('.block[data-role="user"]').getByText("lifecycle-one", { exact: true })).toBeVisible();
  const answer = page.locator('.block[data-role="assistant"]').getByText("canonical:lifecycle-one", { exact: true });
  await expect(answer).toBeVisible({ timeout: 45_000 });
  await expect(message).toBeEnabled();
  const sessionHash = new URL(page.url()).hash;
  if (!/^#session\/[A-Za-z0-9_-]+$/.test(sessionHash)) throw new Error("invalid native session route");
  checked.push("ui_session_create", "real_guest_submit", "canonical_assistant_answer");

  async function assertReadTool() {
    const parentStage = stage;
    stage = `${parentStage}:read-group`;
    const group = page.locator(".tool-accordion-group").filter({ hasText: "read" }).first();
    await expect(group).toBeVisible();
    if (await group.locator(".tool-accordion-items").count() === 0) {
      await group.locator(".tool-accordion-group-header").click();
    }
    stage = `${parentStage}:read-card`;
    const read = group.locator(".tool-accordion-row").filter({ has: page.locator(".tool-name", { hasText: /^read$/ }) }).first();
    await expect(read).toHaveAttribute("data-status", "completed");
    if (await read.getAttribute("data-expanded") !== "true") await read.locator(".tool-accordion-header").click();
    stage = `${parentStage}:read-arguments`;
    const argumentsSection = read.locator(".tool-body-section").filter({
      has: page.locator(".tool-body-section-label", { hasText: /^Arguments$/ }),
    });
    await expect(argumentsSection).toHaveCount(1);
    await expect(argumentsSection.locator("pre.tool-code-pre")).toContainText("seed.txt");
    stage = `${parentStage}:read-output`;
    const outputSection = read.locator(".tool-body-section").filter({
      has: page.locator(".tool-body-section-label", { hasText: /^Output$/ }),
    });
    await expect(outputSection.locator("pre.tool-output-pre")).toContainText("synthetic-seed-content");
    stage = parentStage;
  }
  await assertReadTool();
  checked.push("canonical_read_tool_arguments_and_output");

  stage = "reload-canonical-reattach";
  const initialSockets = websocketCount;
  await page.reload({ waitUntil: "domcontentloaded" });
  await expect(answer).toBeVisible({ timeout: 30_000 });
  await expect(page.locator('.block[data-role="user"]').getByText("lifecycle-one", { exact: true })).toHaveCount(1);
  await expect(answer).toHaveCount(1);
  await expect.poll(() => websocketCount).toBeGreaterThan(initialSockets);
  if (new URL(page.url()).hash !== sessionHash) throw new Error("session route lost on reload");
  await assertReadTool();
  checked.push("reload_new_socket_canonical_reattach", "no_duplicate_user_or_answer");

  stage = "native-mode-controls";
  const mode = page.getByLabel("运行模式");
  await expect(mode).toBeEnabled();
  if (await mode.locator('option[value="build"]').count()) {
    await mode.selectOption("build"); await expect(mode).toHaveValue("build");
    await mode.selectOption("plan"); await expect(mode).toHaveValue("plan");
    checked.push("actual_mode_build_then_plan");
  } else unavailable.push("build_mode_not_in_actual_guest_catalog");

  stage = "policy-load-current";
  const controls = page.getByRole("button", { name: /权限 \/ 历史 \/ Jobs/ });
  await expect(controls).toBeVisible();
  await expect(controls).toBeEnabled();
  await controls.click();
  const policySummary = page.locator("summary").filter({ hasText: "Runtime 权限配置" });
  await expect(policySummary).toBeVisible();
  await policySummary.click();
  const loadCurrent = page.getByRole("button", { name: "载入当前配置", exact: true });
  await expect(loadCurrent).toBeVisible();
  await expect(loadCurrent).toBeEnabled();
  await loadCurrent.click();
  const save = page.getByRole("button", { name: "按当前 revision 更新", exact: true });
  await expect(save).toBeEnabled();
  const revisionBefore = await policySummary.innerText();
  const overrides = page.getByLabel("权限 overrides JSON");
  // Mode edits update the current policy revision while the editor preserves its
  // draft. Explicitly load current first, just as a user resolving stale CAS does.
  const actualOverrides = await overrides.inputValue();
  JSON.parse(actualOverrides);
  await overrides.fill(actualOverrides);
  stage = "policy-cas";
  await save.click();
  await expect.poll(() => policySummary.innerText()).not.toBe(revisionBefore);
  checked.push("actual_policy_revision_cas");

  stage = "history-undo";
  const undo = page.getByRole("button", { name: "撤销", exact: true });
  const redo = page.getByRole("button", { name: "重做", exact: true });
  await expect(undo).toBeVisible();
  await expect(undo).toBeEnabled();
  await expect(redo).toBeVisible();
  await undo.click();
  await expect(answer).toHaveCount(0);
  stage = "history-redo";
  await expect(redo).toBeEnabled();
  await redo.click();
  await expect(answer).toBeVisible();
  await assertReadTool();
  checked.push("actual_history_backward_forward");

  stage = "canonical-screenshot";
  await mkdir(dirname(screenshot), { recursive: true });
  await page.screenshot({ path: screenshot, fullPage: true, timeout: 10_000 });
  checked.push("canonical_ui_screenshot");

  stage = "logout-and-sign-in-view";
  const logoutMethods = [];
  page.on("request", request => {
    const target = new URL(request.url());
    if (target.origin === origin.origin && target.pathname === "/api/auth/logout") logoutMethods.push(request.method());
  });
  const activeSocket = sockets.at(-1);
  if (!activeSocket || activeSocket.isClosed()) throw new Error("active application socket missing before logout");
  await page.locator(".user-card-btn").first().click();
  await page.getByRole("button", { name: "退出登录", exact: true }).click();
  await expect(page.getByRole("button", { name: "登录", exact: true }).first()).toBeVisible();
  await expect(page.locator(".user-card-name").first()).toHaveText("未登录");
  await expect(answer).toHaveCount(0);
  await expect.poll(() => activeSocket.isClosed()).toBe(true);
  expect(logoutMethods).toEqual(["POST"]);
  checked.push("actual_logout_post", "socket_closed_on_logout", "identity_cleared_sign_in_view");

  stage = "logged-out-reload";
  await page.reload({ waitUntil: "domcontentloaded" });
  await expect(page.getByRole("button", { name: "登录", exact: true }).first()).toBeVisible();
  await expect(page.locator(".user-card-name").first()).toHaveText("未登录");
  checked.push("logged_out_reload_stays_unauthenticated");
  expect(credentialQueryCount).toBe(0);
  expect(pageErrors).toEqual([]);
  console.log(JSON.stringify({ status: "passed", checked, unavailable, page_error_count: pageErrors.length, websocket_count: websocketCount, socket_closed_count: socketClosedCount, screenshot: "target/tmp/v013-browser-smoke.png" }));
} catch (error) {
  failure = true;
  // Never print Error.message/stack: Playwright errors can include auth URLs.
  console.error(JSON.stringify({ status: "failed", stage, failure_class: error?.name === "AssertionError" ? "AssertionError" : "BrowserSmokeError", page_error_count: pageErrors.length, page_error_classes: pageErrors, checked, unavailable }));
  if (spaReady && page && !page.isClosed()) {
    try {
      await mkdir(dirname(screenshot), { recursive: true });
      await page.screenshot({ path: screenshot, fullPage: true, timeout: 5_000 });
    } catch { /* Screenshot failure must not hide the original failing stage. */ }
  }
} finally {
  if (browser) {
    try { await browser.close(); }
    catch { failure = true; console.error('{"status":"failed","stage":"browser-cleanup"}'); }
  }
  if (failure) process.exitCode = 1;
}
