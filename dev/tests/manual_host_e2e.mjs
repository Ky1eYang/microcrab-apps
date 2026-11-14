#!/usr/bin/env node
/** Actual manual services only. No builds, service ownership, fixtures or provider probes.
 * Public metadata only; never print response bodies, credentials or error stacks.
 */
import {readFile, writeFile, realpath, readdir} from 'node:fs/promises';
import {resolve, join, relative, isAbsolute} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createHash, randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';

const checked = [];
let browser, stage = 'arguments', report, metadata, lastObserved;
const assert = (value, code) => { if (!value) throw Object.assign(new Error(code), {code}); };
const normalize = value => String(value).replace(/^\\\\\?\\/, '').replaceAll('\\', '/').replace(/\/$/, '').toLowerCase();
const digest = bytes => createHash('sha256').update(bytes).digest('hex');
async function privateTreeFingerprint(root) {
  const result = [];
  async function walk(directory) {
    let entries;
    try { entries = await readdir(directory, {withFileTypes: true}); }
    catch (error) { if (error.code === 'ENOENT') return; throw error; }
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(directory, entry.name);
      assert(!entry.isSymbolicLink(), 'old_bridge_link');
      if (entry.isDirectory()) await walk(path);
      else if (entry.isFile()) result.push([relative(root, path), digest(await readFile(path))]);
    }
  }
  await walk(root);
  return digest(JSON.stringify(result));
}
try {
  assert(process.argv.length === 4 && process.argv[2] === '--ready-file', 'arguments');
  metadata = JSON.parse(await readFile(resolve(process.argv[3]), 'utf8'));
  const {url, identity_url, bridge_url, project_id, project_name, subject} = metadata;
  assert(url === 'http://127.0.0.1:4096' && identity_url === 'http://127.0.0.1:8091', 'manual_urls');
  assert(bridge_url === 'ws://127.0.0.1:8789/host', 'manual_bridge_url');
  assert(subject === 'alice' && /^host-e2e-[a-z0-9-]+$/.test(project_id) && typeof project_name === 'string' && project_name.length > 0, 'metadata');
  assert(isAbsolute(metadata.root), 'absolute_root');
  const repository = await realpath(resolve('.'));
  const root = await realpath(metadata.root);
  const rootRelative = relative(repository, root).replaceAll('\\', '/');
  assert(/^test-dir\/host-e2e-[a-zA-Z0-9-]+$/.test(rootRelative), 'isolated_root');
  const oldBridge = join(repository, '.microcrab', 'host-bridge');
  const oldBefore = await privateTreeFingerprint(oldBridge);
  const identityPath = join(root, '.microcrab', 'host-bridge', 'identity.json');
  const identityBefore = await readFile(identityPath);
  const durable = JSON.parse(identityBefore);
  assert(normalize(durable.canonical_root) === normalize(root) && durable.host_id && durable.workspace_id, 'bridge_identity');
  const nonce = randomUUID().replaceAll('-', '');
  const proofName = 'host-e2e-proof-' + nonce + '.txt';
  const token = 'HOST_E2E_' + nonce;
  const {chromium} = await import(pathToFileURL(resolve('node_modules/@playwright/test/index.mjs')).href);
  browser = await chromium.launch({headless: true});
  const context = await browser.newContext();
  const page = await context.newPage();
  page.setDefaultTimeout(30000);
  const creates = [], commandErrors = [], methods = new Map();
  let approvalEvents = 0;
  page.on('request', request => {
    const target = new URL(request.url());
    if (target.origin === url && target.pathname === '/api/v1/sessions' && request.method() === 'POST') creates.push(request.postDataJSON());
  });
  page.on('websocket', socket => {
    socket.on('framesent', ({payload}) => {
      try { const frame = JSON.parse(String(payload)); if (frame.type === 'command') methods.set(frame.request_id, frame.method); } catch {}
    });
    socket.on('framereceived', ({payload}) => {
      try {
        const frame = JSON.parse(String(payload));
        if (frame.type === 'result' && frame.ok === false) commandErrors.push({method: methods.get(frame.request_id) ?? 'unknown', code: /^[a-z_]+$/.test(frame.error?.code ?? '') ? frame.error.code : 'operation_failed'});
        if (frame.type === 'event' && /^(permission_request|question|question_request|approval|permission\.request)$/.test(frame.event)) approvalEvents++;
        if (frame.type === 'event' && frame.event === 'turn' && ['failed', 'cancelled', 'interrupted', 'unknown'].includes(frame.data?.status)) commandErrors.push({method: 'turn.submit', code: 'turn_not_completed'});
      } catch {}
    });
  });
  async function http(path, method = 'GET', body) {
    return page.evaluate(async ({path, method, body}) => {
      const response = await fetch(path, {method, credentials: 'include', ...(body === undefined ? {} : {headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body)})});
      return {ok: response.ok, status: response.status, data: await response.json()};
    }, {path, method, body});
  }
  async function noApproval() {
    assert(approvalEvents === 0, 'approval_event');
    assert(await page.getByRole('button', {name: /^(Allow once|Always allow|Deny)$/}).count() === 0, 'approval_ui');
    assert(await page.getByRole('heading', {name: 'Question', exact: true}).count() === 0, 'question_ui');
  }
  stage = 'normal-oauth-login';
  await page.goto(url + '/login', {waitUntil: 'domcontentloaded'});
  await page.getByLabel('账户标识（可选）').fill(subject);
  await page.getByRole('button', {name: '登录', exact: true}).click();
  await page.waitForURL(target => target.origin === url && target.pathname === '/', {timeout: 30000});
  const identity = await http('/api/v1/identity');
  assert(identity.ok && identity.data.subject === subject && typeof identity.data.id === 'string', 'authenticated_subject');
  checked.push('normal_oauth_alice');

  stage = 'durable-project-create-or-reconcile';
  const projects = await http('/api/v1/projects');
  assert(projects.ok && Array.isArray(projects.data.items), 'project_list');
  const existing = projects.data.items.find(item => item.id === project_id);
  if (existing) {
    assert(existing.name === project_name && normalize(existing.root) === normalize(root), 'existing_project_scope');
    assert(!existing.host_id || existing.host_id === durable.host_id, 'existing_host_identity');
    assert(!existing.workspace_id || existing.workspace_id === durable.workspace_id, 'existing_workspace_identity');
    checked.push('existing_project_reconciled_without_replay');
  } else {
    const created = await http('/api/v1/projects', 'POST', {id: project_id, name: project_name, root, host_id: durable.host_id, workspace_id: durable.workspace_id});
    assert(created.ok && created.data.id === project_id && normalize(created.data.root) === normalize(root), 'project_create');
    checked.push('authenticated_api_project_created');
  }
  await page.reload({waitUntil: 'domcontentloaded'});
  const hostButton = page.getByTestId('header-host-connection');
  stage = 'no-token-ui-connect';
  await hostButton.click();
  const dialog = page.getByRole('dialog', {name: '连接项目 Host 执行端', exact: true});
  await dialog.getByRole('button', {name: 'Host WebSocket', exact: true}).click();
  await dialog.getByLabel('目标项目 (Project)', {exact: true}).selectOption(project_id);
  await dialog.getByLabel('Host WebSocket 地址', {exact: true}).fill(bridge_url);
  await dialog.getByLabel('一次性配对码', {exact: false}).fill('');
  stage = 'no-token-ui-submit';
  await dialog.getByRole('button', {name: '连接 Host', exact: true}).click();
  stage = 'no-token-ui-ready';
  await dialog.waitFor({state: 'hidden'});
  await hostButton.filter({hasText: '已就绪'}).waitFor();
  assert(normalize(await hostButton.getAttribute('title')) === normalize(root), 'ready_exact_root');
  const connected = await http('/api/v1/projects');
  const project = connected.data.items.find(item => item.id === project_id);
  assert(project?.host_connected === true && project.host_id === durable.host_id && project.workspace_id === durable.workspace_id, 'durable_project_identity');
  checked.push('empty_token_ui_connected_exact_workspace_and_identity');

  stage = 'universal-execute-catalog';
  const catalog = await http('/api/v1/projects/' + encodeURIComponent(project_id) + '/agents');
  assert(catalog.ok, 'agent_catalog');
  const agent = catalog.data.items.find(item => item.name === 'universal');
  const execute = agent?.modes.find(mode => mode.name === 'execute');
  const tool = agent?.tools.find(item => item.name === 'pwsh');
  assert(execute && tool && agent.component, 'universal_windows_execute');
  for (const capability of ['tool.invoke', 'process.execute']) assert(execute.permissions.some(permission => permission.tool?.kind === 'exact' && permission.tool.value === 'pwsh' && permission.capability === capability && permission.action === 'allow'), 'default_allow_' + capability.replace('.', '_'));
  const models = await http('/api/v1/models');
  assert(models.ok && models.data.default_model_selection?.provider_id && models.data.default_model_selection?.model, 'configured_default_model');
  const selection = models.data.default_model_selection;
  await page.getByTitle('快捷操作与工具', {exact: true}).click();
  await page.getByRole('button', {name: /^Agent · universal/}).click();
  checked.push('catalog_universal_execute_pwsh_default_allow');
  const commands = [
    `Set-Content -LiteralPath '${proofName}' -Value '${token}' -NoNewline; Get-Content -LiteralPath '${proofName}' -Raw`,
    `Get-Content -LiteralPath '${proofName}' -Raw`,
  ];
  const prompts = commands.map((command, index) => `This is authorized local Host E2E verification, turn ${index + 1} of 2. Use the advertised pwsh tool exactly once to execute this PowerShell command in the configured workspace root: ${command}\nSet cwd to ${root}, yield_ms to 10000 and timeout_ms to 10000. Do not use other tools or ask questions. After the command completes, briefly report its outcome. Do not simulate execution.`);
  let sessionId;
  async function canonicalIdle(index) {
    // This installed Playwright treats an async waitForFunction predicate's
    // Promise as truthy before its boolean settles. Poll authenticated HTTP from
    // Node so only an observed successful canonical snapshot ends the wait.
    const deadline = performance.now() + 300000;
    let snapshot;
    while (performance.now() < deadline) {
      const candidate = await http('/api/v1/sessions/' + encodeURIComponent(sessionId));
      assert(candidate.ok, 'canonical_snapshot_http');
      const value = candidate.data;
      const successes = value.messages?.filter(message => {
        if (message.role !== 'tool' || message.tool_name !== 'pwsh') return false;
        try { const output = JSON.parse(message.text); return output.exit_code === 0 && typeof output.stdout === 'string' && output.stdout.trim() === token; } catch { return false; }
      }) ?? [];
      const shellResults = value.messages?.filter(message => message.role === 'tool' && message.tool_name === 'pwsh') ?? [];
      const terminalErrors = shellResults.flatMap(message => {
        try { const output = JSON.parse(message.text); return output.error ? [output.error === 'permission denied' ? 'permission_denied' : 'tool_execution_failed_unknown_effect'] : []; } catch { return ['invalid_tool_result']; }
      });
      lastObserved = {session_id: sessionId, status: value.session?.status, user_messages: value.messages?.filter(message => message.role === 'user').length, shell_results: shellResults.length, successful_shell_results: successes.length, terminal_error_categories: terminalErrors};
      if (value.session?.status === 'idle' && terminalErrors.length) assert(false, terminalErrors[0]);
      if (value.session?.status === 'idle' && prompts.slice(0, index + 1).every(text => value.messages.some(message => message.role === 'user' && message.text === text)) && successes.length >= index + 1) {
        snapshot = candidate;
        break;
      }
      await noApproval();
      assert(commandErrors.length === 0, 'turn_failed_before_proof');
      await delay(250);
    }
    assert(snapshot, 'canonical_idle_deadline');
    assert(snapshot.ok && snapshot.data.session.agent === 'universal' && snapshot.data.session.mode === 'execute' && snapshot.data.session.project_id === project_id && snapshot.data.session.component === agent.component, 'session_pins');
    const actual = snapshot.data.session.model_selection;
    assert(actual?.provider_id === selection.provider_id && actual?.model === selection.model && (actual?.variant ?? null) === (selection.variant ?? null), 'default_model_preserved');
    const calls = snapshot.data.messages.flatMap(message => message.role === 'assistant' ? message.tool_calls ?? [] : []).filter(call => call.name === 'pwsh');
    const results = snapshot.data.messages.filter(message => message.role === 'tool' && message.tool_name === 'pwsh');
    assert(results.length === index + 1 && calls.length === index + 1, 'exact_bounded_shell_calls');
    for (const result of results) {
      assert(calls.some(call => call.id === result.call_id && call.name === 'pwsh'), 'canonical_call_result_link');
      const output = JSON.parse(result.text);
      assert(output.exit_code === 0 && output.stdout.trim() === token && output.truncated !== true, 'real_shell_output');
    }
    assert(await readFile(join(root, proofName), 'utf8') === token, 'actual_workspace_proof');
    const renderedShell = page.locator('.tool-accordion-row').filter({has: page.locator('.tool-name', {hasText: /^pwsh$/})});
    // Groups start collapsed; open only groups that contain the real shell name.
    const groups = page.locator('.tool-accordion-group').filter({hasText: 'pwsh'});
    await groups.first().waitFor();
    for (const group of await groups.all()) if (await group.locator('.tool-accordion-items').count() === 0) await group.locator('.tool-accordion-group-header').click();
    await renderedShell.first().waitFor();
    assert(await renderedShell.count() === index + 1, 'rendered_shell_count');
    for (const row of await renderedShell.all()) assert(await row.getAttribute('data-status') === 'completed', 'rendered_shell_completed');
    await noApproval();
    await page.getByTitle('发送 (Enter)', {exact: true}).waitFor();
    for (const text of prompts.slice(0, index + 1)) await page.locator('.block[data-role="user"]').getByText(text, {exact: true}).waitFor();
    return snapshot.data;
  }
  for (let index = 0; index < 2; index++) {
    stage = 'actual-shell-turn-' + (index + 1);
    await page.getByLabel('消息', {exact: true}).fill(prompts[index]);
    await page.getByTitle('发送 (Enter)', {exact: true}).click();
    await page.waitForURL(target => /^\/?session\/[^/]+$/.test(target.hash.slice(1) || target.pathname));
    const target = new URL(page.url());
    const id = decodeURIComponent((target.hash.slice(1) || target.pathname).match(/^\/?session\/([^/]+)$/)[1]);
    assert(!sessionId || sessionId === id, 'same_session');
    sessionId = id;
    await canonicalIdle(index);
    checked.push('turn_' + (index + 1) + '_real_pwsh_canonical_result_file_and_idle');
  }
  assert(creates.length === 1 && creates[0].agent === 'universal' && creates[0].project_id === project_id, 'one_ui_session_create');
  stage = 'refresh-resume';
  const sessionUrl = page.url();
  await page.reload({waitUntil: 'domcontentloaded'});
  await hostButton.filter({hasText: '已就绪'}).waitFor({timeout: 60000});
  assert(page.url() === sessionUrl && normalize(await hostButton.getAttribute('title')) === normalize(root), 'refresh_same_session_root');
  await canonicalIdle(1);
  assert(creates.length === 1, 'refresh_no_create');
  const finalProjects = await http('/api/v1/projects');
  const finalProject = finalProjects.data.items.find(item => item.id === project_id);
  assert(finalProject?.host_connected === true && finalProject.host_id === durable.host_id && finalProject.workspace_id === durable.workspace_id, 'refresh_identity_preserved');
  assert((await readFile(identityPath)).equals(identityBefore), 'durable_identity_unchanged');
  assert(await privateTreeFingerprint(oldBridge) === oldBefore, 'old_bridge_untouched');
  assert(commandErrors.length === 0, 'unexpected_command_error');
  checked.push('refresh_same_session_history_reconnected', 'new_durable_identity_preserved', 'old_bridge_tree_unchanged', 'no_approval_questions', 'exactly_two_real_turns');
  report = {status: 'passed', checked, project_id, session_id: sessionId, root, proof_file: proofName, tool: 'pwsh', real_tool_results: 2, session_create_requests: creates.length, old_bridge_unchanged: true};
  if (metadata.evidence_file) await writeFile(resolve(metadata.evidence_file), JSON.stringify(report, null, 2) + '\n', {encoding: 'utf8'});
  console.log(JSON.stringify(report));
} catch (error) {
  process.exitCode = 1;
  console.error(JSON.stringify({status: 'failed', stage, checked, failure_class: /^[A-Za-z]+Error$/.test(error?.name ?? '') ? error.name : 'Error', code: /^[a-z_]+$/.test(error?.code ?? '') ? error.code : 'acceptance_failed'}));
} finally {
  if (browser) await browser.close();
}
