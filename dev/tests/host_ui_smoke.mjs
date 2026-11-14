#!/usr/bin/env node
// Product UI acceptance: real authentication, bridge and business socket, no injection.
import {readFile} from "node:fs/promises";
import {resolve} from "node:path";
import {pathToFileURL} from "node:url";
import {pollUntil} from "./poll.mjs";
const args=process.argv.slice(2);
let browser,page,stage="arguments";
const checked=[];
const commandErrors=[];
const methods=new Map();
let sentTurnSubmits=0;
let expectedPairingToken;
const pairingObserved=[];
try {
  if(args[0]!=="--ready-file"||!args[1]) throw Error("arguments");
  const metadata=JSON.parse(await readFile(resolve(args[1]),"utf8"));
  const real=args.includes("--real-provider");
  const {chromium}=await import(pathToFileURL(resolve("node_modules/@playwright/test/index.mjs")).href);
  browser=await chromium.launch({headless:true});
  const context=await browser.newContext();
  const origin=new URL(metadata.url);
  if(!["localhost","127.0.0.1","[::1]"].includes(origin.hostname))throw Error("localhost_fixture_required");
  await context.grantPermissions(["clipboard-read","clipboard-write"],{origin:origin.origin});
  page=await context.newPage();
  const pageRequests={compact:0};
  page.on("request",request=>{
    const url=new URL(request.url());
    if(request.method()==="POST"&&url.origin===origin.origin&&/^\/api\/v1\/sessions\/[^/]+\/compact$/.test(url.pathname))pageRequests.compact++;
  });
  page.setDefaultTimeout(25000);
  page.on("websocket",socket=>{
    socket.on("framesent",({payload})=>{try{const value=JSON.parse(String(payload));if(value.kind==="pair")pairingObserved.push({token_matches:value.token===expectedPairingToken,token_present:typeof value.token==="string"&&value.token.length>0,scope_valid:["backend_id","binding_id","project_id"].every(key=>typeof value.scope?.[key]==="string"&&value.scope[key].length>0&&value.scope[key].length<=256)});if(value.type==="command"){methods.set(value.request_id,value.method);if(value.method==="turn.submit")sentTurnSubmits++;}}catch{}});
    socket.on("framereceived",({payload})=>{try{
      const value=JSON.parse(String(payload));
      if(value.type==="result"&&value.ok===false&&/^[a-z_]+$/.test(value.error?.code??""))commandErrors.push({method:methods.get(value.request_id)??"unknown",code:value.error.code});
      if(value.type==="event"&&value.event==="turn"&&value.data?.status==="failed"){
        const code=value.data.result?.error?.code??"operation_failed";
        commandErrors.push({method:value.data.method??"turn.submit",code:/^[a-z_]+$/.test(code)?code:"operation_failed"});
      }
    }catch{}});
  });
  stage="login-entry";
  await page.goto(metadata.login_entry_url ?? metadata.url+"/login",{waitUntil:"domcontentloaded"});
  await page.getByLabel("账户标识（可选）").fill("alice");
  await page.getByRole("button",{name:"登录",exact:true}).click();
  await page.waitForURL(url=>url.origin===new URL(metadata.url).origin&&url.pathname==="/",{timeout:30000});
  const hostButton=page.getByTestId("header-host-connection");
  await hostButton.waitFor();
  checked.push("fixed_login_entry","normal_oauth_redirect","detached_hydration");
  stage="pairing-rejected-explanation";
  await hostButton.click();
  const dialog=page.getByRole("dialog",{name:"连接项目 Host 执行端",exact:true});
  await dialog.getByRole("button",{name:"SSH（暂不支持）",exact:true}).click();
  await dialog.getByText("SSH 连接暂不支持。",{exact:false}).waitFor();
  checked.push("ssh_explicitly_unsupported");
  stage="pairing-select-host-websocket";
  await dialog.getByRole("button",{name:"Host WebSocket",exact:true}).click();
  stage="pairing-read-bridge-ready";
  const ready=JSON.parse(await readFile(metadata.bridge_ready_file,"utf8"));
  expectedPairingToken=ready.pairing_token;
  if(metadata.require_token!==true||!ready.pairing_token)throw Error("token_fixture_required");
  stage="pairing-select-project";
  await dialog.getByLabel("目标项目 (Project)",{exact:true}).selectOption(metadata.project_id);
  stage="pairing-fill-address";
  await dialog.getByLabel("Host WebSocket 地址",{exact:true}).fill(ready.url);
  stage="pairing-fill-invalid-code";
  await dialog.getByLabel("一次性配对码",{exact:false}).fill("invalid-test-code");
  stage="pairing-click-invalid-code";
  await dialog.getByRole("button",{name:"配对并连接 Host",exact:true}).click();
  stage="pairing-wait-rejection-feedback";
  await dialog.getByTestId("host-status").getByText(/配对码无效|配对码已过期/).waitFor();
  checked.push("rejected_pairing_explanation");
  stage="pairing-dismiss-feedback-toast";
  const dismissFeedback=page.getByRole("button",{name:"关闭提示",exact:true});
  if(await dismissFeedback.count())await dismissFeedback.click();
  stage="pair-valid-wait-idle";
  await dialog.getByRole("button",{name:"连接 Host",exact:true}).waitFor();
  stage="pair-valid-address";
  await dialog.getByLabel("Host WebSocket 地址",{exact:true}).fill(ready.url);
  stage="pair-valid-code";
  await dialog.getByLabel("一次性配对码",{exact:false}).fill(ready.pairing_token);
  if(await dialog.getByLabel("一次性配对码",{exact:false}).inputValue()!==ready.pairing_token)throw Error("pairing_input_handoff");
  stage="pair-valid-click";
  await dialog.getByRole("button",{name:"配对并连接 Host",exact:true}).click();
  stage="pair-valid-dialog-close";
  // Successful connection closes the dialog; the header owns persistent status.
  await dialog.waitFor({state:"hidden"});
  await hostButton.filter({hasText:"已就绪"}).waitFor();
  const normalize=value=>value.toLowerCase().replace(/^\\\\\?\\/,"");
  if(normalize(await hostButton.getAttribute("title")??"")!==normalize(metadata.root))throw Error("workspace");
  checked.push("project_pairing","ready_exact_workspace");

  async function currentSessionId() {
    const url=new URL(page.url());
    const match=(url.hash.slice(1)||url.pathname).match(/^\/?session\/([^/]+)$/);
    if(!match)throw Error("existing_session_route_required");
    return decodeURIComponent(match[1]);
  }
  async function canonicalIdle(sessionId,userTexts,assistantTexts) {
    // Read-only native HTTP confirms durable canonical materialization and idle.
    // Polling is bounded by state, never a fixed sleep or an optimistic banner.
    await pollUntil(() => page.evaluate(async ({sessionId,userTexts,assistantTexts})=>{
      const response=await fetch("/api/v1/sessions/"+encodeURIComponent(sessionId),{credentials:"include",signal:AbortSignal.timeout(15000)});
      if(!response.ok)return false;
      const value=await response.json();
      return value.session?.status==="idle"
        &&userTexts.every(text=>value.messages.some(message=>message.role==="user"&&message.text===text))
        &&assistantTexts.every(text=>value.messages.some(message=>message.role==="assistant"&&message.text===text));
    },{sessionId,userTexts,assistantTexts}),{timeoutMs:90000,intervalMs:100,code:"canonical_idle_deadline"});
    const canonicalStage=stage;
    const earlier=page.getByRole("button",{name:/展开更早的.*轮历史对话/});
    if(await earlier.count())await earlier.click();
    for(const [index,text] of userTexts.entries()){stage=canonicalStage+"-user-"+index;await page.locator('.block[data-role="user"]').getByText(text,{exact:true}).first().waitFor();}
    for(const [index,text] of assistantTexts.entries()){stage=canonicalStage+"-assistant-"+index;await page.locator('.block[data-role="assistant"]').getByText(text,{exact:true}).first().waitFor();}
    stage=canonicalStage+"-send";
    await page.getByTitle("发送 (Enter)",{exact:true}).waitFor({state:"visible"});
    stage=canonicalStage;
  }
  async function assertReadTool() {
    const group=page.locator(".tool-accordion-group").filter({hasText:"read"}).first();
    await group.waitFor();
    if(await group.locator(".tool-accordion-items").count()===0)await group.locator(".tool-accordion-group-header").click();
    const read=group.locator(".tool-accordion-row").filter({has:page.locator(".tool-name",{hasText:/^read$/})}).first();
    if(await read.getAttribute("data-status")!=="completed")throw Error("read_status");
    if(await read.getAttribute("data-expanded")!=="true")await read.locator(".tool-accordion-header").click();
    await read.getByTestId("tool-payload-output").locator("pre.payload-body").filter({hasText:"synthetic-seed-content"}).waitFor();
  }
  const inputs=real?["Reply exactly MICROCRAB_PROVIDER_OK_ONE","Reply exactly MICROCRAB_PROVIDER_OK_TWO"]:["lifecycle-one","after-restart"];
  const outputs=real?["MICROCRAB_PROVIDER_OK_ONE","MICROCRAB_PROVIDER_OK_TWO"]:["canonical:lifecycle-one","canonical:after-restart"];
  let sessionId;
  for(let index=0;index<2;index++){
    stage="real-turn-"+(index+1);
    await page.getByLabel("消息",{exact:true}).fill(inputs[index]);
    await page.getByTitle("发送 (Enter)",{exact:true}).click();
    await page.waitForURL(url=>/^\/?session\/[^/]+$/.test(url.hash.slice(1)||url.pathname),{timeout:30000});
    const selected=await currentSessionId();
    if(sessionId&&selected!==sessionId)throw Error("second_turn_changed_session");
    sessionId=selected;
    await canonicalIdle(sessionId,inputs.slice(0,index+1),outputs.slice(0,index+1));
    if(!real&&index===0)await assertReadTool();
    checked.push("turn_"+(index+1)+"_canonical_and_idle");
  }
  checked.push(real?"real_provider_two_responses":"real_wasm_remote_read");

  async function snapshot() {
    return page.evaluate(async id=>{
      const response=await fetch("/api/v1/sessions/"+encodeURIComponent(id),{credentials:"include",signal:AbortSignal.timeout(15000)});
      if(!response.ok)throw Error("snapshot_http");
      return response.json();
    },sessionId);
  }
  const observe=(predicate,code)=>pollUntil(async()=>{
    const value=await snapshot();
    return predicate(value)?value:false;
  },{timeoutMs:90000,intervalMs:100,code});
  const turnCount=()=>sentTurnSubmits;
  const editor=page.getByLabel("消息",{exact:true});

  stage="existing-pinned-agent-mode";
  const agentPill=page.getByTestId("composer-agent-pill");
  const beforeMode=await snapshot();
  if(beforeMode.session.agent!=="coding")throw Error("coding_pin_required");
  for(const mode of ["build","plan"]){
    await agentPill.click();
    const menu=page.getByRole("menu").filter({has:page.getByTestId("composer-mode-item")});
    await menu.waitFor();
    if(await menu.getByTestId("composer-agent-item").count()!==0)throw Error("existing_agent_not_pinned");
    const choices=await menu.getByTestId("composer-mode-item").allTextContents();
    if(choices.length!==2||!choices.every(text=>["plan","build"].includes(text.trim())))throw Error("eligible_modes");
    await menu.getByRole("menuitemradio",{name:mode,exact:true}).click();
    await observe(value=>value.session.agent==="coding"&&value.session.mode===mode,"mode_selection_deadline");
    await pollUntil(async()=>await agentPill.getAttribute("aria-busy")!=="true"&&(await agentPill.textContent()).includes(mode),{timeoutMs:25000,code:"mode_ui_deadline"});
  }
  checked.push("existing_coding_pin","eligible_plan_build_mode_selection");

  async function assertStatistics(pending) {
    const value=await snapshot(),stats=value.statistics;
    if(!stats||stats.activity.scope!=="selected_context_history"||!Number.isFinite(stats.context.bytes)||!stats.context.fingerprint)throw Error("authoritative_statistics_required");
    if(pending!==undefined&&stats.context.pending_epoch!==pending)throw Error("pending_epoch_mismatch");
    const formatted=number=>number==null?"未提供":Math.round(number).toLocaleString("en-US");
    const row=(panel,label)=>panel.locator(".dock-stat-row").filter({has:page.getByText(label,{exact:true})}).locator(".dock-stat-val");
    async function equal(locator,expected){
      await pollUntil(async()=>await locator.textContent()===expected,{timeoutMs:25000,code:"statistics_ui_deadline"});
    }
    await page.getByTestId("statistics-activity").click();
    const activity=page.locator("#statistics-activity-details");
    await equal(row(activity,"当前轮次"),formatted(stats.activity.turns));
    await equal(row(activity,"执行步骤"),formatted(stats.activity.steps));
    await equal(row(activity,"工具调用"),formatted(stats.activity.tool_calls));
    await page.keyboard.press("Escape");
    await activity.waitFor({state:"hidden"});
    await page.getByTestId("statistics-usage").click();
    const usage=page.locator("#statistics-usage-details");
    const prompt=stats.provider?.latest_prompt;
    const window=stats.model?.context_window_tokens;
    const tokenValue=number=>number==null?"未获取":formatted(number)+" tok";
    const tokenScope=prompt?.freshness==="current"&&prompt.input_tokens!=null?"当前输入 token":"上次请求输入 token";
    await equal(row(usage,tokenScope),tokenValue(prompt?.input_tokens)+" / "+tokenValue(window));
    const pill=await page.getByTestId("statistics-usage").textContent();
    if(!pill.includes(tokenValue(prompt?.input_tokens)+" / "+tokenValue(window)))throw Error("context_primary_tokens_required");
    if(prompt?.freshness!=="current"&&pill.includes("%"))throw Error("stale_prompt_cannot_claim_current_occupancy");
    await equal(row(usage,"上下文占用字节"),formatted(stats.context.bytes)+" B / "+formatted(stats.context.limit_bytes)+" B");
    await equal(row(usage,"系统字节"),formatted(stats.context.categories.system)+" B");
    await equal(row(usage,"工具定义字节"),formatted(stats.context.categories.tools)+" B");
    const conversationBytes=["state","user","assistant","tool_result"].reduce((sum,key)=>sum+stats.context.categories[key],0);
    await equal(row(usage,"对话字节（含 State）"),formatted(conversationBytes)+" B");
    if(await usage.textContent().then(text=>/未提供|上下文指纹|Epoch/.test(text)))throw Error("statistics_context_diagnostics_leaked");
    await page.keyboard.press("Escape");
    await usage.waitFor({state:"hidden"});
  }
  stage="authoritative-statistics-dock";
  await assertStatistics();
  checked.push("statistics_dock_matches_server_activity_context_epoch");

  if(!real){
    stage="trajectory-real-tool-shared-dialog";
    await page.getByRole("tab",{name:"轨迹",exact:true}).click();
    await page.locator('[data-testid="trajectory-entry"][data-kind="tool"]').filter({hasText:"read"}).first().click();
    const inspector=page.getByTestId("trajectory-inspector");
    await inspector.getByRole("tab",{name:"概述",exact:true}).click();
    const payload=inspector.getByTestId("tool-payload-output");
    await payload.locator("pre.payload-body").filter({hasText:"synthetic-seed-content"}).waitFor();
    if(await payload.getByTestId("payload-expand").count())throw Error("trajectory_payload_folded");
    await payload.getByTestId("payload-dialog-expand").click();
    const content=page.getByTestId("content-dialog");
    await content.locator("pre.payload-body").filter({hasText:"synthetic-seed-content"}).waitFor();
    if(await content.getByTestId("payload-dialog-expand").count())throw Error("recursive_content_expand");
    await page.keyboard.press("Escape");
    await content.waitFor({state:"hidden"});
    stage="historical-tool-schema";
    await inspector.getByRole("tab",{name:"Schema",exact:true}).click();
    await inspector.getByTestId("historical-tool-schema").locator("pre.payload-body").waitFor();
    if(await inspector.textContent().then(text=>/未找到历史请求证据|此历史请求未记录该工具 Schema/.test(text)))throw Error("historical_schema_missing");
    stage="historical-result-request-delta";
    await inspector.getByRole("tab",{name:"请求增量",exact:true}).click();
    await inspector.getByTestId("historical-request-diff").locator("pre.payload-body").filter({hasText:"synthetic-seed-content"}).waitFor();
    await inspector.getByRole("tab",{name:"请求信息",exact:true}).click();
    await inspector.getByTestId("request-audit-metadata").locator("dl").waitFor();
    if(await inspector.textContent().then(text=>text.includes("未找到历史请求证据")))throw Error("historical_request_metadata_missing");
    await page.getByRole("tab",{name:"对话",exact:true}).click();
    checked.push("trajectory_real_tool_scroll_and_shared_expand_dialog","historical_schema_and_result_request_evidence");
  }

  stage="real-clipboard-png-preview-remove";
  const imageText=real?"Reply exactly MICROCRAB_PROVIDER_OK_IMAGE":"after-restart";
  const imageOutput=real?"MICROCRAB_PROVIDER_OK_IMAGE":"canonical:after-restart";
  async function pastePng(){
    // Canvas creates a browser-decodable PNG; the OS clipboard and native paste
    // reach the product's normal intake without dispatching synthetic events.
    await page.evaluate(async()=>{
      const canvas=document.createElement("canvas");
      canvas.width=2;canvas.height=2;
      const drawing=canvas.getContext("2d");
      if(!drawing)throw Error("canvas_required");
      drawing.fillStyle="#3685ab";drawing.fillRect(0,0,2,2);
      const png=await new Promise(resolve=>canvas.toBlob(resolve,"image/png"));
      if(!png)throw Error("png_required");
      await navigator.clipboard.write([new ClipboardItem({"image/png":png})]);
    });
    await editor.focus();
    await page.keyboard.press("Control+V");
    await page.getByTestId("composer-images").getByTestId("image-preview-trigger").waitFor();
    await pollUntil(async()=>await page.getByTitle("发送 (Enter)",{exact:true}).isEnabled(),{timeoutMs:25000,code:"image_intake_deadline"});
  }
  async function preview(rail){
    await rail.getByTestId("image-preview-trigger").click();
    const preview=page.getByTestId("image-preview-dialog");
    await preview.waitFor();
    await pollUntil(()=>preview.getByTestId("image-preview-original").evaluate(image=>image.complete&&image.naturalWidth===2&&image.naturalHeight===2),{timeoutMs:25000,code:"image_decode_deadline"});
    await preview.getByRole("button").click();
    await preview.waitFor({state:"hidden"});
    await rail.getByTestId("image-preview-trigger").click();
    await preview.waitFor();
    await page.keyboard.press("Escape");
    await preview.waitFor({state:"hidden"});
  }
  await editor.fill(imageText);
  await pastePng();
  await preview(page.getByTestId("composer-images"));
  await page.getByTestId("composer-image-remove").click();
  await page.getByTestId("composer-images").waitFor({state:"hidden"});
  if(await editor.inputValue()!==imageText)throw Error("image_remove_changed_text");
  checked.push("native_clipboard_canvas_png","draft_image_preview_close_escape","remove_image_preserves_text");
  stage="clipboard-second-paste";
  await pastePng();
  stage="image-read-before-submit";
  const beforeImage=await snapshot();
  const previousUsers=new Set(beforeImage.messages.filter(message=>message.role==="user").map(message=>message.id));
  const beforeImageTurns=turnCount();
  stage="image-editor-enter";
  await editor.press("Enter");
  stage="image-wait-structured-source";
  const imageSnapshot=await observe(value=>value.session.status==="idle"&&value.messages.some(message=>message.role==="user"&&!previousUsers.has(message.id)&&message.text===imageText&&message.parts?.some(part=>part.type==="image"&&part.source?.type==="url")),"structured_image_deadline");
  if(turnCount()!==beforeImageTurns+1)throw Error("image_turn_count");
  const imageMessage=imageSnapshot.messages.find(message=>message.role==="user"&&!previousUsers.has(message.id)&&message.text===imageText);
  const imageIndex=imageMessage.parts.findIndex(part=>part.type==="image");
  const source=imageMessage.parts[imageIndex].source;
  if(source.type!=="url"||source.url!==`/api/v1/sessions/${sessionId}/images/${imageMessage.id}/${imageIndex}`||"data" in source)throw Error("image_source_reference");
  stage="image-canonical-ui";
  await canonicalIdle(sessionId,[...inputs,imageText],[...outputs,imageOutput]);
  stage="historical-image-preview";
  await preview(page.getByTestId("conversation-images").last());
  checked.push("structured_image_turn_and_durable_source_reference","historical_image_preview_close_escape");
  inputs.push(imageText);outputs.push(imageOutput);
  stage="refresh-existing-session-reopen";
  const existingUrl=page.url();
  await page.reload({waitUntil:"domcontentloaded"});
  await hostButton.filter({hasText:"已就绪"}).waitFor({timeout:30000});
  if(page.url()!==existingUrl||await currentSessionId()!==sessionId)throw Error("refresh_changed_session");
  await canonicalIdle(sessionId,inputs,outputs);
  if(!real)await assertReadTool();
  await preview(page.getByTestId("conversation-images").last());
  checked.push("same_tab_resume","two_turn_canonical_history_reopen","historical_image_preview_after_reload");

  stage="bare-compact-editor-enter";
  const beforeCompact=await snapshot();
  const existingMarkers=new Set((beforeCompact.compactions??[]).map(marker=>marker.id));
  const submitsBeforeCompact=turnCount();
  const compactRequestsBefore=pageRequests.compact;
  await editor.fill("/compact ");
  await editor.press("Enter");
  await pollUntil(async()=>{
    const value=await snapshot();
    return value.operations?.some(operation=>operation.method==="session.compact"&&operation.status==="completed"&&!beforeCompact.operations.some(prior=>prior.id===operation.id));
  },{timeoutMs:90000,code:"compact_ui_terminal_deadline"});
  const compacted=await observe(value=>value.session.status==="idle"
    &&value.statistics?.context.pending_epoch===true
    &&value.compactions?.some(marker=>!existingMarkers.has(marker.id)&&typeof marker.summary==="string"&&marker.summary.length>0)
    &&value.operations?.some(operation=>operation.method==="session.compact"&&operation.status==="completed"&&!beforeCompact.operations.some(prior=>prior.id===operation.id)),"compact_snapshot_terminal_deadline");
  const marker=compacted.compactions.find(marker=>!existingMarkers.has(marker.id));
  const snapshotReceipt=compacted.operations.find(operation=>operation.method==="session.compact"&&operation.status==="completed"&&!beforeCompact.operations.some(prior=>prior.id===operation.id));
  const receipt=await page.evaluate(async({sessionId,operationId})=>{
    const response=await fetch(`/api/v1/sessions/${encodeURIComponent(sessionId)}/operations/${encodeURIComponent(operationId)}`,{credentials:"include",signal:AbortSignal.timeout(15000)});
    if(!response.ok)throw Error("compact_receipt_http");
    return response.json();
  },{sessionId,operationId:snapshotReceipt.id});
  if(receipt.id!==snapshotReceipt.id||receipt.method!=="session.compact"||receipt.status!=="completed"||receipt.result?.compaction_id!==marker.id)throw Error("compact_receipt_marker");
  await page.locator(".compaction-row").waitFor();
  if(await page.locator(".composer-compact-status").count()||await page.getByTestId("compact-lifecycle").count())throw Error("compact_terminal_banner_retained");
  if(await editor.inputValue()!=="")throw Error("compact_draft_not_cleared");
  await assertStatistics(true);
  // Re-observe after terminal rendering and StatisticsDock inspection: compact
  // must leave the session idle and cannot enqueue an ordinary model turn.
  const afterCompact=await snapshot();
  if(turnCount()!==submitsBeforeCompact||pageRequests.compact!==compactRequestsBefore+1
    ||afterCompact.session.status!=="idle"||afterCompact.statistics?.context.pending_epoch!==true
    ||afterCompact.operations.filter(operation=>operation.method==="turn.submit").length!==beforeCompact.operations.filter(operation=>operation.method==="turn.submit").length)throw Error("compact_auto_turn");
  checked.push("bare_compact_enter_terminal_ui_and_http","compact_receipt_marker_pending_epoch","compact_no_automatic_turn_submit");
  if(args.includes("--verify-deletion")) {
    if(real)throw Error("synthetic_deletion_fixture_required");
    stage="confirmed-project-cascade";
    const projectId=afterCompact.session.project_id;
    const related=await page.evaluate(async projectId=>{
      const created=await fetch("/api/v1/sessions",{method:"POST",credentials:"include",headers:{"content-type":"application/json"},body:JSON.stringify({project_id:projectId,agent:"coding",mode:"plan",operation_id:crypto.randomUUID()}),signal:AbortSignal.timeout(25000)});
      if(!created.ok)throw Error("cascade_fixture_create");
      const second=await created.json();
      const response=await fetch("/api/v1/projects",{credentials:"include"});
      const projects=await response.json();
      return {secondId:second.id,projectName:projects.items.find(project=>project.id===projectId)?.name};
    },projectId);
    if(!related.secondId||!related.projectName)throw Error("cascade_fixture_missing");
    const folder=page.locator(".rail-project-folder").filter({hasText:related.projectName});
    await folder.locator(".rail-project-row").hover();
    await folder.locator(".rail-project-more-btn").click();
    await page.getByRole("button",{name:/删除项目/}).click();
    const confirmation=page.getByRole("dialog",{name:"删除项目",exact:true});
    await confirmation.waitFor();
    if(!(await confirmation.textContent()).includes("所有会话"))throw Error("cascade_warning_missing");
    const response=page.waitForResponse(response=>response.request().method()==="DELETE"&&new URL(response.url()).pathname===`/api/v1/projects/${encodeURIComponent(projectId)}`);
    await confirmation.getByRole("button",{name:"删除",exact:true}).click();
    const deleted=await response;
    if(!deleted.ok())throw Error("cascade_http_failed");
    const receipt=await deleted.json();
    if(![sessionId,related.secondId].every(id=>receipt.archived_session_ids?.includes(id)))throw Error("cascade_receipt_missing_children");
    await confirmation.waitFor({state:"hidden"});
    const remaining=await page.evaluate(async()=>{
      const sessions=await fetch("/api/v1/sessions",{credentials:"include"}).then(response=>response.json());
      const projects=await fetch("/api/v1/projects",{credentials:"include"}).then(response=>response.json());
      return {sessions:sessions.items,projects:projects.items};
    });
    if(remaining.sessions.some(session=>session.project_id===projectId&&!session.archived)||remaining.projects.some(project=>project.id===projectId))throw Error("cascade_left_visible_orphans");
    if(![sessionId,related.secondId].every(id=>remaining.sessions.some(session=>session.id===id&&session.archived)))throw Error("cascade_children_not_archived");
    checked.push("confirmed_project_warning_active_inactive_child_cascade","cascade_confirmation_settled_no_visible_orphans");
  }
  if(commandErrors.length)throw Error("unexpected_command_error");
  console.log(JSON.stringify({status:"passed",storage:metadata.storage,checked}));
} catch(error) {
  process.exitCode=1;
  const hostDiagnostic=page?await page.evaluate(()=>{
    const status=document.querySelector('[data-testid="host-status"]');
    const text=status?.textContent??"";
    const error=document.querySelector('.host-connect-error')?.textContent??"";
    const known=[["配对码无效","pairing_rejected"],["配对码已过期","pairing_expired"],["该目录不在","directory_not_allowed"],["工作目录与项目固定","workspace_mismatch"],["项目已固定","binding_mismatch"],["无法连接桥接","connection_failed"],["恢复凭据已失效","resume_rejected"],["配对响应超时","handshake_timeout"],["旧版本桥接","bridge_upgrade_required"]];
    const reason=known.find(([label])=>error.includes(label))?.[1]??/Host 状态 \[([a-z_]{1,64})\]/.exec(error)?.[1]??null;
    const action=document.querySelector('[role="dialog"] button.primary-btn');
    const label=action?.textContent?.trim();
    const actionLabel=["配对并连接 Host","正在连接…","连接 Host","重新连接 Host"].includes(label)?label:null;
    return {action_label:actionLabel,action_disabled:action?.disabled??null,code_present:!!document.querySelector('#host-code')?.value,address_present:!!document.querySelector('#host-url')?.value,status_present:!!status,reason,error_present:!!error,ready:status?.getAttribute('data-ready')??null,connecting:text.includes('正在连接'),pairing:text.includes('正在配对')};
  }).catch(()=>null):null;
  console.error(JSON.stringify({status:"failed",stage,failure_class:error?.name??"Error",application_code:/^[a-z_]{1,80}$/.test(error?.code??"")?error.code:/^[a-z_]{1,80}$/.test(error?.message??"")?error.message:null,host_diagnostic:hostDiagnostic,pairing_observed:pairingObserved,command_errors:commandErrors,checked}));
} finally{if(browser)await browser.close();}
