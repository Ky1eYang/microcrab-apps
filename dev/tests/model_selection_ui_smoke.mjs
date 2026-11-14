#!/usr/bin/env node
/** Real UI selection -> native HTTP -> persisted selection -> audited sent bytes.
 * Only the fixture's current configured default model is allowed. Never emit bodies,
 * credentials, endpoints, provider names, response text, screenshots or error stacks.
 */
import {readFile} from 'node:fs/promises';
import {resolve,join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {pollUntil} from './poll.mjs';
const checked=[];
let browser,stage='arguments';
const assert=value=>{if(!value)throw Error('acceptance_assertion');};
const equalSelection=(left,right)=>left?.provider_id===right.provider_id&&left?.model===right.model&&(left?.variant??null)===(right.variant??null);
try {
  const args=process.argv.slice(2);
  assert(args.length===2&&args[0]==='--ready-file');
  const metadata=JSON.parse(await readFile(resolve(args[1]),'utf8'));
  assert(metadata.real_provider===true&&metadata.storage==='file'&&typeof metadata.data_dir==='string');
  const {chromium}=await import(pathToFileURL(resolve('node_modules/@playwright/test/index.mjs')).href);
  browser=await chromium.launch({headless:true});
  const context=await browser.newContext(),page=await context.newPage();
  page.setDefaultTimeout(30000);
  const requests=[];
  page.on('request',request=>{
    const url=new URL(request.url());
    if(url.origin!==new URL(metadata.url).origin)return;
    if(request.method()==='POST'&&url.pathname==='/api/v1/sessions')requests.push({kind:'create',body:request.postDataJSON()});
    if(request.method()==='PUT'&&/^\/api\/v1\/sessions\/[^/]+\/model$/.test(url.pathname))requests.push({kind:'select',path:url.pathname,body:request.postDataJSON()});
  });
  async function http(path,method='GET',body) {
    const result=await page.evaluate(async ({path,method,body})=>{
      const response=await fetch(path,{method,credentials:'include',signal:AbortSignal.timeout(15000),...(body===undefined?{}:{headers:{'Content-Type':'application/json'},body:JSON.stringify(body)})});
      return {status:response.status,ok:response.ok,data:await response.json()};
    },{path,method,body});
    return result;
  }
  function selectedId(){const url=new URL(page.url());return decodeURIComponent((url.hash.slice(1)||url.pathname).match(/^\/?session\/([^/]+)$/)?.[1]??'');}
  async function snapshot(id){const value=await http('/api/v1/sessions/'+encodeURIComponent(id));assert(value.ok);return value.data;}
  async function waitState(id,selection,inputs,outputs,named=false){
    await pollUntil(async ()=>{
      const observed=await http('/api/v1/sessions/'+encodeURIComponent(id));if(!observed.ok)return false;
      const value=observed.data,session=value.session,actual=session?.model_selection;
      const nameReady=!named||(session.title&&session.title!=='New session'&&value.operations?.some(operation=>operation.method==='session.auto_rename'&&operation.status==='completed'));
      return session?.status==='idle'&&actual?.provider_id===selection.provider_id&&actual?.model===selection.model&&(actual?.variant??null)===(selection.variant??null)&&nameReady
        &&inputs.every(text=>value.messages.some(message=>message.role==='user'&&message.text===text))
        &&outputs.every(text=>value.messages.some(message=>message.role==='assistant'&&message.text===text));
    },{timeoutMs:120000,intervalMs:150,code:'canonical_selection_deadline'});
    assert(selectedId()===id);
    for(const text of inputs)await page.locator('.block[data-role="user"]').getByText(text,{exact:true}).waitFor();
    for(const text of outputs)await page.locator('.block[data-role="assistant"]').getByText(text,{exact:true}).waitFor();
    await page.getByTitle('发送 (Enter)',{exact:true}).waitFor();
  }
  async function chooseVariant(label){
    await page.locator('.composer-model-pill').click();
    await page.locator('.model-variant-picker-dropdown').getByRole('button',{name:new RegExp('^'+label+'(?:\\s*✓)?$','i')}).click();
  }
  async function send(text){await page.getByLabel('消息',{exact:true}).fill(text);await page.getByTitle('发送 (Enter)',{exact:true}).click();}
  async function auditWire(id,prompt,selection){
    assert(/^[a-f0-9]{32}$/.test(id));
    const auditPath=join(metadata.data_dir,'sessions',id,'audit.jsonl');
    const deadline=performance.now()+20000;
    while(performance.now()<deadline){
      let text;
      try{text=await readFile(auditPath,'utf8');}catch(error){if(error.code!=='ENOENT')throw error;await delay(100);continue;}
      const matching=[];
      for(const line of text.split('\n').filter(Boolean)){
        let row;try{row=JSON.parse(line);}catch{continue;}
        const data=row.data;if(data?.kind!=='published')continue;
        const record=data.record?.body??data.record;
        if(typeof record?.sent_request!=='string')continue;
        const wire=JSON.parse(record.sent_request);
        const users=(wire.messages?.filter(message=>message.role==='user')??[]).map(message=>{
          const value=message.content;
          return typeof value==='string'?value:Array.isArray(value)?value.filter(part=>part.type==='text').map(part=>part.text).join('\n'):null;
        }).filter(value=>{
          if(typeof value!=='string')return false;
          try{const frame=JSON.parse(value);return !(['state_snapshot','state_patch'].includes(frame?.type));}catch{return true;}
        });
        // Runtime synchronizes State frames as user-role JSON after admission.
        // Exclude only typed State frames; naming's last functional prompt differs.
        // Automatic naming reuses the original input with a separate system prompt,
        // but has no user-turn attribution. Only actual turn requests prove routing.
        if(record.turn_id!=null&&users.at(-1)===prompt)matching.push({wire,record});
      }
      if(matching.length){
        assert(matching.every(({wire,record})=>wire.model===selection.model&&wire.reasoning_effort===selection.variant&&record.turn_id!=null&&record.call_id));
        return {matching_requests:matching.length,actual_model_matches:true,actual_reasoning_matches:true,turn_attribution_present:true};
      }
      await delay(100);
    }
    throw Error('sent_request_evidence_missing');
  }
  stage='real-login-pair';
  await page.goto(metadata.login_entry_url,{waitUntil:'domcontentloaded'});
  await page.getByLabel('账户标识（可选）').fill('model-selection-ui-tester');
  await page.getByRole('button',{name:'登录',exact:true}).click();
  await page.waitForURL(url=>url.origin===new URL(metadata.url).origin&&url.pathname==='/');
  const hostButton=page.getByTestId('header-host-connection');await hostButton.click();
  const dialog=page.getByRole('dialog',{name:'连接项目 Host 执行端',exact:true});
  const ready=JSON.parse(await readFile(metadata.bridge_ready_file,'utf8'));
  await dialog.getByLabel('目标项目 (Project)',{exact:true}).selectOption(metadata.project_id);
  await dialog.getByLabel('Host WebSocket 地址',{exact:true}).fill(ready.url);
  await dialog.getByLabel('一次性配对码',{exact:false}).fill(ready.pairing_token);
  await dialog.getByRole('button',{name:'配对并连接 Host',exact:true}).click();
  await dialog.waitFor({state:'hidden'});await hostButton.filter({hasText:'已就绪'}).waitFor();
  const catalog=await http('/api/v1/models');assert(catalog.ok);
  const configured=catalog.data.default_model_selection;
  assert(configured?.provider_id&&configured?.model);
  const choice=catalog.data.items.find(item=>item.provider_id===configured.provider_id&&item.id===configured.model);
  assert(choice?.variants.includes('medium')&&choice.variants.includes('low'));
  checked.push('catalog_default_exact_tuple_supported_medium_low');
  const medium={...configured,variant:'medium'},low={...configured,variant:'low'};
  const nonce=randomUUID().replaceAll('-','');
  const outputs=['MODEL_SELECTION_ONE_'+nonce,'MODEL_SELECTION_TWO_'+nonce];
  const inputs=outputs.map(text=>'Reply exactly '+text+'. Do not call tools.');
  stage='draft-medium-create-turn-one';
  await chooseVariant('Medium');await send(inputs[0]);
  await page.waitForURL(url=>/^\/?session\/[^/]+$/.test(url.hash.slice(1)||url.pathname));
  const id=selectedId();assert(id);
  await waitState(id,medium,inputs.slice(0,1),outputs.slice(0,1),true);
  assert(requests.filter(request=>request.kind==='create').length===1);
  assert(equalSelection(requests.find(request=>request.kind==='create').body.model_selection,medium));
  const first=await snapshot(id);assert(first.session.model===configured.model);
  stage='actual-medium-sent-request-evidence';
  const mediumEvidence=await auditWire(id,inputs[0],medium);
  checked.push('actual_create_model_selection_medium','turn_one_canonical_idle_after_auto_rename_same_id','sent_wire_medium_and_actual_model');
  stage='manual-rename-same-selected-session';
  await page.locator('.header-crumb-current').dblclick();
  const rename=page.getByRole('dialog',{name:'重命名会话',exact:true});
  await rename.getByLabel('会话标题',{exact:true}).fill('Model selection acceptance renamed');
  await rename.getByRole('button',{name:'保存',exact:true}).click();await rename.waitFor({state:'hidden'});
  await pollUntil(async ()=>{const value=await http('/api/v1/sessions/'+encodeURIComponent(id));return value.ok&&value.data.session.title==='Model selection acceptance renamed';},{timeoutMs:30000,code:'canonical_rename_deadline'});
  assert(selectedId()===id);
  const beforeSwitch=await snapshot(id);
  stage='existing-low-native-cas';
  const switchResponse=page.waitForResponse(response=>response.request().method()==='PUT'&&new URL(response.url()).pathname==='/api/v1/sessions/'+id+'/model');
  await chooseVariant('Low');
  const selectedResponse=await switchResponse;assert(selectedResponse.ok());
  const selected=await selectedResponse.json();assert(equalSelection(selected.session.model_selection,low));
  const switchRequest=requests.findLast(request=>request.kind==='select');
  assert(switchRequest.path==='/api/v1/sessions/'+id+'/model'&&switchRequest.body.expected_revision===beforeSwitch.session.revision&&equalSelection(switchRequest.body,low));
  assert(selected.session.revision>beforeSwitch.session.revision);
  // Explicit stale request verifies CAS once; never retry a rejected mutation.
  const stale=await http('/api/v1/sessions/'+id+'/model','PUT',{...medium,expected_revision:beforeSwitch.session.revision});
  assert(!stale.ok);
  assert(equalSelection((await snapshot(id)).session.model_selection,low));
  checked.push('manual_rename_preserves_selected_session','actual_set_model_low_expected_revision','stale_selection_rejected_without_mutation_replay');
  stage='turn-two-low-canonical-idle';
  await send(inputs[1]);await waitState(id,low,inputs,outputs);
  assert((await snapshot(id)).session.title==='Model selection acceptance renamed');
  const lowEvidence=await auditWire(id,inputs[1],low);
  assert(requests.filter(request=>request.kind==='create').length===1);
  checked.push('turn_two_after_rename_same_id_sendable','sent_wire_low_and_actual_model','two_canonical_responses_idle');
  stage='refresh-restore-selection-no-create';
  const existingUrl=page.url();await page.reload({waitUntil:'domcontentloaded'});
  await hostButton.filter({hasText:'已就绪'}).waitFor();
  assert(page.url()===existingUrl);await waitState(id,low,inputs,outputs);
  await page.locator('.composer-model-pill').filter({hasText:/low/i}).waitFor();
  assert(requests.filter(request=>request.kind==='create').length===1);
  checked.push('refresh_restores_low_selection_same_id_no_create');
  stage='durable-close-reopen-selection';
  assert((await http('/api/v1/sessions/'+id+'/runtime','DELETE',{})).ok);
  const reopened=await http('/api/v1/sessions/'+id+'/runtime','PUT',{});
  assert(reopened.ok&&equalSelection(reopened.data.session.model_selection,low));
  await page.reload({waitUntil:'domcontentloaded'});await hostButton.filter({hasText:'已就绪'}).waitFor();
  await waitState(id,low,inputs,outputs);assert(requests.filter(request=>request.kind==='create').length===1);
  checked.push('runtime_close_open_persisted_low_choice_and_history');
  console.log(JSON.stringify({status:'passed',storage:metadata.storage,checked,wire:{medium:mediumEvidence,low:lowEvidence},session_create_requests:1}));
}catch(error){process.exitCode=1;console.error(JSON.stringify({status:'failed',stage,checked,failure_class:/^[A-Za-z]+Error$/.test(error?.name??'')?error.name:'Error'}));}
finally{if(browser)await browser.close();}
