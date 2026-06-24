"""The single-page admin dashboard (served at ``/``).

Self-contained HTML/CSS/JS — no build step, no external deps. Polls
``/api/status`` and ``/api/matches`` and renders the read-only overview.
"""

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>polytape · admin</title>
<style>
  :root{--bg:#0f1115;--surface:#171a21;--surface2:#1f2330;--border:#2a2f3a;
        --text:#e6e8ee;--muted:#9aa3b2;--dim:#6b7280;--green:#3ddc97;--amber:#f5b14c;--red:#f06363;--blue:#5aa6f0;--mono:ui-monospace,SFMono-Regular,Menlo,monospace}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--text);font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:20px}
  .wrap{max-width:1000px;margin:0 auto}
  header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-bottom:18px}
  .title{font-size:18px;font-weight:500;display:flex;align-items:center;gap:10px}
  .sub{font-family:var(--mono);font-size:12px;color:var(--muted)}
  .pill{display:inline-flex;align-items:center;gap:8px;padding:6px 12px;border-radius:8px;font-size:13px;font-weight:500;background:var(--surface2);color:var(--muted)}
  .dot{width:8px;height:8px;border-radius:50%;background:var(--dim)}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:18px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:12px 14px}
  .card .lbl{font-size:12px;color:var(--muted)}
  .card .val{font-size:24px;font-weight:500;margin-top:2px}
  .controls{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:20px}
  button{background:var(--surface);color:var(--muted);border:1px solid var(--border);border-radius:8px;padding:7px 12px;font:inherit;font-size:13px;cursor:not-allowed;opacity:.6}
  .note{margin-left:auto;font-size:12px;color:var(--dim)}
  h2{font-size:14px;font-weight:500;color:var(--muted);margin:0 0 8px}
  table{width:100%;border-collapse:collapse;font-size:13px;background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:hidden}
  th,td{text-align:left;padding:8px 12px;border-top:1px solid var(--border)}
  th{color:var(--muted);font-weight:500;border-top:none;background:var(--surface2)}
  td.num{font-family:var(--mono)}
  .tag{font-size:12px;padding:2px 8px;border-radius:6px}
  .live{background:rgba(61,220,151,.15);color:var(--green)}
  .quiet{background:rgba(245,177,76,.15);color:var(--amber)}
  .pending{background:var(--surface2);color:var(--dim)}
  .finished{background:var(--surface2);color:var(--muted)}
  .dlchk:disabled{opacity:.3;cursor:not-allowed}
  .footer{margin-top:14px;font-size:12px;color:var(--dim);font-family:var(--mono)}
  .ok{color:var(--green)} .warn{color:var(--amber)} .bad{color:var(--red)}
  .dlcol{display:none}
  body.authed .dlcol{display:table-cell}
  .dlchk{cursor:pointer;width:15px;height:15px;accent-color:var(--blue);vertical-align:middle}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <div class="title">polytape · admin <span class="sub" id="run">—</span></div>
    </div>
    <div class="pill" id="recpill"><span class="dot" id="recdot"></span><span id="recstate">connecting…</span></div>
  </header>

  <div class="cards" id="cards"></div>

  <div class="controls">
    <button id="ctl-restart" onclick="askControl('restart')" disabled>restart</button>
    <button id="ctl-refresh" onclick="askControl('refresh')" disabled>refresh match set</button>
    <button id="ctl-arm-heartbeat" onclick="askControl('arm-heartbeat')" disabled>arm heartbeat</button>
    <button id="authbtn" style="display:none;cursor:pointer;opacity:1" onclick="authClick()">log in</button>
    <span class="note" id="ctlnote">checking controls&hellip;</span>
  </div>

  <div class="controls" id="dlbar" style="display:none">
    <button id="dl-run" style="cursor:pointer;opacity:1" onclick="downloadRun()">&#11015; download whole run</button>
    <button id="dl-sel" style="opacity:.6" onclick="downloadSelected()" disabled>&#11015; download selected</button>
    <span class="note" id="dlnote">tick matches to download &middot; the .tar.gz lands on your machine (through the tunnel)</span>
  </div>

  <h2 id="mtitle">matches</h2>
  <table>
    <thead><tr><th class="dlcol" style="width:28px"></th><th style="width:34%">match</th><th>date</th><th>book</th><th>comments</th><th>last seen</th><th>status</th></tr></thead>
    <tbody id="rows"><tr><td colspan="7" style="color:var(--dim)">loading…</td></tr></tbody>
  </table>
  <div class="footer" id="footer"></div>
</div>
<div id="modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:10;align-items:center;justify-content:center"></div>

<script>
const n = x => (x==null?'—':x.toLocaleString());
function age(s){ if(s==null) return '—'; if(s<1) return Math.round(s*1000)+'ms';
  if(s<90) return Math.round(s)+'s'; if(s<5400) return Math.round(s/60)+'m'; return Math.round(s/3600)+'h'; }
function freshClass(s){ if(s==null) return 'bad'; if(s<=60) return 'ok'; if(s<=600) return 'warn'; return 'bad'; }
// The live tail + per-match order-book preview were removed: the dashboard now reads
// counts + freshness straight from meta.json (no log scan), and downloads cover the
// raw data. Status + matches + download below are all metadata-driven.
// --- guarded controls: session state + typed-confirm modal -----------------
var SESSION={controls_enabled:false,authed:false,actions:[]};
async function refreshSession(){
  try{ SESSION=await fetch('/api/session').then(function(r){return r.json();}); }
  catch(e){ SESSION={controls_enabled:false,authed:false,actions:[]}; }
  var on=SESSION.controls_enabled, authed=SESSION.authed;
  ['restart','refresh','arm-heartbeat'].forEach(function(a){
    var b=document.getElementById('ctl-'+a); if(!b) return;
    var en=on&&authed; b.disabled=!en; b.style.cursor=en?'pointer':'not-allowed'; b.style.opacity=en?'1':'.6';
  });
  var ab=document.getElementById('authbtn');
  ab.style.display=on?'inline-block':'none'; ab.textContent=authed?'log out':'log in';
  document.getElementById('ctlnote').textContent =
    !on ? 'controls disabled — no admin secret configured'
    : authed ? 'controls unlocked · every action is audited'
    : 'controls locked — log in to enable';
  // Download is the most sensitive read (raw payloads), so it follows the login
  // session: the tick-boxes + download bar appear only once authed.
  document.body.classList.toggle('authed', !!authed);
  var dlb=document.getElementById('dlbar'); if(dlb) dlb.style.display=authed?'flex':'none';
  if(!authed) dlSelected.clear();
  updateDlBar();
}
function authClick(){
  if(SESSION.authed){ fetch('/api/logout',{method:'POST'}).then(refreshSession); return; }
  var t=prompt('Admin secret:'); if(!t) return;
  fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:t})})
    .then(function(r){ if(!r.ok) alert('login failed'); return refreshSession(); });
}
function closeModal(){ var m=document.getElementById('modal'); m.style.display='none'; m.innerHTML=''; }
var CONFIRM={restart:'polytape',refresh:'refresh','arm-heartbeat':'arm'};
var WARN={
  restart:'Restart drops the live sockets briefly — the order-book gap during reconnect is permanent (book does not backfill).',
  refresh:'Re-discovers the open match set; restarts the recorder only if the set changed.',
  'arm-heartbeat':'Writes POLYTAPE_HEARTBEAT_URL into the recorder env and restarts it.'
};
function askControl(action){
  if(!SESSION.authed){ alert('log in first'); return; }
  var needsUrl=(action==='arm-heartbeat'), phrase=CONFIRM[action];
  var m=document.getElementById('modal');
  m.innerHTML='<div style="background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px;max-width:430px;width:90%">'+
    '<div style="font-weight:600;font-size:16px;margin-bottom:8px">'+action+'</div>'+
    '<div style="color:var(--muted);font-size:13px;margin-bottom:12px">'+WARN[action]+'</div>'+
    (needsUrl?'<input id="m-url" placeholder="https://hc-ping.com/…" style="width:100%;padding:8px;margin-bottom:8px;background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);font:inherit">':'')+
    '<div style="color:var(--muted);font-size:13px;margin-bottom:6px">type <b style="color:var(--text)">'+phrase+'</b> to confirm</div>'+
    '<input id="m-confirm" autocomplete="off" style="width:100%;padding:8px;margin-bottom:12px;background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);font:inherit">'+
    '<div style="display:flex;gap:8px;justify-content:flex-end"><button id="m-cancel" style="cursor:pointer;opacity:1">cancel</button>'+
    '<button id="m-go" style="cursor:pointer;opacity:1;background:var(--blue);color:#0b0d12;border-color:var(--blue);font-weight:600">confirm</button></div>'+
    '<div id="m-err" class="bad" style="font-size:12px;margin-top:8px"></div></div>';
  m.style.display='flex';
  document.getElementById('m-cancel').onclick=closeModal;
  document.getElementById('m-go').onclick=function(){ submitControl(action, needsUrl); };
  setTimeout(function(){ var f=document.getElementById(needsUrl?'m-url':'m-confirm'); if(f) f.focus(); },40);
}
async function submitControl(action,needsUrl){
  var body={confirm:(document.getElementById('m-confirm')||{}).value||''};
  if(needsUrl) body.url=(document.getElementById('m-url')||{}).value||'';
  try{
    var r=await fetch('/api/control/'+action,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    var d=await r.json().catch(function(){return {};});
    if(r.ok){ closeModal(); refreshSession(); }
    else { var e=document.getElementById('m-err'); if(e) e.textContent=d.error||('error '+r.status); }
  }catch(e){ var el=document.getElementById('m-err'); if(el) el.textContent='request failed'; }
}

// --- match-data download (login-gated; the cookie rides the same-origin GET) -
var dlSelected = new Set();
function updateDlBar(){
  var b=document.getElementById('dl-sel'); if(!b) return;
  var k=dlSelected.size; b.disabled=!k;
  b.style.opacity=k?'1':'.6'; b.style.cursor=k?'pointer':'not-allowed';
  b.innerHTML='&#11015; download selected'+(k?' ('+k+')':'');
}
function downloadHref(url){
  var a=document.createElement('a'); a.href=url; a.download='';
  document.body.appendChild(a); a.click(); a.remove();
}
var _DLNOTE_DEFAULT=(document.getElementById('dlnote')||{}).textContent||'';
var _dlNoteTimer=null;
function dlNote(msg, revertMs){
  var note=document.getElementById('dlnote'); if(!note) return;
  if(_dlNoteTimer){ clearTimeout(_dlNoteTimer); _dlNoteTimer=null; }
  note.textContent = (msg===null) ? _DLNOTE_DEFAULT : msg;
  if(msg!==null && revertMs){ _dlNoteTimer=setTimeout(function(){ dlNote(null); }, revertMs); }
}
async function startDownload(qs, label){
  // Sessions are in-memory, so an admin restart makes a plain <a download> silently
  // 403. Re-check auth FIRST and tell the user, instead of nothing happening.
  var s; try{ s=await fetch('/api/session',{cache:'no-store'}).then(function(r){return r.json();}); }
  catch(e){ s={}; }
  if(!s.authed){
    refreshSession();
    alert('Your admin session expired (the dashboard restarted). Click “log in”, then try the download again.');
    return;
  }
  // An <a download> gives no completion callback; show a best-effort "preparing" hint
  // (a per-match archive is built by scanning the whole run, which can take a minute).
  dlNote('preparing '+label+' — the server is scanning the run; your download will start shortly…', 120000);
  downloadHref('/api/download?'+qs);
}
function downloadRun(){ startDownload('all=1', 'the whole run'); }
function downloadSelected(){
  if(!dlSelected.size) return;
  var qs=Array.from(dlSelected).map(function(e){return 'event='+encodeURIComponent(e);}).join('&');
  startDownload(qs, dlSelected.size+' selected match(es)');
}

async function tick(){
  try{
    const [st, ms] = await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/matches').then(r=>r.json())
    ]);
    const rec = st.recorder||{};
    const up = rec.active==='active';
    document.getElementById('recdot').style.background = up?'var(--green)':'var(--red)';
    document.getElementById('recstate').textContent =
      (rec.active||'?') + (rec.restarts!=null?' · '+rec.restarts+' restarts':'');
    document.getElementById('run').textContent = (st.recorder? 'run-wc':'') ;
    const cov = st.coverage||{};
    const cards = [
      ['records', n((st.records?.book||0)+(st.records?.comments||0))],
      ['last record', '<span class="'+freshClass(st.last_record_age_s)+'">'+age(st.last_record_age_s)+'</span>'],
      ['coverage', (cov.seen||0)+'/'+(cov.total||0)],
      ['open matches', n(st.open_matches)],
      ['disk', st.disk_percent==null?'—':st.disk_percent+'%'],
      ['heartbeat', st.heartbeat_armed?'<span class="ok">armed</span>':'<span class="warn">off</span>'],
    ];
    document.getElementById('cards').innerHTML = cards.map(([l,v])=>
      '<div class="card"><div class="lbl">'+l+'</div><div class="val">'+v+'</div></div>').join('');

    var nRec=ms.filter(function(m){return m.status==='live'||m.status==='quiet';}).length;
    var nFin=ms.filter(function(m){return m.status==='finished';}).length;
    document.getElementById('mtitle').textContent =
      'matches · '+ms.length+' ('+nRec+' recording · '+nFin+' finished)';
    document.getElementById('rows').innerHTML = ms.map(function(m){
      var dis = m.downloadable ? '' : ' disabled';
      // finished/pending: a dimmed em-dash for "last seen" (recency is meaningless there)
      var lastSeen = (m.status==='finished'||m.status==='pending')
        ? '<td class="num" style="color:var(--dim)">—</td>'
        : '<td class="num '+freshClass(m.last_seen_age_s)+'">'+age(m.last_seen_age_s)+'</td>';
      return '<tr data-eid="'+m.event_id+'">'+
        '<td class="dlcol"><input type="checkbox" class="dlchk" data-eid="'+m.event_id+'"'+(dlSelected.has(m.event_id)?' checked':'')+dis+' title="'+(m.downloadable?'select to download':'no recorded data')+'"></td>'+
        '<td>'+m.title+'</td><td class="num">'+(m.date||'—')+'</td>'+
        '<td class="num">'+n(m.counts?.book||0)+'</td>'+
        '<td class="num">'+n(m.counts?.comments||0)+'</td>'+
        lastSeen+
        '<td><span class="tag '+m.status+'">'+m.status+'</span></td></tr>';
    }).join('')
      || '<tr><td colspan="7" style="color:var(--dim)">no matches in this run yet</td></tr>';
    // forget ticks for matches that are no longer downloadable, then refresh the bar
    var present=new Set(ms.filter(function(m){return m.downloadable;}).map(function(m){return m.event_id;}));
    Array.from(dlSelected).forEach(function(e){ if(!present.has(e)) dlSelected.delete(e); });
    updateDlBar();
    document.getElementById('footer').textContent = 'updated '+(st.as_of||'')+' · started '+(st.started_at||'?');
  }catch(e){
    document.getElementById('recstate').textContent = 'admin unreachable';
    document.getElementById('recdot').style.background = 'var(--red)';
  }
}
document.getElementById('rows').onclick=function(e){
  // Only the download tick-boxes are interactive now (the per-match preview was removed).
  var chk=e.target.closest('.dlchk');
  if(chk){ if(chk.disabled) return; var id=chk.getAttribute('data-eid'); if(chk.checked) dlSelected.add(id); else dlSelected.delete(id); updateDlBar(); }
};
tick(); setInterval(tick, 3000);
refreshSession(); setInterval(refreshSession, 20000);
</script>
</body>
</html>
"""
