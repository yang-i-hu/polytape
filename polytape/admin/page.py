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
  .footer{margin-top:14px;font-size:12px;color:var(--dim);font-family:var(--mono)}
  .ok{color:var(--green)} .warn{color:var(--amber)} .bad{color:var(--red)}
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
    <button title="Phase 2">restart</button>
    <button title="Phase 2">refresh match set</button>
    <button title="Phase 2">arm heartbeat</button>
    <button title="Phase 2">stop</button>
    <span class="note">read-only overview · controls land in a later phase</span>
  </div>

  <h2 id="mtitle">matches</h2>
  <table>
    <thead><tr><th style="width:34%">match</th><th>date</th><th>book</th><th>comments</th><th>last seen</th><th>status</th></tr></thead>
    <tbody id="rows"><tr><td colspan="6" style="color:var(--dim)">loading…</td></tr></tbody>
  </table>
  <div class="footer" id="footer"></div>
  <div id="preview" style="display:none;margin-top:18px"></div>
</div>

<script>
const n = x => (x==null?'—':x.toLocaleString());
function age(s){ if(s==null) return '—'; if(s<1) return Math.round(s*1000)+'ms';
  if(s<90) return Math.round(s)+'s'; if(s<5400) return Math.round(s/60)+'m'; return Math.round(s/3600)+'h'; }
function freshClass(s){ if(s==null) return 'bad'; if(s<=60) return 'ok'; if(s<=600) return 'warn'; return 'bad'; }
function fmt(x){ return x==null?'—':(+x).toFixed(3); }
function ladder(levels,col){ var ls=(levels&&levels.length)?levels:[]; return ls.map(function(l){
  return '<div style="display:flex;justify-content:space-between;color:'+col+'"><span>'+fmt(l.price)+'</span><span>'+l.size+'</span></div>'; }).join('') || '<div style="color:var(--dim)">—</div>'; }
function spark(h){ if(!h||h.length<2) return '<div style="height:46px"></div>';
  var ps=h.map(function(d){return d.p;}); var mn=Math.min.apply(null,ps),mx=Math.max.apply(null,ps),rg=(mx-mn)||1;
  var pts=h.map(function(d,i){ return (2+i/(h.length-1)*236).toFixed(1)+','+(42-((d.p-mn)/rg)*38).toFixed(1); }).join(' ');
  return '<svg viewBox="0 0 240 46" width="100%" height="46" preserveAspectRatio="none"><polyline fill="none" stroke="var(--green)" stroke-width="1.5" points="'+pts+'"/></svg>'; }
function closePreview(){ document.getElementById('preview').style.display='none'; }
function renderPreview(m){
  var cards=m.markets.map(function(mk){ return '<div style="background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:10px 12px">'+
    '<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:4px"><span style="font-weight:500">'+mk.label+'</span><span style="font-family:var(--mono);font-size:18px">'+fmt(mk.mid)+'</span></div>'+
    spark(mk.price_hist)+
    '<div style="font-family:var(--mono);font-size:12px;margin-top:6px">'+ladder((mk.asks||[]).slice(0,3).reverse(),'var(--red)')+
    '<div style="border-top:1px solid var(--border);border-bottom:1px solid var(--border);color:var(--muted);padding:2px 0;display:flex;justify-content:space-between"><span>mid</span><span>'+fmt(mk.mid)+'</span></div>'+
    ladder((mk.bids||[]).slice(0,3),'var(--green)')+'</div>'+
    (mk.last_trade?'<div style="font-size:11px;color:var(--dim);margin-top:6px">last '+fmt(mk.last_trade.price)+' × '+mk.last_trade.size+'</div>':'')+
    '</div>'; }).join('');
  return '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px"><span style="font-weight:500;font-size:16px">'+m.title+'  <span class="sub">'+(m.date||'')+'</span></span><button style="cursor:pointer;opacity:1" onclick="closePreview()">close</button></div>'+
    '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px">'+cards+'</div>';
}
async function openMatch(id){
  var el=document.getElementById('preview'); el.style.display='block';
  el.innerHTML='<div style="color:var(--muted)">loading preview…</div>';
  try{ var m=await fetch('/api/matches/'+id).then(function(r){return r.json();}); el.innerHTML=renderPreview(m); }
  catch(e){ el.innerHTML='<div class="bad">failed to load preview</div>'; }
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

    document.getElementById('mtitle').textContent = 'matches · '+ms.length+' open';
    document.getElementById('rows').innerHTML = ms.map(m=>
      '<tr data-eid="'+m.event_id+'" style="cursor:pointer"><td>'+m.title+'</td><td class="num">'+(m.date||'—')+'</td>'+
      '<td class="num">'+n(m.counts?.book||0)+'</td>'+
      '<td class="num">'+n(m.counts?.comments||0)+'</td>'+
      '<td class="num '+freshClass(m.last_seen_age_s)+'">'+age(m.last_seen_age_s)+'</td>'+
      '<td><span class="tag '+m.status+'">'+m.status+'</span></td></tr>').join('')
      || '<tr><td colspan="6" style="color:var(--dim)">no matches in this run yet</td></tr>';
    document.getElementById('footer').textContent = 'updated '+(st.as_of||'')+' · started '+(st.started_at||'?');
  }catch(e){
    document.getElementById('recstate').textContent = 'admin unreachable';
    document.getElementById('recdot').style.background = 'var(--red)';
  }
}
document.getElementById('rows').onclick=function(e){ var tr=e.target.closest('tr'); if(tr&&tr.dataset.eid) openMatch(tr.dataset.eid); };
tick(); setInterval(tick, 3000);
</script>
</body>
</html>
"""
