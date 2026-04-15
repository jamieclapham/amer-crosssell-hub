// Health Check tab — Salesforce hygiene scoring, deal flags
import { BENCHMARKS, REP_ROSTER, QUARTER } from '../config.js';
import { formatUSD, formatDate, renderRiskBadge, renderScoreGauge, computeHygieneFlags, computeHygieneScore, toDateStr, sfLink } from '../ui.js';

// State for deal age drill-down
let _ageDrillDeals = [];
let _ageDrillIsTeam = false;
let _ageDrillActive = null;

window.showHealthAgeBucket = function(bucket) {
  const drillEl = document.getElementById('health-age-drill');
  if (!drillEl) return;
  if (_ageDrillActive === bucket) {
    drillEl.style.display = 'none';
    _ageDrillActive = null;
    document.querySelectorAll('.age-bucket-card').forEach(c => c.classList.remove('active'));
    return;
  }
  _ageDrillActive = bucket;
  document.querySelectorAll('.age-bucket-card').forEach(c => c.classList.remove('active'));
  const activeCard = document.querySelector(`.age-bucket-card[data-bucket="${bucket}"]`);
  if (activeCard) activeCard.classList.add('active');
  const filtered = _ageDrillDeals.filter(d => d._bucket === bucket).sort((a, b) => (Number(b.pbr) || 0) - (Number(a.pbr) || 0));
  const bucketLabel = { under30: '< 30 days', d30_60: '30–60 days', d60_90: '60–90 days', over90: '90+ days' }[bucket];
  const repCol = _ageDrillIsTeam ? '<th>Rep</th>' : '';
  const colCount = _ageDrillIsTeam ? 6 : 5;
  const repColW = _ageDrillIsTeam ? '<col style="width:18%">' : '';
  const dealColW = _ageDrillIsTeam ? '22%' : '30%';
  const rows = filtered.map(d => {
    const repCell = _ageDrillIsTeam ? `<td>${d.salesforce_owner_name || '—'}</td>` : '';
    return `<tr>${repCell}<td>${sfLink(d.opp_name, d.opportunity_id)}</td><td>${d.current_stage_name || '—'}</td><td class="text-right text-mono">${formatUSD(Number(d.pbr) || 0)}</td><td class="text-right">${d.age}d</td><td>${formatDate(d.close_date)}</td></tr>`;
  }).join('');
  drillEl.innerHTML = `
    <div style="padding:14px 0 6px;font-weight:600;font-size:13px;color:var(--text-muted)">${bucketLabel} — ${filtered.length} deal${filtered.length !== 1 ? 's' : ''}</div>
    <table class="data-table" style="table-layout:fixed">
      <colgroup>${repColW}<col style="width:${dealColW}"><col style="width:16%"><col style="width:14%"><col style="width:8%"><col style="width:12%"></colgroup>
      <thead><tr>${repCol}<th>Deal</th><th>Stage</th><th class="text-right">PBR</th><th class="text-right">Age</th><th>Close</th></tr></thead>
      <tbody>${rows || `<tr><td colspan="${colCount}" style="text-align:center;color:var(--text-muted);padding:12px">No deals in this bucket</td></tr>`}</tbody>
    </table>`;
  drillEl.style.display = 'block';
};

export function renderHealth(data, user, targetEl = null) {
  const el = targetEl || document.getElementById('tab-health');
  if (!el) return;

  if (data.viewMode === 'team') {
    renderTeamHealth(el, data);
  } else {
    renderRepHealth(el, data);
  }
}

// ─── Rep Health View ───

function renderRepHealth(el, data) {
  const deals = (data.hygieneDeals || data.pipeline || []).filter(d => !d.is_closed);
  const hygieneScore = computeHygieneScore(deals); // null if no deals
  // Compute flags for each deal
  const flaggedDeals = deals.map(d => ({
    ...d,
    flags: computeHygieneFlags(d),
  })).filter(d => d.flags.length > 0)
    .sort((a, b) => {
      const aHigh = a.flags.some(f => f.level === 'high') ? 0 : 1;
      const bHigh = b.flags.some(f => f.level === 'high') ? 0 : 1;
      return aHigh - bHigh || (Number(b.pbr) || 0) - (Number(a.pbr) || 0);
    });

  // Pipeline stats
  const withNextStep = deals.filter(d => d.next_step && d.next_step.trim() !== '').length;
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const withValidClose = deals.filter(d => {
    const cs = toDateStr(d.close_date);
    if (!cs) return false;
    const close = new Date(cs + 'T00:00:00');
    return !isNaN(close.getTime()) && close >= today;
  }).length;
  const totalDeals = deals.length;

  const flaggedRows = flaggedDeals.map(d => `<tr>
    <td>${sfLink(d.opp_name, d.opportunity_id)}</td>
    <td>${d.current_stage_name || '—'}</td>
    <td class="text-right text-mono">${formatUSD(Number(d.pbr) || 0)}</td>
    <td>${formatDate(d.close_date)}</td>
    <td class="text-truncate">${d.next_step || '<span class="text-muted">—</span>'}</td>
    <td>${renderRiskBadge(d.flags)}</td>
  </tr>`).join('');

  // Deal age distribution
  const dealsWithAge = deals.map(d => {
    const cs = toDateStr(d.created_date);
    const age = cs ? Math.max(0, Math.floor((today - new Date(cs + 'T00:00:00')) / 86400000)) : 0;
    const _bucket = age < 30 ? 'under30' : age < 60 ? 'd30_60' : age < 90 ? 'd60_90' : 'over90';
    return { ...d, age, _bucket };
  });
  _ageDrillDeals = dealsWithAge;
  _ageDrillIsTeam = false;
  _ageDrillActive = null;
  const under30 = dealsWithAge.filter(d => d._bucket === 'under30').length;
  const d30_60 = dealsWithAge.filter(d => d._bucket === 'd30_60').length;
  const d60_90 = dealsWithAge.filter(d => d._bucket === 'd60_90').length;
  const over90 = dealsWithAge.filter(d => d._bucket === 'over90').length;

  const statusText = hygieneScore == null ? 'No open deals' : hygieneScore >= 80 ? 'Great shape' : hygieneScore >= 60 ? 'Needs attention' : 'Action required';
  const scoreColor = hygieneScore == null ? '' : hygieneScore >= 80 ? 'color:var(--success)' : hygieneScore >= 60 ? 'color:var(--warning)' : 'color:var(--danger)';

  el.innerHTML = `
    <div class="hero-banner">
      <div class="hero-title">${data.repName || 'Your'} Pipeline Health · ${QUARTER.label}</div>
      <div class="hero-name">${statusText}</div>
      <div class="metric-grid" style="grid-template-columns:repeat(5,1fr)">
        <div class="metric-card">
          <div class="metric-label">Hygiene Score</div>
          <div class="metric-value" style="${scoreColor}">${hygieneScore == null ? '—' : hygieneScore}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Open Deals</div>
          <div class="metric-value">${totalDeals}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">With Next Step</div>
          <div class="metric-value">${withNextStep}/${totalDeals}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Valid Close Date</div>
          <div class="metric-value">${withValidClose}/${totalDeals}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Flagged Deals</div>
          <div class="metric-value">${flaggedDeals.length}</div>
        </div>
      </div>
    </div>

    <!-- Deal Age Distribution -->
    <div class="card">
      <div class="card-title">Deal Age Distribution</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px">
        <div class="metric-card age-bucket-card" data-bucket="under30" onclick="window.showHealthAgeBucket('under30')" style="background:var(--accent-light)">
          <div class="metric-label" style="color:var(--accent)">< 30 days</div>
          <div class="metric-value" style="color:var(--accent)">${under30}</div>
        </div>
        <div class="metric-card age-bucket-card" data-bucket="d30_60" onclick="window.showHealthAgeBucket('d30_60')" style="background:var(--warning-light)">
          <div class="metric-label" style="color:var(--warning)">30–60 days</div>
          <div class="metric-value" style="color:var(--warning)">${d30_60}</div>
        </div>
        <div class="metric-card age-bucket-card" data-bucket="d60_90" onclick="window.showHealthAgeBucket('d60_90')" style="background:var(--warning-light)">
          <div class="metric-label" style="color:var(--warning)">60–90 days</div>
          <div class="metric-value" style="color:var(--warning)">${d60_90}</div>
        </div>
        <div class="metric-card age-bucket-card" data-bucket="over90" onclick="window.showHealthAgeBucket('over90')" style="background:${over90 > 0 ? 'var(--danger-light)' : 'var(--accent-light)'}">
          <div class="metric-label" style="color:${over90 > 0 ? 'var(--danger)' : 'var(--accent)'}">90+ days</div>
          <div class="metric-value" style="color:${over90 > 0 ? 'var(--danger)' : 'var(--accent)'}">${over90}</div>
        </div>
      </div>
      <div id="health-age-drill" style="display:none"></div>
    </div>

    <!-- Flagged Deals -->
    <div class="card">
      <div class="card-title">Flagged Deals (${flaggedDeals.length})</div>
      ${flaggedDeals.length === 0 ? '<div class="empty-state" style="padding:20px">All deals are clean. Nice work!</div>' : `
        <table class="data-table" style="table-layout:fixed">
          <colgroup><col style="width:26%"><col style="width:16%"><col style="width:14%"><col style="width:12%"><col style="width:16%"><col style="width:16%"></colgroup>
          <thead><tr><th>Deal</th><th>Stage</th><th class="text-right">PBR</th><th>Close</th><th>Next Step</th><th>Issues</th></tr></thead>
          <tbody>${flaggedRows}</tbody>
        </table>
      `}
    </div>
  `;

}

// ─── Manager Team Health View ───

function renderTeamHealth(el, data) {
  const allDeals = (data.hygieneDeals || data.pipeline || []).filter(d => !d.is_closed);

  // Per-rep hygiene
  const repHealth = Object.entries(REP_ROSTER).map(([email, rep]) => {
    const repDeals = allDeals.filter(d => d.salesforce_owner_name === rep.sfName);
    const score = computeHygieneScore(repDeals); // null if no deals
    const flagged = repDeals.filter(d => computeHygieneFlags(d).length > 0).length;
    const overdue = repDeals.filter(d => computeHygieneFlags(d).some(f => f.type === 'overdue')).length;
    const staleOrMissing = repDeals.filter(d => computeHygieneFlags(d).some(f => f.type === 'no_next_step' || f.type === 'stale_next_step')).length;
    return { email, name: rep.name, score, total: repDeals.length, flagged, overdue, staleOrMissing };
  }).sort((a, b) => (a.score ?? 101) - (b.score ?? 101)); // null scores sort last

  const repRows = repHealth.map(r => {
    const scoreColor = r.score == null ? 'var(--text-muted)' : r.score >= 80 ? 'var(--accent)' : r.score >= 60 ? 'var(--warning)' : 'var(--danger)';
    const scoreDisplay = r.score == null ? '—' : r.score;
    return `<tr class="rep-row" onclick="document.getElementById('repSelector').value='${r.email}'; window.onRepChange('${r.email}')">
      <td><strong>${r.name}</strong></td>
      <td class="text-right" style="font-weight:700;color:${scoreColor}">${scoreDisplay}</td>
      <td class="text-right">${r.total}</td>
      <td class="text-right" style="color:${r.flagged > 0 ? 'var(--danger)' : 'var(--accent)'}"><strong>${r.flagged}</strong></td>
      <td class="text-right" style="color:${r.overdue > 0 ? 'var(--danger)' : 'var(--text-muted)'}">${r.overdue}</td>
      <td class="text-right" style="color:${r.staleOrMissing > 0 ? 'var(--warning)' : 'var(--text-muted)'}">${r.staleOrMissing}</td>
    </tr>`;
  }).join('');

  // Top 10 worst deals across team
  const worstDeals = allDeals
    .map(d => ({ ...d, flags: computeHygieneFlags(d) }))
    .filter(d => d.flags.length > 0)
    .sort((a, b) => {
      const aScore = a.flags.reduce((s, f) => s + (f.level === 'high' ? 2 : 1), 0);
      const bScore = b.flags.reduce((s, f) => s + (f.level === 'high' ? 2 : 1), 0);
      return bScore - aScore || (Number(b.pbr) || 0) - (Number(a.pbr) || 0);
    })
    .slice(0, 10);

  const worstRows = worstDeals.map(d => `<tr>
    <td>${d.salesforce_owner_name || '—'}</td>
    <td>${sfLink(d.opp_name, d.opportunity_id)}</td>
    <td class="text-right text-mono">${formatUSD(Number(d.pbr) || 0)}</td>
    <td>${formatDate(d.close_date)}</td>
    <td>${renderRiskBadge(d.flags)}</td>
  </tr>`).join('');

  const totalFlagged = repHealth.reduce((s, r) => s + r.flagged, 0);
  const totalOverdue = repHealth.reduce((s, r) => s + r.overdue, 0);
  const scoredReps = repHealth.filter(r => r.score != null);
  const avgScore = scoredReps.length > 0 ? Math.round(scoredReps.reduce((s, r) => s + r.score, 0) / scoredReps.length) : null;
  const avgScoreColor = avgScore == null ? '' : avgScore >= 80 ? 'color:var(--success)' : avgScore >= 60 ? 'color:var(--warning)' : 'color:var(--danger)';

  // Deal age distribution (pre-compute for drill-down)
  const ageToday = new Date(); ageToday.setHours(0, 0, 0, 0);
  const teamDealsWithAge = allDeals.map(d => {
    const cs = toDateStr(d.created_date);
    const age = cs ? Math.max(0, Math.floor((ageToday - new Date(cs + 'T00:00:00')) / 86400000)) : 0;
    const _bucket = age < 30 ? 'under30' : age < 60 ? 'd30_60' : age < 90 ? 'd60_90' : 'over90';
    return { ...d, age, _bucket };
  });
  _ageDrillDeals = teamDealsWithAge;
  _ageDrillIsTeam = true;
  _ageDrillActive = null;
  const tUnder30 = teamDealsWithAge.filter(d => d._bucket === 'under30').length;
  const tD30_60 = teamDealsWithAge.filter(d => d._bucket === 'd30_60').length;
  const tD60_90 = teamDealsWithAge.filter(d => d._bucket === 'd60_90').length;
  const tOver90 = teamDealsWithAge.filter(d => d._bucket === 'over90').length;

  el.innerHTML = `
    <div class="hero-banner">
      <div class="hero-title">Team Pipeline Health · ${QUARTER.label}</div>
      <div class="hero-name">${totalFlagged} flagged across ${allDeals.length} open deals</div>
      <div class="metric-grid" style="grid-template-columns:repeat(4,1fr)">
        <div class="metric-card">
          <div class="metric-label">Open Deals</div>
          <div class="metric-value">${allDeals.length}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Avg Score</div>
          <div class="metric-value" style="${avgScoreColor}">${avgScore == null ? '—' : avgScore}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Flagged</div>
          <div class="metric-value" style="color:${totalFlagged > 0 ? 'var(--danger)' : 'var(--success)'}">${totalFlagged}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Overdue</div>
          <div class="metric-value" style="color:${totalOverdue > 0 ? 'var(--danger)' : 'var(--success)'}">${totalOverdue}</div>
        </div>
      </div>
    </div>

    <!-- Team Hygiene Leaderboard -->
    <div class="card">
      <div class="card-title">Team Hygiene Scores</div>
      <table class="data-table team-table" style="table-layout:fixed">
        <colgroup><col style="width:24%"><col style="width:14%"><col style="width:14%"><col style="width:16%"><col style="width:16%"><col style="width:16%"></colgroup>
        <thead><tr>
          <th>Rep</th>
          <th class="text-right">Score</th>
          <th class="text-right">Deals</th>
          <th class="text-right">Flagged</th>
          <th class="text-right">Overdue</th>
          <th class="text-right">Stale/Missing</th>
        </tr></thead>
        <tbody>${repRows}</tbody>
      </table>
      <div class="last-updated" style="margin-top:8px">Sorted by score (worst first). Click a rep to drill in.</div>
    </div>

    <!-- Worst Deals Across Team -->
    <div class="card">
      <div class="card-title">Top Flagged Deals</div>
      ${worstDeals.length === 0 ? '<div class="empty-state" style="padding:20px">No flagged deals across the team.</div>' : `
        <table class="data-table" style="table-layout:fixed">
          <colgroup><col style="width:18%"><col style="width:26%"><col style="width:16%"><col style="width:14%"><col style="width:26%"></colgroup>
          <thead><tr><th>Rep</th><th>Deal</th><th class="text-right">PBR</th><th>Close</th><th>Issues</th></tr></thead>
          <tbody>${worstRows}</tbody>
        </table>
      `}
    </div>

    <!-- Deal Age Distribution -->
    <div class="card">
      <div class="card-title">Deal Age Distribution</div>
      <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px">
        <div class="metric-card age-bucket-card" data-bucket="under30" onclick="window.showHealthAgeBucket('under30')" style="background:var(--accent-light)">
          <div class="metric-label" style="color:var(--accent)">< 30 days</div>
          <div class="metric-value" style="color:var(--accent)">${tUnder30}</div>
        </div>
        <div class="metric-card age-bucket-card" data-bucket="d30_60" onclick="window.showHealthAgeBucket('d30_60')" style="background:var(--warning-light)">
          <div class="metric-label" style="color:var(--warning)">30–60 days</div>
          <div class="metric-value" style="color:var(--warning)">${tD30_60}</div>
        </div>
        <div class="metric-card age-bucket-card" data-bucket="d60_90" onclick="window.showHealthAgeBucket('d60_90')" style="background:var(--warning-light)">
          <div class="metric-label" style="color:var(--warning)">60–90 days</div>
          <div class="metric-value" style="color:var(--warning)">${tD60_90}</div>
        </div>
        <div class="metric-card age-bucket-card" data-bucket="over90" onclick="window.showHealthAgeBucket('over90')" style="background:${tOver90 > 0 ? 'var(--danger-light)' : 'var(--accent-light)'}">
          <div class="metric-label" style="color:${tOver90 > 0 ? 'var(--danger)' : 'var(--accent)'}">90+ days</div>
          <div class="metric-value" style="color:${tOver90 > 0 ? 'var(--danger)' : 'var(--accent)'}">${tOver90}</div>
        </div>
      </div>
      <div id="health-age-drill" style="display:none"></div>
    </div>
  `;
}
