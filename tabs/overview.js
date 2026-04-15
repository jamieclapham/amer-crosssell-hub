// Sales Dive tab — Performance / Top Deals / Coaching / Outlook / AI Priorities sub-tabs
// Sub-tabs above hero; hero KPIs with team avg comparison; forecast bar always visible.
import { QUOTA_USD, BENCHMARKS, REP_ROSTER, QUARTER, NEXT_QUARTER, COACH_QUOTAS, COACHES, STAGE_ORDER, saveBenchmarkOverride, getQuotaFallback, saveQuotaFallback } from '../config.js';
import { formatUSD, formatUSDFull, formatPct, daysLeft, renderMetricCard, computeDealRisks, renderRiskBadge, formatDate, toDateStr, renderSparkline, renderDelta, computeHygieneFlags, getISOWeek, sfLink } from '../ui.js';
import { generatePriorities } from '../ai-priorities.js';
import { fetchWeeklyWonPBR, fetchAllRepsWeeklyWonPBR, fetchNextQuarterPipeline, fetchRepActivityComparison, fetchTranscriptDetails, fetchCallWithTranscript, fetchNextQuarterQuotas, fetchNextQuarterWonPBR, fetchAllRepsNextQuarterWonPBR } from '../queries.js';
import { createLineChart, createBarChart, repLineDataset, teamAvgLineDataset, repBarDataset, teamAvgBarDataset, COLORS, formatWeekLabel, currencyTick, computeWeeklyTeamAvg, destroyChart, dimLineDataset, MEDDIC_DIM_COLORS } from '../chart-utils.js';
import { streamAI } from '../ai-stream.js';
import { renderForecast } from './forecast.js';
import { renderHealth } from './health.js';

let currentSubtab = 'performance';
const dealCoachingCache = quick.db.collection('hub_deal_coaching');
const COACHING_TTL_MS = 24 * 60 * 60 * 1000; // 24h
const outlookMeddicCache = quick.db.collection('hub_outlook_meddic');
const outlookActionsCache = quick.db.collection('hub_outlook_actions');
let outlookDrillRep = null; // null = team view, sfName = drilled rep

// MEDDIC scoring caches
const MEDDIC_CACHE = quick.db.collection('hub_meddic');
const MEDDIC_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days (weekly snapshots)
const MEDDIC_CALL_CACHE = quick.db.collection('hub_meddic_call');
const MEDDIC_CALL_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30 days

// ─── Entry point ───

export async function renderOverview(data, user) {
  const el = document.getElementById('tab-overview');
  if (!el) return;
  currentSubtab = 'performance';
  if (data.viewMode === 'team') {
    await renderTeamOverview(el, data, user);
  } else {
    renderRepOverview(el, data, user);
  }
}

// ─── Sub-tab nav HTML ───

function subtabNav(isManager = false) {
  const tabs = [
    { id: 'performance', label: 'Performance' },
    { id: 'priorities', label: 'AI Priorities' },
    { id: 'pipeline', label: 'Pipeline' },
    { id: 'activity', label: 'Activity' },
    { id: 'deals', label: 'Top Deals' },
    { id: 'coaching', label: 'Coaching' },
    { id: 'forecast', label: 'Forecast' },
    { id: 'health', label: 'Health' },
    { id: 'outlook', label: `${NEXT_QUARTER.label} Outlook` },
  ];
  if (isManager) {
    tabs.push({ id: 'analytics', label: 'Analytics' });
    tabs.push({ id: 'settings', label: 'Settings' });
  }
  return `<div class="overview-subtabs">${tabs.map(t =>
    `<button class="subtab-btn${t.id === currentSubtab ? ' active' : ''}" data-subtab="${t.id}" onclick="window.switchOverviewSubtab('${t.id}')">${t.label}</button>`
  ).join('')}</div>`;
}

window.switchOverviewSubtab = function(tab) {
  currentSubtab = tab;
  if (tab !== 'outlook') outlookDrillRep = null;
  try { quick.db.collection('hub_tab_views').create({ email: window.__currentUser?.email || '', tab: 'overview', subtab: tab, created_at: new Date().toISOString() }); } catch (_) {}
  document.querySelectorAll('.subtab-btn').forEach(b => b.classList.toggle('active', b.dataset.subtab === tab));
  const data = window.__appData || {};
  if (data.viewMode === 'team') {
    renderTeamSubtabContent(data);
  } else {
    renderRepSubtabContent(data);
  }
};

// ════════════════════════════════════════════════════════════════════
//  REP VIEW
// ════════════════════════════════════════════════════════════════════

function renderRepOverview(el, data) {
  el.innerHTML = `
    ${subtabNav()}
    <div id="overview-subtab-content"></div>
  `;

  renderRepSubtabContent(data);
}

// ─── Team averages for KPI comparison ───

function computeTeamAvgs(data) {
  const teamPipeline = data.teamPipeline || [];
  const teamWonPBR = data.teamWonPBR || [];
  const repCount = Object.keys(REP_ROSTER).length || 1;

  // Won PBR avg
  const totalWon = teamWonPBR.reduce((s, r) => s + (Number(r.won_pbr) || 0), 0);
  const avgWonPBR = totalWon / repCount;

  // Pipeline avg (open deals only)
  const openTeam = teamPipeline.filter(d => !d.is_closed);
  const totalPipeline = openTeam.reduce((s, d) => s + (Number(d.pbr) || 0), 0);
  const avgPipeline = totalPipeline / repCount;

  // Commit avg
  const today = new Date();
  const in30 = new Date(); in30.setDate(today.getDate() + 30);
  let teamForecastPBR = 0;
  for (const d of openTeam) {
    if (d.forecast_category === 'Forecast') teamForecastPBR += Number(d.pbr) || 0;
  }
  const avgCommit = (totalWon + teamForecastPBR) / repCount;

  // Win rate avg
  const teamWonDeals = teamPipeline.filter(d => d.is_won);
  const teamClosedDeals = teamPipeline.filter(d => d.is_closed);
  const avgWinRate = teamClosedDeals.length > 0 ? (teamWonDeals.length / teamClosedDeals.length) * 100 : null;

  // Avg deal size
  const avgDealSize = teamWonDeals.length > 0 ? totalWon / teamWonDeals.length : null;

  return { wonPBR: avgWonPBR, openPipeline: avgPipeline, commit: avgCommit, winRate: avgWinRate, avgDealSize };
}

// ─── KPI card with team avg comparison ───

function kpiCard(label, value, subtext, repVal, teamVal, formatter) {
  let teamText = '';
  if (repVal != null && teamVal != null && formatter) {
    const isAbove = repVal >= teamVal;
    const color = isAbove ? 'var(--success)' : 'var(--danger)';
    teamText = ` · <span style="color:${color};font-weight:500">Team avg ${formatter(teamVal)}</span>`;
  }
  return `<div class="metric-card">
    <div class="metric-label">${label}</div>
    <div class="metric-value">${value}</div>
    <div class="metric-sub">${subtext}${teamText}</div>
  </div>`;
}

function repMetrics(data) {
  const wonPBR = Number(data.wonPBR) || 0;
  // Use BQ quota if available, fallback to config
  const quotaRows = data.quotas || [];
  const bqQuota = quotaRows.find(q => q.name === data.sfName && q.role_type === 'rep');
  const quota = bqQuota ? Number(bqQuota.quarterly_quota) : getQuotaFallback();
  const attainment = quota > 0 ? (wonPBR / quota) * 100 : 0;
  const gap = Math.max(0, quota - wonPBR);
  const allDeals = data.pipeline || [];
  const openDeals = allDeals.filter(d => !d.is_closed);
  const wonDeals = allDeals.filter(d => d.is_won);
  const closedDeals = allDeals.filter(d => d.is_closed);
  const openPipeline = openDeals.reduce((sum, d) => sum + (Number(d.pbr) || 0), 0);
  const pipelineExclPreQual = openDeals.filter(d => d.current_stage_name !== 'Pre-Qualified').reduce((sum, d) => sum + (Number(d.pbr) || 0), 0);
  const days = daysLeft();
  const coverage = gap > 0 ? (pipelineExclPreQual / gap) : 999;
  const calls = data.callActivity || {};

  // Win rate + avg deal size
  const wonCount = wonDeals.length;
  const closedCount = closedDeals.length;
  const winRate = closedCount > 0 ? (wonCount / closedCount) * 100 : null;
  const avgDealSize = wonCount > 0 ? wonPBR / wonCount : null;

  // Forecast categories
  const today = new Date();
  const in30 = new Date(); in30.setDate(today.getDate() + 30);
  let forecastPBR = 0, bestCasePBR = 0, upsidePBR = 0;
  for (const d of openDeals) {
    const pbr = Number(d.pbr) || 0;
    const cs = toDateStr(d.close_date);
    const close = cs ? new Date(cs + 'T00:00:00') : null;
    if (d.forecast_category === 'Forecast') forecastPBR += pbr;
    else if (d.forecast_category === 'Pipeline') {
      if (close && close <= in30) bestCasePBR += pbr;
      else upsidePBR += pbr;
    }
  }
  const commit = wonPBR + forecastPBR;
  const bestCase = commit + bestCasePBR;
  const upside = bestCase + upsidePBR;
  const maxFc = Math.max(upside, quota);

  return { quota, wonPBR, attainment, gap, openDeals, wonDeals, openPipeline, pipelineExclPreQual, days, coverage,
           calls, winRate, avgDealSize, wonCount, closedCount,
           forecastPBR, bestCasePBR, upsidePBR, commit, bestCase, upside, maxFc };
}

// ─── Rep sub-tab content switcher ───

function renderRepSubtabContent(data) {
  const el = document.getElementById('overview-subtab-content');
  if (!el) return;
  const m = repMetrics(data);

  if (currentSubtab === 'performance') {
    el.innerHTML = renderRepPerformance(data, m);
    loadRevenueTrend(data);
    loadRepMEDDICForCompare(data);
  } else if (currentSubtab === 'deals') {
    el.innerHTML = renderRepDeals(m);
  } else if (currentSubtab === 'coaching') {
    el.innerHTML = renderRepCoaching(data);
    const repKey = data.repEmail || data.sfName;
    loadCachedMEDDIC(repKey);
    loadMEDDICTrend(repKey);
    wireCoachingButtons(data);
  } else if (currentSubtab === 'outlook') {
    el.innerHTML = '<div class="ai-loading">Loading next-quarter pipeline...</div>';
    loadRepOutlook(data);
  } else if (currentSubtab === 'priorities') {
    el.innerHTML = renderRepPriorities();
    loadPriorities(data);
  } else if (currentSubtab === 'pipeline') {
    el.innerHTML = renderRepPipeline(data, m);
    initPipelineChart(data);
  } else if (currentSubtab === 'activity') {
    el.innerHTML = renderRepActivity(data, m);
    initActivityCharts(data);
  } else if (currentSubtab === 'forecast') {
    renderForecast(data, window.__currentUser, el);
  } else if (currentSubtab === 'health') {
    renderHealth(data, window.__currentUser, el);
  }
}

// ─── Rep Performance sub-tab ───

function renderRepPerformance(data, m) {
  const ta = computeTeamAvgs(data);
  const sfCommit = Number(data.forecastCommit) || 0;
  const repCount = Object.keys(REP_ROSTER).length || 1;
  const teamWonDeals = (data.teamPipeline || []).filter(d => d.is_won).length;
  const teamAvgDeals = Math.round(teamWonDeals / repCount);

  // You vs Team Average comparison rows (MEDDIC filled async)
  const compareMetrics = [
    ['Revenue', m.wonPBR, ta.wonPBR, formatUSD],
    ['Deals Won', m.wonCount, teamAvgDeals, v => String(Math.round(v))],
    ['Avg Deal Size', m.avgDealSize || 0, ta.avgDealSize || 0, formatUSD],
    ['Win Rate', m.winRate || 0, ta.winRate || 0, v => formatPct(v)],
  ];

  const compareRows = compareMetrics.map(([label, repVal, teamVal, fmt]) => {
    const isAbove = repVal >= teamVal;
    const color = isAbove ? 'var(--success)' : 'var(--danger)';
    const pctFill = teamVal > 0 ? Math.min((repVal / teamVal) * 100, 150) : 0;
    return `<div class="compare-row">
      <div class="compare-label">${label}</div>
      <div class="compare-you" style="color:${color}">${fmt(repVal)}</div>
      <div class="compare-team">${fmt(teamVal)}</div>
      <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:${pctFill}%;background:${color}"></div></div>
    </div>`;
  }).join('');

  // Team Quota Attainment table
  const wonMap = {};
  for (const row of (data.teamWonPBR || [])) wonMap[row.salesforce_owner_name] = Number(row.won_pbr) || 0;
  const teamClosedMap = {};
  const teamWonCountMap = {};
  for (const d of (data.teamPipeline || [])) {
    if (d.is_closed) {
      teamClosedMap[d.salesforce_owner_name] = (teamClosedMap[d.salesforce_owner_name] || 0) + 1;
      if (d.is_won) teamWonCountMap[d.salesforce_owner_name] = (teamWonCountMap[d.salesforce_owner_name] || 0) + 1;
    }
  }

  // Per-rep quota from BQ for leaderboard
  const lbQuotaRows = data.quotas || [];
  const lbQuotaMap = {};
  for (const q of lbQuotaRows) {
    if (q.role_type === 'rep') lbQuotaMap[q.name] = Number(q.quarterly_quota) || 0;
  }

  const repRows = Object.entries(REP_ROSTER)
    .map(([email, rep]) => {
      const won = wonMap[rep.sfName] || 0;
      const repQuota = lbQuotaMap[rep.sfName] || getQuotaFallback();
      const att = repQuota > 0 ? (won / repQuota) * 100 : 0;
      const closed = teamClosedMap[rep.sfName] || 0;
      const wonCt = teamWonCountMap[rep.sfName] || 0;
      const wr = closed > 0 ? Math.round((wonCt / closed) * 100) : 0;
      const isMe = email === data.repEmail;
      return { name: rep.name, email, won, attainment: att, winRate: wr, isMe };
    })
    .sort((a, b) => b.attainment - a.attainment);

  const attainmentRows = repRows.map((r, i) => {
    const rank = i + 1;
    const attColor = r.attainment >= 80 ? 'var(--success)' : r.attainment >= 50 ? 'var(--warning)' : 'var(--danger)';
    const attBg = r.attainment >= 80 ? 'var(--success-light)' : r.attainment >= 50 ? 'var(--warning-light)' : 'var(--danger-light)';
    const highlight = r.isMe ? ' style="background:var(--accent-light)"' : '';
    return `<tr${highlight}>
      <td style="text-align:center"><span style="display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;border-radius:50%;font-size:11px;font-weight:700;background:${rank <= 3 ? 'var(--accent-light)' : 'var(--bg)'};color:${rank <= 3 ? 'var(--accent)' : 'var(--text-muted)'}">${rank}</span></td>
      <td><strong>${r.name}</strong>${r.isMe ? ' <span style="font-size:10px;color:var(--text-muted)">(you)</span>' : ''}</td>
      <td class="text-right text-mono">${formatUSD(r.won)}</td>
      <td class="text-right"><span style="display:inline-flex;padding:2px 8px;border-radius:20px;font-size:11px;font-weight:600;background:${attBg};color:${attColor}">${formatPct(r.attainment)}</span></td>
      <td class="text-right">${r.winRate}%</td>
      <td class="text-right" id="attainment-meddic-${r.email}"><span class="text-muted">—</span></td>
    </tr>`;
  }).join('');

  return `
    <!-- Hero KPIs -->
    <div class="hero-banner">
      <div class="hero-title">${data.repName || 'Your'} Performance · ${QUARTER.label}</div>
      <div class="hero-name">${formatPct(m.attainment)} to target${m.days <= 7 ? ' · Final push!' : ''}</div>
      <div class="metric-grid metric-grid-compact">
        ${kpiCard('Won PBR', formatUSD(m.wonPBR), `of ${formatUSDFull(m.quota)}`, m.wonPBR, ta.wonPBR, formatUSD)}
        ${kpiCard('Gap to Target', formatUSD(m.gap), `${m.days} days left`, null, null)}
        ${kpiCard('Open Pipeline', formatUSD(m.openPipeline), `${m.openDeals.length} deals · ${m.coverage.toFixed(1)}x coverage`, m.openPipeline, ta.openPipeline, formatUSD)}
        ${kpiCard('Pipe Coverage', m.coverage < 999 ? `${m.coverage.toFixed(1)}x` : '—', 'excl. Pre-Qualified', null, null)}
        ${kpiCard('Forecast', sfCommit > 0 ? formatUSD(sfCommit) : '—', sfCommit > 0 ? `${formatPct((sfCommit / m.quota) * 100)} of quota` : 'No forecast submitted', null, null)}
        ${kpiCard('Win Rate', m.winRate != null ? formatPct(m.winRate) : '—', m.closedCount > 0 ? `${m.wonCount}W / ${m.closedCount}C` : 'No closed deals', m.winRate, ta.winRate, v => formatPct(v))}
        ${kpiCard('Avg Won Deal', m.avgDealSize != null ? formatUSD(m.avgDealSize) : '—', m.wonCount > 0 ? `${m.wonCount} won deals` : 'No won deals', m.avgDealSize, ta.avgDealSize, formatUSD)}
      </div>
    </div>

    <!-- Forecast Bar -->
    <div class="card">
      <div class="card-title">Forecast</div>
      <div class="forecast-bar-container">
        <div class="forecast-bar">
          ${m.wonPBR > 0 ? `<div class="forecast-segment forecast-won" style="width:${(m.wonPBR / m.maxFc) * 100}%">${formatUSD(m.wonPBR)}</div>` : ''}
          ${m.forecastPBR > 0 ? `<div class="forecast-segment forecast-commit" style="width:${(m.forecastPBR / m.maxFc) * 100}%">${formatUSD(m.forecastPBR)}</div>` : ''}
          ${m.bestCasePBR > 0 ? `<div class="forecast-segment forecast-bestcase" style="width:${(m.bestCasePBR / m.maxFc) * 100}%">${formatUSD(m.bestCasePBR)}</div>` : ''}
          ${m.upsidePBR > 0 ? `<div class="forecast-segment forecast-upside" style="width:${(m.upsidePBR / m.maxFc) * 100}%">${formatUSD(m.upsidePBR)}</div>` : ''}
        </div>
        <div class="forecast-legend">
          <span class="legend-won">Won: ${formatUSD(m.wonPBR)}</span>
          <span class="legend-commit">Commit: ${formatUSD(m.commit)}</span>
          <span class="legend-bestcase">Best Case: ${formatUSD(m.bestCase)}</span>
          <span class="legend-upside">Upside: ${formatUSD(m.upside)}</span>
        </div>
      </div>
    </div>

    <!-- Two columns: Revenue Trend + You vs Team -->
    <div class="overview-two-col">
      <div class="card">
        <div class="card-title">Revenue Trend</div>
        <div id="overview-revenue-trend" class="chart-wrap-lg"><canvas id="chartRevenueTrend"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">You vs Team Average</div>
        <div class="compare-header">
          <div class="compare-header-spacer"></div>
          <div class="compare-header-label">You</div>
          <div class="compare-header-label">Team Avg</div>
          <div class="compare-header-bar"></div>
        </div>
        ${compareRows}
        <div class="compare-row" id="compare-meddic-row">
          <div class="compare-label">MEDDIC</div>
          <div class="compare-you" style="color:var(--text-muted)">—</div>
          <div class="compare-team">—</div>
          <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:0%"></div></div>
        </div>
      </div>
    </div>

    <!-- Team Quota Attainment -->
    <div class="card">
      <div class="card-title">Team Quota Attainment — ${QUARTER.label}</div>
      <table class="data-table" style="table-layout:fixed">
        <colgroup><col style="width:6%"><col style="width:24%"><col style="width:20%"><col style="width:18%"><col style="width:16%"><col style="width:16%"></colgroup>
        <thead><tr>
          <th>#</th><th>Rep</th><th class="text-right">Revenue</th><th class="text-right">Attainment</th><th class="text-right">Win Rate</th><th class="text-right">MEDDIC</th>
        </tr></thead>
        <tbody>${attainmentRows}</tbody>
      </table>
    </div>
  `;
}

// ─── AI Priorities sub-tab ───

function renderRepPriorities() {
  return `
    <div class="card priorities-section">
      <div class="card-title">
        AI Weekly Priorities
        <button class="refresh-btn" onclick="window.refreshPriorities()">Refresh</button>
      </div>
      <div id="priorities-container">
        <div class="ai-loading">Analyzing your pipeline and activity...</div>
      </div>
    </div>
  `;
}

// ─── Async: fill MEDDIC compare row ───

async function loadRepMEDDICForCompare(data) {
  const scoreColor = s => s >= 7 ? 'var(--success)' : s >= 5 ? 'var(--warning)' : 'var(--danger)';
  try {
    const repKey = data.repEmail || data.sfName;
    const items = await MEDDIC_CACHE.where({ key: repKey }).orderBy('created_at', 'desc').limit(1).find();
    const repScore = items.length ? JSON.parse(items[0].data).overall || 0 : null;

    // Fetch all reps' MEDDIC + fill attainment table cells
    let teamTotal = 0, teamCount = 0;
    for (const [email] of Object.entries(REP_ROSTER)) {
      try {
        const repItems = await MEDDIC_CACHE.where({ key: email }).orderBy('created_at', 'desc').limit(1).find();
        if (repItems.length) {
          const sc = JSON.parse(repItems[0].data);
          if (sc.overall) {
            teamTotal += sc.overall; teamCount++;
            // Fill attainment table MEDDIC cell
            const cell = document.getElementById(`attainment-meddic-${email}`);
            if (cell) cell.innerHTML = `<span style="color:${scoreColor(sc.overall)};font-weight:700">${sc.overall.toFixed(1)}</span>`;
          }
        }
      } catch (_) {}
    }
    const teamAvg = teamCount > 0 ? teamTotal / teamCount : null;

    // Fill compare row
    const row = document.getElementById('compare-meddic-row');
    if (row && repScore != null && teamAvg != null) {
      const isAbove = repScore >= teamAvg;
      const color = isAbove ? 'var(--success)' : 'var(--danger)';
      const pctFill = teamAvg > 0 ? Math.min((repScore / teamAvg) * 100, 150) : 0;
      row.innerHTML = `
        <div class="compare-label">MEDDIC</div>
        <div class="compare-you" style="color:${color}">${repScore.toFixed(1)}</div>
        <div class="compare-team">${teamAvg.toFixed(1)}</div>
        <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:${pctFill}%;background:${color}"></div></div>`;
    } else if (row && repScore != null) {
      row.innerHTML = `
        <div class="compare-label">MEDDIC</div>
        <div class="compare-you">${repScore.toFixed(1)}</div>
        <div class="compare-team">—</div>
        <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:0%"></div></div>`;
    }
  } catch (_) {}
}

// ─── Top Deals sub-tab ───

function renderRepDeals(m) {
  const topDeals = m.openDeals.slice(0, 10);
  const totalPBR = topDeals.reduce((s, d) => s + (Number(d.pbr) || 0), 0);

  return `
    <div class="hero-banner">
      <div class="hero-title">Top Deals · ${QUARTER.label}</div>
      <div class="metric-grid" style="grid-template-columns:repeat(3,1fr)">
        ${renderMetricCard('Open Deals', String(m.openDeals.length), '')}
        ${renderMetricCard('Top 10 PBR', formatUSD(totalPBR), '')}
        ${renderMetricCard('Avg Deal', m.openDeals.length > 0 ? formatUSD(m.openPipeline / m.openDeals.length) : '—', '')}
      </div>
    </div>

    <div class="card">
      <div class="card-title">Top Deals by PBR (${topDeals.length})</div>
      ${topDeals.length === 0 ? '<div class="empty-state">No open deals this quarter.</div>' : `
        <table class="data-table" style="table-layout:fixed">
          <colgroup><col style="width:30%"><col style="width:20%"><col style="width:18%"><col style="width:15%"><col style="width:17%"></colgroup>
          <thead><tr><th>Deal</th><th>Stage</th><th class="text-right">PBR</th><th>Close</th><th>Flags</th></tr></thead>
          <tbody>${topDeals.map(d => {
            const risks = computeDealRisks(d);
            return `<tr>
              <td>${sfLink(d.opp_name, d.opportunity_id)}</td>
              <td>${d.current_stage_name || '—'}</td>
              <td class="text-right text-mono">${formatUSD(Number(d.pbr) || 0)}</td>
              <td>${formatDate(d.close_date)}</td>
              <td>${renderRiskBadge(risks)}</td>
            </tr>`;
          }).join('')}</tbody>
        </table>
      `}
    </div>
  `;
}

// ─── Pipeline sub-tab ───

function renderRepPipeline(data, m) {
  const ta = computeTeamAvgs(data);
  const repCount = Object.keys(REP_ROSTER).length || 1;
  const avgDealPipeline = m.openDeals.length > 0 ? m.openPipeline / m.openDeals.length : 0;
  const teamOpenDeals = (data.teamPipeline || []).filter(d => !d.is_closed);
  const teamAvgDealPipeline = teamOpenDeals.length > 0
    ? teamOpenDeals.reduce((s, d) => s + (Number(d.pbr) || 0), 0) / teamOpenDeals.length
    : 0;

  // Stage breakdown
  const stageMap = {};
  for (const d of m.openDeals) {
    const stage = d.current_stage_name || 'Unknown';
    if (!stageMap[stage]) stageMap[stage] = { count: 0, pbr: 0 };
    stageMap[stage].count++;
    stageMap[stage].pbr += Number(d.pbr) || 0;
  }
  const teamStageMap = {};
  for (const d of teamOpenDeals) {
    const stage = d.current_stage_name || 'Unknown';
    if (!teamStageMap[stage]) teamStageMap[stage] = { count: 0, pbr: 0 };
    teamStageMap[stage].count++;
    teamStageMap[stage].pbr += Number(d.pbr) || 0;
  }
  const allStages = [...new Set([...STAGE_ORDER, ...Object.keys(stageMap), ...Object.keys(teamStageMap)])].filter(s => stageMap[s] || teamStageMap[s]);
  const stageRows = allStages
    .map(stage => {
      const rep = stageMap[stage] || { count: 0, pbr: 0 };
      const team = teamStageMap[stage] || { count: 0, pbr: 0 };
      const teamAvgPBR = Math.round(team.pbr / repCount);
      const isAbove = rep.pbr >= teamAvgPBR;
      const color = isAbove ? 'var(--success)' : 'var(--danger)';
      const pct = teamAvgPBR > 0 ? Math.min((rep.pbr / teamAvgPBR) * 100, 150) : 0;
      return `<div class="compare-row">
        <div class="compare-label">${stage}</div>
        <div class="compare-you" style="color:${color}">${formatUSD(rep.pbr)}</div>
        <div class="compare-team">${formatUSD(teamAvgPBR)}</div>
        <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:${pct}%;background:${color}"></div></div>
      </div>`;
    }).join('');

  return `
    <div class="hero-banner">
      <div class="hero-title">Pipeline · ${QUARTER.label}</div>
      <div class="metric-grid" style="grid-template-columns:repeat(3,1fr)">
        ${kpiCard('Total Pipeline', formatUSD(m.openPipeline), `${m.openDeals.length} deals`, m.openPipeline, ta.openPipeline, formatUSD)}
        ${kpiCard('Avg Pipeline Deal', formatUSD(avgDealPipeline), '', avgDealPipeline, teamAvgDealPipeline, formatUSD)}
        ${kpiCard('Coverage', m.coverage.toFixed(1) + 'x', 'vs gap to target', null, null)}
      </div>
    </div>

    <div class="overview-two-col">
      <div class="card">
        <div class="card-title">Pipeline by Stage</div>
        <div class="chart-wrap-lg"><canvas id="chartPipelineStageSub"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">Stage Breakdown — You vs Team Avg</div>
        <div class="compare-header">
          <div class="compare-header-spacer"></div>
          <div class="compare-header-label">You</div>
          <div class="compare-header-label">Team Avg</div>
          <div class="compare-header-bar"></div>
        </div>
        ${stageRows}
      </div>
    </div>
  `;
}

function initPipelineChart(data) {
  const openDeals = (data.pipeline || []).filter(d => !d.is_closed);
  if (openDeals.length === 0) return;
  const stageMap = {};
  for (const d of openDeals) {
    const stage = d.current_stage_name || 'Unknown';
    stageMap[stage] = (stageMap[stage] || 0) + (Number(d.pbr) || 0);
  }
  const repCount = Object.keys(REP_ROSTER).length || 1;
  const teamOpenDeals = (data.teamPipeline || []).filter(d => !d.is_closed);
  const teamStageMap = {};
  for (const d of teamOpenDeals) {
    const stage = d.current_stage_name || 'Unknown';
    teamStageMap[stage] = (teamStageMap[stage] || 0) + (Number(d.pbr) || 0);
  }
  const stageNames = [...new Set([...STAGE_ORDER, ...Object.keys(stageMap)])].filter(s => stageMap[s] || teamStageMap[s]);

  createBarChart('chartPipelineStageSub', {
    labels: stageNames,
    datasets: [
      repBarDataset('You', stageNames.map(s => stageMap[s] || 0), COLORS.primaryBar),
      teamAvgBarDataset(stageNames.map(s => Math.round((teamStageMap[s] || 0) / repCount))),
    ],
    yTickCallback: currencyTick,
  });
}

// ─── Activity sub-tab ───

function renderStandardsTrackerCard(data, isTeamView, weeksElapsed, dialerCallsPerDay, meetings, oppsCreatedThisWeek, cwThisWeek, avgPBRStd, winRateStd, timeToWin, outboundMix, stdBar) {
  const repCount  = isTeamView
    ? Object.values(REP_ROSTER).filter(r => !data.selectedTeam || r.team === data.selectedTeam).length
    : 1;
  const tgtCW     = BENCHMARKS.closedWonPerWeek * repCount;
  const tgtMtg    = BENCHMARKS.meetingsPerWeek * repCount;
  const tgtDialer = BENCHMARKS.dialerCallsPerDay * repCount;
  const tgtOpps   = BENCHMARKS.oppsCreatedPerWeek * repCount;
  const tgtEmails = BENCHMARKS.emailsPerWeek * repCount;
  const title     = isTeamView ? `${QUARTER.label} Standards Tracker · Team total` : `${QUARTER.label} Standards Tracker`;

  // Email metrics from appData
  const rawEmailActivity = data.emailActivity;
  const emailTotal = Array.isArray(rawEmailActivity)
    ? rawEmailActivity.reduce((s, r) => s + (Number(r.outbound_emails) || 0), 0)
    : (Number((rawEmailActivity || {}).outbound_emails) || 0);

  const activityRows = [
    stdBar('Dialer Calls / day',   dialerCallsPerDay,   tgtDialer, dialerCallsPerDay.toFixed(1),      String(tgtDialer)),
    stdBar('Meetings / week',      meetings,            tgtMtg,    String(meetings),                  String(tgtMtg)),
    stdBar('Emails / week',        emailTotal,          tgtEmails, String(emailTotal),                 String(tgtEmails)),
    stdBar('Opps Created / week',  oppsCreatedThisWeek, tgtOpps,   String(oppsCreatedThisWeek),        String(tgtOpps)),
  ].join('');

  const conversionRows = [
    stdBar('Closed Won (this week)', cwThisWeek,   tgtCW,                       String(cwThisWeek),                                      String(tgtCW)),
    stdBar('Avg PBR / deal',         avgPBRStd,    BENCHMARKS.avgPBRPerDeal,    avgPBRStd   != null ? formatUSD(avgPBRStd)             : '—', formatUSD(BENCHMARKS.avgPBRPerDeal)),
    stdBar('Win Rate',               winRateStd,   BENCHMARKS.winRateTarget,    winRateStd  != null ? Math.round(winRateStd * 100) + '%' : '—', Math.round(BENCHMARKS.winRateTarget * 100) + '%'),
    stdBar('Time to Win',            timeToWin,    BENCHMARKS.timeToWinDays,    timeToWin   != null ? timeToWin + 'd'                   : '—', BENCHMARKS.timeToWinDays + 'd', true),
    stdBar('Outbound Mix',           outboundMix,  BENCHMARKS.outboundMixTarget, outboundMix != null ? Math.round(outboundMix * 100) + '%' : '—', Math.round(BENCHMARKS.outboundMixTarget * 100) + '%'),
  ].join('');

  return `<div class="card">
    <div class="card-title" style="display:flex;justify-content:space-between;align-items:center">
      <span>${title}</span>
      <span style="font-size:12px;font-weight:400;color:var(--text-muted)">Wk ${weeksElapsed} of 13 \xb7 ${QUARTER.label}</span>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:0 28px">
      <div>
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted);margin-bottom:8px">Activity</div>
        ${activityRows}
      </div>
      <div>
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted);margin-bottom:8px">Conversion \xb7 Q2 to date</div>
        ${conversionRows}
      </div>
    </div>
  </div>`;
}

function renderRepActivity(data, m) {
  const isTeamView = data.viewMode === 'team';
  // In team view, data.callActivity is an array (one row per rep) — sum it into a single object
  const rawCallActivity = data.callActivity;
  const calls = Array.isArray(rawCallActivity)
    ? rawCallActivity.reduce((acc, r) => {
        acc.total_interactions += Number(r.total_interactions) || 0;
        acc.dialer_calls       += Number(r.dialer_calls)       || 0;
        acc.meetings           += Number(r.meetings)           || 0;
        acc.connected_calls    += Number(r.connected_calls)    || 0;
        acc.transcribed        += Number(r.transcribed)        || 0;
        return acc;
      }, { total_interactions: 0, dialer_calls: 0, meetings: 0, connected_calls: 0, transcribed: 0, avg_duration_min: 0 })
    : (m.calls || {});
  const meetings = Number(calls.meetings) || 0;
  const connected = Number(calls.connected_calls) || 0;
  const total = Number(calls.total_interactions) || 0;
  const transcribed = Number(calls.transcribed) || 0;
  const avgDuration = Number(calls.avg_duration_min) || 0;

  // Activities per deal
  const wonCount = m.wonDeals ? m.wonDeals.length : 0;
  const activitiesPerDeal = wonCount > 0 ? (total / wonCount).toFixed(1) : '—';

  // Team activities per deal
  const teamPipeline = data.teamPipeline || [];
  const teamWonCount = teamPipeline.filter(d => d.is_won).length;
  const repEmails = new Set(Object.keys(REP_ROSTER));
  const repCount = repEmails.size || 1;
  const teamCallData = data.teamActivityTrend || [];

  // Sum totals across all reps
  const lastWeekTeam = {};
  for (const row of teamCallData) {
    if (!repEmails.has(row.rep_email)) continue;
    for (const metric of ['total', 'meetings', 'dialer', 'connected', 'transcribed']) {
      lastWeekTeam[metric] = (lastWeekTeam[metric] || 0) + (Number(row[metric]) || 0);
    }
  }
  const uniqueWeeks = new Set(teamCallData.filter(r => repEmails.has(r.rep_email)).map(r => r.week)).size || 1;
  const teamWeeklyAvg = {
    total: Math.round((lastWeekTeam.total || 0) / repCount / uniqueWeeks),
    meetings: Math.round((lastWeekTeam.meetings || 0) / repCount / uniqueWeeks),
    connected: Math.round((lastWeekTeam.connected || 0) / repCount / uniqueWeeks),
    dialer: Math.round((lastWeekTeam.dialer || 0) / repCount / uniqueWeeks),
    transcribed: Math.round((lastWeekTeam.transcribed || 0) / repCount / uniqueWeeks),
  };
  const teamAvgPerDeal = teamWonCount > 0
    ? ((lastWeekTeam.total || 0) / uniqueWeeks / teamWonCount).toFixed(1)
    : null;
  const activitiesPerDealNum = wonCount > 0 ? total / wonCount : null;
  const teamAvgPerDealNum = teamAvgPerDeal ? Number(teamAvgPerDeal) : null;

  // Team view: compute per-coach averages from activityTrend (most recent ISO week)
  const coachAvgs = {}; // keyed by team ID, e.g. 'D2CRETAIL1'
  if (isTeamView) {
    const allRepsTrend = data.activityTrend || [];
    const recentByRep = {};
    for (const row of allRepsTrend) {
      if (!repEmails.has(row.rep_email)) continue;
      if (!recentByRep[row.rep_email] || row.week > recentByRep[row.rep_email].week) {
        recentByRep[row.rep_email] = row;
      }
    }
    const tAvg = (emails, metric) => {
      const vals = emails.map(e => Number(recentByRep[e]?.[metric]) || 0);
      return Math.round(vals.reduce((a, b) => a + b, 0) / (emails.length || 1));
    };
    for (const [teamId, coach] of Object.entries(COACHES)) {
      const teamEmails = Object.entries(REP_ROSTER).filter(([, r]) => r.team === teamId).map(([e]) => e);
      coachAvgs[teamId] = { total: tAvg(teamEmails, 'total'), meetings: tAvg(teamEmails, 'meetings'), connected: tAvg(teamEmails, 'connected'), dialer: tAvg(teamEmails, 'dialer'), transcribed: tAvg(teamEmails, 'transcribed') };
    }

  }

  // --- Email metrics ---
  const rawEmailActivity = data.emailActivity;
  const emails = Array.isArray(rawEmailActivity)
    ? rawEmailActivity.reduce((acc, r) => {
        acc.total_emails    += Number(r.total_emails)    || 0;
        acc.outbound_emails += Number(r.outbound_emails) || 0;
        acc.inbound_replies += Number(r.inbound_replies) || 0;
        acc.opened          += Number(r.opened)          || 0;
        acc.clicked         += Number(r.clicked)         || 0;
        return acc;
      }, { total_emails: 0, outbound_emails: 0, inbound_replies: 0, opened: 0, clicked: 0 })
    : (rawEmailActivity || {});
  const outboundEmails = Number(emails.outbound_emails) || 0;
  const inboundReplies = Number(emails.inbound_replies) || 0;
  const emailOpenRate = outboundEmails > 0 ? ((Number(emails.opened) || 0) / outboundEmails * 100).toFixed(0) : '—';
  const emailReplyRate = outboundEmails > 0 ? (inboundReplies / outboundEmails * 100).toFixed(0) : '—';

  const activityMetrics = isTeamView ? [] : [
    ['Interactions', total, teamWeeklyAvg.total, v => String(v)],
    ['Meetings', meetings, teamWeeklyAvg.meetings, v => String(v)],
    ['Connected', connected, teamWeeklyAvg.connected, v => String(v)],
    ['Dialer Calls', Number(calls.dialer_calls) || 0, teamWeeklyAvg.dialer, v => String(v)],
    ['Emails Sent', outboundEmails, 0, v => String(v)],
    ['Replies In', inboundReplies, 0, v => String(v)],
  ];

  const compareRows = activityMetrics.map(([label, repVal, teamVal, fmt]) => {
    const isAbove = repVal >= teamVal;
    const color = isAbove ? 'var(--success)' : 'var(--danger)';
    const pctFill = teamVal > 0 ? Math.min((repVal / teamVal) * 100, 150) : 0;
    return `<div class="compare-row">
      <div class="compare-label">${label}</div>
      <div class="compare-you" style="color:${color}">${fmt(repVal)}</div>
      <div class="compare-team">${fmt(teamVal)}</div>
      <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:${pctFill}%;background:${color}"></div></div>
    </div>`;
  }).join('');

  // Q2 Standards Tracker: computed values
  // Activity + conversion metrics are derived from already-loaded data (m.* and data.pipeline).
  // Only avg_time_to_win and outbound_mix require the separate BQ query (data.standards).
  const stdData    = data.standards || {};
  const today      = new Date();
  const quarterStart = new Date(QUARTER.start + 'T00:00:00');
  const daysElapsed  = Math.floor((today - quarterStart) / 86400000);
  const weeksElapsed = Math.max(1, Math.ceil((daysElapsed + 1) / 7));
  const dialerCallsPerDay = Math.round(((Number(calls.dialer_calls) || 0) / 5) * 10) / 10;
  // CW this week: filter from pipeline data using local-time Monday (avoids UTC offset bug)
  const _dow = (today.getDay() + 6) % 7; // 0=Mon … 6=Sun
  const _mon = new Date(today.getFullYear(), today.getMonth(), today.getDate() - _dow);
  const _mondayStr = `${_mon.getFullYear()}-${String(_mon.getMonth()+1).padStart(2,'0')}-${String(_mon.getDate()).padStart(2,'0')}`;
  // Opps created this week: team view sums BQ result; rep view uses per-rep BQ result
  const oppsCreatedThisWeek = isTeamView
    ? (data.teamOppsCreated || []).reduce((sum, r) => sum + (Number(r.opps_created) || 0), 0)
    : Number(data.oppsCreated) || 0;

  // For team view, aggregate from full team pipeline; for rep view, use repMetrics + per-rep standards
  let cwThisWeek, avgPBRStd, winRateStd, timeToWin, outboundMix;
  if (isTeamView) {
    const allRepsPipeline = data.pipeline || [];
    let teamWonThisWeek = 0, teamWonPBR = 0, teamWonCount = 0, teamClosedCount = 0;
    for (const d of allRepsPipeline) {
      if (d.is_won) {
        teamWonCount++;
        teamWonPBR += Number(d.pbr) || 0;
        if ((toDateStr(d.close_date) || '') >= _mondayStr) teamWonThisWeek++;
      }
      if (d.is_closed) teamClosedCount++;
    }
    cwThisWeek = teamWonThisWeek;
    avgPBRStd  = teamWonCount > 0 ? teamWonPBR / teamWonCount : null;
    winRateStd = teamClosedCount > 0 ? teamWonCount / teamClosedCount : null;
    // TTW + outbound mix from team standards BQ query (aggregated avg)
    const allStd = data.teamStandards || [];
    const ttwVals = allStd.map(s => Number(s.avg_time_to_win)).filter(v => !isNaN(v) && v > 0);
    const obVals  = allStd.map(s => Number(s.outbound_mix)).filter(v => !isNaN(v));
    timeToWin   = ttwVals.length > 0 ? Math.round(ttwVals.reduce((a, b) => a + b, 0) / ttwVals.length) : null;
    outboundMix = obVals.length > 0 ? obVals.reduce((a, b) => a + b, 0) / obVals.length : null;
  } else {
    cwThisWeek  = m.wonDeals.filter(d => (toDateStr(d.close_date) || '') >= _mondayStr).length;
    avgPBRStd   = m.avgDealSize;
    winRateStd  = m.winRate != null ? m.winRate / 100 : null;
    timeToWin   = stdData.avg_time_to_win != null ? Math.round(Number(stdData.avg_time_to_win)) : null;
    outboundMix = stdData.outbound_mix != null ? Number(stdData.outbound_mix) : null;
  }

  function stdBar(label, val, target, valStr, tgtStr, inverted = false) {
    if (val == null) return `<div class="progress-row">
      <span class="progress-label">${label}</span>
      <div class="progress-track" style="opacity:0.2"><div class="progress-fill bar-yellow" style="width:0%"></div></div>
      <span class="progress-value" style="color:var(--text-muted)">— / ${tgtStr}</span>
    </div>`;
    const pct = inverted
      ? Math.min(100, Math.round((target / Math.max(val, 0.01)) * 100))
      : Math.min(100, Math.round((val / Math.max(target, 0.01)) * 100));
    const cls = pct >= 100 ? 'bar-green' : pct >= 70 ? 'bar-yellow' : 'bar-red';
    return `<div class="progress-row">
      <span class="progress-label">${label}</span>
      <div class="progress-track"><div class="progress-fill ${cls}" style="width:${pct}%"></div></div>
      <span class="progress-value">${valStr} / ${tgtStr}</span>
    </div>`;
  }

  return `
    <div class="hero-banner">
      <div class="hero-title">Activity · ${QUARTER.label}</div>
      <div class="metric-grid" style="grid-template-columns:repeat(4,1fr)">
        ${kpiCard('Interactions', String(total), '/week', total, teamWeeklyAvg.total, v => String(v))}
        ${kpiCard('Meetings', String(meetings), '/week', meetings, teamWeeklyAvg.meetings, v => String(v))}
        ${kpiCard('Emails Sent', String(outboundEmails), '/week', null, null)}
        ${kpiCard('Replies In', String(inboundReplies), '/week', null, null)}
      </div>
      <div class="metric-grid" style="grid-template-columns:repeat(4,1fr);margin-top:8px">
        ${kpiCard('Avg Duration', avgDuration + 'm', '', null, null)}
        ${kpiCard('Open Rate', emailOpenRate + '%', '', null, null)}
        ${kpiCard('Reply Rate', emailReplyRate + '%', '', null, null)}
        ${kpiCard('Acts / Deal', String(activitiesPerDeal), '', activitiesPerDealNum, teamAvgPerDealNum, v => v != null ? v.toFixed(1) : '—')}
      </div>
    </div>

    <!-- Q2 Standards Tracker -->
    ${renderStandardsTrackerCard(data, isTeamView, weeksElapsed, dialerCallsPerDay, meetings, oppsCreatedThisWeek, cwThisWeek, avgPBRStd, winRateStd, timeToWin, outboundMix, stdBar)}

    <div class="overview-two-col">
      <!-- Weekly Charts + Monthly Breakdown -->
      <div>
        <div class="card">
          <div class="card-title">Weekly Calls (12 Weeks)</div>
          <div class="chart-wrap"><canvas id="chartActivityCalls"></canvas></div>
        </div>
        <div class="card">
          <div class="card-title">Weekly Meetings (12 Weeks)</div>
          <div class="chart-wrap"><canvas id="chartActivityMeetings"></canvas></div>
        </div>
        <div class="card">
          <div class="card-title">Monthly Activity Breakdown</div>
          <div class="chart-wrap"><canvas id="chartActivityMonthly"></canvas></div>
        </div>
      </div>
      <!-- Activity Comparison + Peer Benchmark -->
      <div>
        <div class="card">
          <div class="card-title">${isTeamView ? 'Coach Team Comparison — Avg per Rep / Week' : 'Activity Comparison — You vs Team Avg'}</div>
          ${isTeamView ? (() => {
            const coachTeamIds = Object.keys(COACHES);
            const metrics = ['Interactions', 'Meetings', 'Connected', 'Dialer', 'Transcribed'];
            const metricKeys = ['total', 'meetings', 'connected', 'dialer', 'transcribed'];
            const headerCols = coachTeamIds.map(tid => `<th style="font-size:11px;padding:6px 8px;text-align:center">${COACHES[tid].name.split(' ')[0]}</th>`).join('');
            const rows = metrics.map((label, i) => {
              const vals = coachTeamIds.map(tid => (coachAvgs[tid] || {})[metricKeys[i]] || 0);
              const maxVal = Math.max(...vals);
              const cells = vals.map(v => {
                const color = v === maxVal && v > 0 ? 'var(--success)' : 'var(--text-secondary)';
                return `<td style="text-align:center;padding:6px 8px;color:${color};font-weight:${v === maxVal && v > 0 ? '700' : '400'}">${v}</td>`;
              }).join('');
              return `<tr><td style="padding:6px 8px;font-weight:600;font-size:12px">${label}</td>${cells}</tr>`;
            }).join('');
            return `<table style="width:100%;border-collapse:collapse;font-size:13px"><thead><tr><th style="text-align:left;padding:6px 8px"></th>${headerCols}</tr></thead><tbody>${rows}</tbody></table>`;
          })() : (() => {
            const header = `<div class="compare-header">
              <div class="compare-header-spacer"></div>
              <div class="compare-header-label">You</div>
              <div class="compare-header-label">Team Avg</div>
              <div class="compare-header-bar"></div>
            </div>`;
            return header + compareRows;
          })()}
        </div>
        <div class="card">
          <div class="card-title">${isTeamView ? 'Team Activity \u2014 Last Week' : 'Peer Benchmark \u2014 Last 7 Days'}</div>
          <div id="activity-peer-benchmark"><div class="ai-loading">Loading...</div></div>
        </div>
      </div>
    </div>
  `;
}

function initActivityCharts(data) {
  const allTrend = data.activityTrend || [];
  const repTrend = data.viewMode === 'team' ? [] : allTrend; // rep view: single-rep rows
  const teamTrend = data.teamActivityTrend || [];
  const isTeamView = data.viewMode === 'team';

  if (allTrend.length === 0) return;

  // Shared helpers
  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  const today = new Date();
  const currentMonthKey = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, '0')}`;

  function isoWeekToMonth(weekStr) {
    const [yr, wk] = weekStr.split('-W').map(Number);
    const jan4 = new Date(yr, 0, 4);
    const mon1 = new Date(jan4);
    mon1.setDate(jan4.getDate() - ((jan4.getDay() + 6) % 7));
    const weekStart = new Date(mon1);
    weekStart.setDate(mon1.getDate() + (wk - 1) * 7);
    const m = weekStart.getMonth();
    return { key: `${weekStart.getFullYear()}-${String(m + 1).padStart(2, '0')}`, label: `${monthNames[m]} ${weekStart.getFullYear()}` };
  }

  // Dynamic color palette for 6 coaches
  const COACH_COLORS = [
    'rgba(13, 148, 136, 1)',   // teal
    'rgba(249, 115, 22, 0.85)',// orange
    'rgba(99, 102, 241, 0.85)',// indigo
    'rgba(236, 72, 153, 0.85)',// pink
    'rgba(34, 197, 94, 0.85)', // green
    'rgba(139, 92, 246, 0.85)',// violet
  ];
  const COACH_FILLS = COACH_COLORS.map(c => c.replace(/[\d.]+\)$/, '0.08)'));

  if (isTeamView) {
    // Build per-coach email sets and sizes
    const coachTeams = {};
    const coachNames = {};
    const coachSizes = {};
    Object.entries(COACHES).forEach(([tid, coach]) => {
      coachTeams[tid] = new Set(Object.entries(REP_ROSTER).filter(([, r]) => r.team === tid).map(([e]) => e));
      coachNames[tid] = `${coach.name.split(' ')[0]}'s Team`;
      coachSizes[tid] = coachTeams[tid].size || 1;
    });
    const coachIds = Object.keys(COACHES);

    const weeks = [...new Set(allTrend.map(r => r.week))].sort();
    const weekLabels = weeks.map(w => formatWeekLabel(w));

    function weekTeamAvg(week, teamEmails, metric) {
      const rows = allTrend.filter(r => r.week === week && teamEmails.has(r.rep_email));
      if (!rows.length) return 0;
      return Math.round(rows.reduce((a, r) => a + (Number(r[metric]) || 0), 0) / rows.length);
    }

    createLineChart('chartActivityCalls', {
      labels: weekLabels,
      datasets: coachIds.map((tid, i) =>
        repLineDataset(coachNames[tid], weeks.map(w => weekTeamAvg(w, coachTeams[tid], 'dialer')), COACH_COLORS[i], COACH_FILLS[i])
      ),
      xRotation: 45,
    });

    createLineChart('chartActivityMeetings', {
      labels: weekLabels,
      datasets: coachIds.map((tid, i) =>
        repLineDataset(coachNames[tid], weeks.map(w => weekTeamAvg(w, coachTeams[tid], 'meetings')), COACH_COLORS[i], COACH_FILLS[i])
      ),
      xRotation: 45,
    });

    // Monthly breakdown: total activity avg/rep per coach team
    const coachMonthMaps = {};
    for (const tid of coachIds) coachMonthMaps[tid] = {};
    for (const row of allTrend) {
      const { key, label } = isoWeekToMonth(row.week);
      if (key > currentMonthKey) continue;
      for (const tid of coachIds) {
        if (coachTeams[tid].has(row.rep_email)) {
          if (!coachMonthMaps[tid][key]) coachMonthMaps[tid][key] = { label, calls: 0, meetings: 0 };
          coachMonthMaps[tid][key].calls += Number(row.dialer) || 0;
          coachMonthMaps[tid][key].meetings += Number(row.meetings) || 0;
        }
      }
    }
    const allMonthKeys = new Set();
    for (const tid of coachIds) Object.keys(coachMonthMaps[tid]).forEach(k => allMonthKeys.add(k));
    const teamMonths = [...allMonthKeys].sort();
    if (teamMonths.length > 0) {
      const firstMap = coachMonthMaps[coachIds.find(tid => Object.keys(coachMonthMaps[tid]).length > 0)] || {};
      createBarChart('chartActivityMonthly', {
        labels: teamMonths.map(k => {
          for (const tid of coachIds) { if (coachMonthMaps[tid][k]) return coachMonthMaps[tid][k].label; }
          return k;
        }),
        datasets: coachIds.map((tid, i) =>
          repBarDataset(coachNames[tid], teamMonths.map(k => Math.round(((coachMonthMaps[tid][k]?.calls || 0) + (coachMonthMaps[tid][k]?.meetings || 0)) / coachSizes[tid])), COACH_COLORS[i].replace(/[\d.]+\)$/, '0.7)'))
        ),
        stacked: false,
      });
    }
  } else {
    // Rep view: single rep line vs team avg
    const repEmails = new Set(Object.keys(REP_ROSTER));
    const labels = repTrend.map(w => formatWeekLabel(w.week));

    const teamAvgCalls = computeWeeklyTeamAvg(teamTrend, 'dialer', repEmails);
    const teamAvgMeetings = computeWeeklyTeamAvg(teamTrend, 'meetings', repEmails);

    createLineChart('chartActivityCalls', {
      labels,
      datasets: [
        repLineDataset(data.repName || 'You', repTrend.map(w => Number(w.dialer) || 0), COLORS.calls, COLORS.callsFill),
        teamAvgLineDataset(repTrend.map(w => teamAvgCalls[w.week] || 0)),
      ],
      xRotation: 45,
    });

    createLineChart('chartActivityMeetings', {
      labels,
      datasets: [
        repLineDataset(data.repName || 'You', repTrend.map(w => Number(w.meetings) || 0), COLORS.meetings, COLORS.meetingsFill),
        teamAvgLineDataset(repTrend.map(w => teamAvgMeetings[w.week] || 0)),
      ],
      xRotation: 45,
    });

    const repMonthMap = {};
    for (const w of repTrend) {
      const { key, label } = isoWeekToMonth(w.week);
      if (key > currentMonthKey) continue;
      if (!repMonthMap[key]) repMonthMap[key] = { label, calls: 0, meetings: 0 };
      repMonthMap[key].calls += Number(w.dialer) || 0;
      repMonthMap[key].meetings += Number(w.meetings) || 0;
    }

    const teamMonthMap = {};
    for (const row of teamTrend) {
      if (!repEmails.has(row.rep_email)) continue;
      const { key, label } = isoWeekToMonth(row.week);
      if (key > currentMonthKey) continue;
      if (!teamMonthMap[key]) teamMonthMap[key] = { label, calls: 0, meetings: 0 };
      teamMonthMap[key].calls += Number(row.dialer) || 0;
      teamMonthMap[key].meetings += Number(row.meetings) || 0;
    }

    const months = Object.keys(repMonthMap).sort();
    const monthRepCount = repEmails.size || 1;
    if (months.length > 0) {
      createBarChart('chartActivityMonthly', {
        labels: months.map(k => repMonthMap[k].label),
        datasets: [
          repBarDataset('Calls', months.map(k => repMonthMap[k].calls), COLORS.callsBar),
          repBarDataset('Meetings', months.map(k => repMonthMap[k].meetings), COLORS.meetingsBar),
          teamAvgBarDataset(months.map(k => Math.round(((teamMonthMap[k]?.calls || 0) + (teamMonthMap[k]?.meetings || 0)) / monthRepCount))),
        ],
        stacked: false,
      });
    }
  }

  // Peer benchmark — team view shows D2C1 vs D2C2, rep view shows individual vs peers
  if (isTeamView) {
    renderTeamBenchmark(data);
  } else {
    loadActivityPeerBenchmark(data.repEmail);
  }
}

async function loadActivityPeerBenchmark(repEmail) {
  const container = document.getElementById('activity-peer-benchmark');
  if (!container) return;
  if (!repEmail) {
    container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Select a rep to see peer benchmarks.</div>';
    return;
  }
  try {
    const allReps = await fetchRepActivityComparison(7);
    if (!allReps || allReps.length === 0) {
      container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">No team activity data available.</div>';
      return;
    }
    const me = allReps.find(r => r.rep_email === repEmail) || {};
    const teamAvg = {};
    const metrics = ['total', 'meetings', 'connected', 'transcribed'];
    for (const met of metrics) {
      const vals = allReps.map(r => Number(r[met]) || 0);
      teamAvg[met] = vals.reduce((a, b) => a + b, 0) / vals.length;
    }

    const bars = metrics.map(met => {
      const myVal = Number(me[met]) || 0;
      const avg = teamAvg[met];
      const max = Math.max(myVal, avg, 1);
      const myPct = (myVal / max) * 100;
      const avgPct = (avg / max) * 100;
      const label = met === 'total' ? 'Interactions' : met.charAt(0).toUpperCase() + met.slice(1);
      const color = myVal >= avg ? 'var(--accent)' : 'var(--danger)';

      return `<div style="margin-bottom:10px">
        <div style="display:flex;justify-content:space-between;font-size:12px;margin-bottom:3px">
          <span>${label}</span>
          <span style="color:${color};font-weight:600">${myVal} <span style="color:var(--text-muted);font-weight:400">vs ${avg.toFixed(0)} avg</span></span>
        </div>
        <div style="position:relative;height:14px;background:#eee;border-radius:4px;overflow:hidden">
          <div style="position:absolute;left:0;top:0;height:100%;width:${myPct}%;background:${color};border-radius:4px;opacity:0.8"></div>
          <div style="position:absolute;left:${avgPct}%;top:0;height:100%;width:2px;background:var(--text);opacity:0.3"></div>
        </div>
      </div>`;
    }).join('');

    container.innerHTML = `${bars}
      <div style="font-size:11px;color:var(--text-muted);margin-top:4px">Black line = team average. Last 7 days.</div>`;
  } catch (_) {
    container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Could not load benchmark data.</div>';
  }
}

function renderTeamBenchmark(data) {
  const container = document.getElementById('activity-peer-benchmark');
  if (!container) return;

  // Compute per-coach averages from activityTrend (most recent ISO week)
  const allRepsTrend = data.activityTrend || [];
  const allEmails = new Set(Object.keys(REP_ROSTER));
  const recentByRep = {};
  for (const row of allRepsTrend) {
    if (!allEmails.has(row.rep_email)) continue;
    if (!recentByRep[row.rep_email] || row.week > recentByRep[row.rep_email].week) {
      recentByRep[row.rep_email] = row;
    }
  }
  const tAvg = (emails, metric) => {
    const vals = emails.map(e => Number(recentByRep[e]?.[metric]) || 0);
    return Math.round(vals.reduce((a, b) => a + b, 0) / (emails.length || 1));
  };

  const benchmarkColors = [
    'var(--accent)',
    'rgba(249, 115, 22, 0.85)',
    'rgba(99, 102, 241, 0.85)',
    'rgba(236, 72, 153, 0.85)',
    'rgba(34, 197, 94, 0.85)',
    'rgba(139, 92, 246, 0.85)',
  ];

  const coachIds = Object.keys(COACHES);
  const coachBenchmarkData = {};
  for (const tid of coachIds) {
    const teamEmails = Object.entries(REP_ROSTER).filter(([, r]) => r.team === tid).map(([e]) => e);
    coachBenchmarkData[tid] = { total: tAvg(teamEmails, 'total'), meetings: tAvg(teamEmails, 'meetings'), connected: tAvg(teamEmails, 'connected'), dialer: tAvg(teamEmails, 'dialer'), transcribed: tAvg(teamEmails, 'transcribed') };
  }

  const metrics = [
    { key: 'total',       label: 'Interactions' },
    { key: 'meetings',    label: 'Meetings' },
    { key: 'connected',   label: 'Connected' },
    { key: 'dialer',      label: 'Dialer Calls' },
    { key: 'transcribed', label: 'Transcribed' },
  ];

  const bars = metrics.map(({ key, label }) => {
    const vals = coachIds.map(tid => coachBenchmarkData[tid][key]);
    const max = Math.max(...vals, 1);
    const rows = coachIds.map((tid, i) => {
      const v = vals[i];
      const pct = (v / max) * 100;
      const coachFirst = COACHES[tid].name.split(' ')[0];
      return `<div style="display:flex;gap:6px;align-items:center;margin-bottom:2px">
        <span style="font-size:10px;color:var(--text-muted);width:52px">${coachFirst}</span>
        <div style="flex:1;height:8px;background:#eee;border-radius:4px;overflow:hidden">
          <div style="height:100%;width:${pct}%;background:${benchmarkColors[i]};border-radius:4px"></div>
        </div>
        <span style="font-size:11px;font-weight:600;width:22px;text-align:right">${v}</span>
      </div>`;
    }).join('');
    return `<div style="margin-bottom:10px">
      <div style="font-size:12px;font-weight:500;margin-bottom:4px">${label}</div>
      ${rows}
    </div>`;
  }).join('');
  container.innerHTML = `${bars}
    <div style="font-size:11px;color:var(--text-muted);margin-top:4px">Per-rep avg \u00b7 Most recent week.</div>`;
}

// ─── Analytics sub-tab (manager only) ───

const usageCollection = quick.db.collection('hub_tool_usage');
const sessionsCollection = quick.db.collection('hub_sessions');
const tabViewsCollection = quick.db.collection('hub_tab_views');

const TOOL_NAMES = {
  email_composer: 'Email Composer',
  account_research: 'Acct Research',
  objection_handler: 'Objection Handler',
};

function renderAnalytics() {
  return `
    <div class="analytics-head">
      <div>
        <div class="analytics-title">Analytics</div>
        <div class="analytics-sub">AI Tool & Platform Usage · ${QUARTER.label}</div>
      </div>
    </div>
    <div id="analytics-container"><div class="ai-loading">Loading usage data...</div></div>
  `;
}

async function loadAnalytics() {
  const container = document.getElementById('analytics-container');
  if (!container) return;
  try {
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - 13 * 7);

    // Fetch all 3 data sources in parallel
    const [toolUsage, sessions, tabViews] = await Promise.all([
      usageCollection.where({}).orderBy('created_at', 'desc').limit(2000).find().catch(() => []),
      sessionsCollection.where({}).orderBy('created_at', 'desc').limit(2000).find().catch(() => []),
      tabViewsCollection.where({}).orderBy('created_at', 'desc').limit(2000).find().catch(() => []),
    ]);

    const recentTools = toolUsage.filter(u => new Date(u.created_at) >= cutoff);
    const recentSessions = sessions.filter(s => new Date(s.created_at) >= cutoff);
    const recentViews = tabViews.filter(v => new Date(v.created_at) >= cutoff);

    // ── Stat card metrics ──
    const totalSessions = recentSessions.length;
    const uniqueSessionUsers = new Set(recentSessions.map(s => s.email)).size;
    const avgSessions = uniqueSessionUsers > 0 ? Math.round(totalSessions / uniqueSessionUsers) : 0;
    const totalToolUses = recentTools.length;
    const toolCounts = {};
    for (const u of recentTools) { toolCounts[u.tool] = (toolCounts[u.tool] || 0) + 1; }
    const topTool = Object.entries(toolCounts).sort(([, a], [, b]) => b - a)[0];
    const topToolLabel = topTool ? (TOOL_NAMES[topTool[0]] || topTool[0].replace(/_/g, ' ')) : '—';

    // ── AI Tool Usage by Rep (left table) ──
    const repToolBreakdown = {};
    for (const [email] of Object.entries(REP_ROSTER)) {
      repToolBreakdown[email] = { email_composer: 0, account_research: 0, objection_handler: 0, total: 0 };
    }
    for (const u of recentTools) {
      if (repToolBreakdown[u.rep_email]) {
        if (repToolBreakdown[u.rep_email][u.tool] !== undefined) repToolBreakdown[u.rep_email][u.tool]++;
        repToolBreakdown[u.rep_email].total++;
      }
    }
    const repToolRows = Object.entries(repToolBreakdown)
      .sort(([, a], [, b]) => b.total - a.total)
      .map(([email, counts]) => {
        const rep = REP_ROSTER[email];
        return `<tr>
          <td><strong>${rep ? rep.name : email}</strong></td>
          <td class="text-right">${counts.email_composer}</td>
          <td class="text-right">${counts.account_research}</td>
          <td class="text-right">${counts.objection_handler}</td>
          <td class="text-right"><strong>${counts.total}</strong></td>
        </tr>`;
      }).join('');

    // ── Section Usage (right table) ──
    const sectionCounts = {};
    for (const v of recentViews) {
      const label = v.subtab ? `${v.tab} › ${v.subtab}` : v.tab;
      sectionCounts[label] = (sectionCounts[label] || 0) + 1;
    }
    const sectionRows = Object.entries(sectionCounts).sort(([, a], [, b]) => b - a)
      .map(([section, views]) => `<tr>
        <td>${section.replace(/\b\w/g, c => c.toUpperCase())}</td>
        <td class="text-right"><strong>${views}</strong></td>
      </tr>`).join('');

    // ── 13-Week Heatmap ──
    const weeks = [];
    for (let i = 12; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i * 7);
      weeks.push(getISOWeek(d));
    }

    const repWeekly = {};
    for (const email of Object.keys(REP_ROSTER)) {
      repWeekly[email] = weeks.map(() => 0);
    }
    for (const u of recentTools) {
      if (!repWeekly[u.rep_email]) continue;
      const w = getISOWeek(new Date(u.created_at));
      const idx = weeks.indexOf(w);
      if (idx >= 0) repWeekly[u.rep_email][idx]++;
    }
    const maxVal = Math.max(1, ...Object.values(repWeekly).flat());

    const heatmapRows = Object.entries(repWeekly).map(([email, counts]) => {
      const rep = REP_ROSTER[email];
      const name = rep ? rep.name.split(' ')[0] : email.split('@')[0];
      const cells = counts.map(v => {
        const alpha = v === 0 ? 0.08 : (0.15 + (v / maxVal) * 0.85);
        return `<div class="analytics-heatmap-cell" title="${v} uses" style="background:rgba(13,148,136,${alpha.toFixed(2)})"></div>`;
      }).join('');
      return `<div style="display:flex;align-items:center;gap:12px">
        <div style="font-size:12px;font-weight:600;width:100px;flex-shrink:0">${name}</div>
        <div class="analytics-heatmap">${cells}</div>
      </div>`;
    }).join('');

    container.innerHTML = `
      <div class="analytics-grid-4">
        <div class="analytics-stat-card">
          <div class="analytics-stat-label">Total Sessions (13w)</div>
          <div class="analytics-stat-value">${totalSessions || '—'}</div>
        </div>
        <div class="analytics-stat-card">
          <div class="analytics-stat-label">Avg Sessions / User</div>
          <div class="analytics-stat-value">${avgSessions || '—'}</div>
        </div>
        <div class="analytics-stat-card">
          <div class="analytics-stat-label">Most Used AI Tool</div>
          <div class="analytics-stat-value" style="font-size:18px">${topToolLabel}</div>
        </div>
        <div class="analytics-stat-card">
          <div class="analytics-stat-label">AI Tool Uses (13w)</div>
          <div class="analytics-stat-value">${totalToolUses}</div>
        </div>
      </div>

      <div class="analytics-grid-2">
        <div class="card">
          <div class="card-title">AI Tool Usage by Rep</div>
          <div style="overflow-x:auto">
            <table class="data-table" style="table-layout:fixed">
              <colgroup><col style="width:30%"><col style="width:18%"><col style="width:18%"><col style="width:18%"><col style="width:16%"></colgroup>
              <thead><tr><th>Rep</th><th class="text-right">Email</th><th class="text-right">Research</th><th class="text-right">Objection</th><th class="text-right">Total</th></tr></thead>
              <tbody>${repToolRows || '<tr><td colspan="5" class="text-muted">No usage data yet</td></tr>'}</tbody>
            </table>
          </div>
        </div>
        <div class="card">
          <div class="card-title">Section Usage</div>
          <div style="overflow-x:auto">
            <table class="data-table" style="table-layout:fixed">
              <colgroup><col style="width:65%"><col style="width:35%"></colgroup>
              <thead><tr><th>Section</th><th class="text-right">Views</th></tr></thead>
              <tbody>${sectionRows || '<tr><td colspan="2" class="text-muted">No data yet — usage will appear after visits accumulate</td></tr>'}</tbody>
            </table>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-title">13-Week AI Tool Usage Heatmap</div>
        <div style="display:flex;flex-direction:column;gap:10px">
          ${heatmapRows}
        </div>
        <div style="font-size:11px;color:var(--text-muted);margin-top:10px">Each cell = 1 week. Darker = more tool uses.</div>
      </div>
    `;
  } catch (err) {
    container.innerHTML = `<div class="empty-state">Could not load analytics: ${err.message}</div>`;
  }
}

// ─── Rep Coaching sub-tab ───

function renderRepCoaching(data) {
  const recent = data.recentCalls || [];

  const recentRows = recent.slice(0, 20).map(c => {
    const platform = (c.platform || '').replace('salesloft_', '').replace('google_', '');
    const duration = c.call_duration_minutes ? `${Math.round(c.call_duration_minutes)}m` : '—';
    const title = c.call_title || '—';
    const hasSummary = c.summary_text ? true : false;
    const hasTranscript = c.has_transcript;
    const transcriptBadge = hasTranscript
      ? '<span style="color:var(--accent)">Yes</span>'
      : '<span class="text-muted">No</span>';
    const scoreBtn = hasTranscript
      ? `<button class="refresh-btn" style="font-size:11px;padding:2px 6px" onclick="window.scoreCall('${c.event_id}',this)">Score</button>`
      : '';
    return `<tr>
      <td>${formatDate(c.event_start)}</td>
      <td class="text-truncate"><strong>${title}</strong></td>
      <td>${platform}</td>
      <td>${duration}</td>
      <td>${c.call_disposition || '—'}</td>
      <td>${transcriptBadge} ${scoreBtn}</td>
    </tr>
    ${hasSummary ? `<tr><td colspan="6" style="padding:4px 10px 12px;font-size:12px;color:var(--text-secondary);border-bottom:1px solid var(--border)"><em>${truncate(c.summary_text, 200)}</em></td></tr>` : ''}
    <tr id="call-meddic-${c.event_id}" style="display:none"><td colspan="6" style="padding:0"></td></tr>`;
  }).join('');

  return `
    <div class="card">
      <div class="card-title">
        MEDDIC Scorecard
        <button class="refresh-btn" onclick="window.runMEDDIC()">Analyze my calls</button>
      </div>
      <div id="meddic-container">
        <div class="ai-loading">Loading cached scorecard...</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">MEDDIC Score Trend</div>
      <div id="meddic-trend-container">
        <div class="empty-state" style="padding:12px;font-size:12px">Loading trend...</div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">Recent Calls <span style="font-size:12px;color:var(--text-muted);font-weight:400;margin-left:8px">Click "Score" on transcribed calls for per-call MEDDIC</span></div>
      ${recent.length === 0 ? '<div class="empty-state">No calls in the last 14 days.</div>' : `
        <table class="data-table" style="table-layout:fixed">
          <colgroup><col style="width:12%"><col style="width:28%"><col style="width:14%"><col style="width:12%"><col style="width:16%"><col style="width:18%"></colgroup>
          <thead><tr><th>Date</th><th>Call</th><th>Platform</th><th>Duration</th><th>Disposition</th><th>Transcript</th></tr></thead>
          <tbody>${recentRows}</tbody>
        </table>
      `}
    </div>
  `;
}

function wireCoachingButtons(data) {
  window.runMEDDIC = async function() {
    const container = document.getElementById('meddic-container');
    container.innerHTML = '<div class="ai-loading">Fetching transcripts and scoring against MEDDIC...</div>';
    try {
      const transcripts = await fetchTranscriptDetails(data.repEmail, 5, true);
      if (!transcripts || transcripts.length === 0) {
        container.innerHTML = '<div class="empty-state">No transcribed calls found in the last 14 days.</div>';
        return;
      }
      await scoreMEDDIC(transcripts, container, data.repEmail || data.sfName);
    } catch (err) {
      container.innerHTML = `<div class="empty-state">Error: ${err.message}</div>`;
    }
  };

  window.scoreCall = async function(eventId, btn) {
    const row = document.getElementById(`call-meddic-${eventId}`);
    if (!row) return;

    if (row.style.display !== 'none') {
      row.style.display = 'none';
      return;
    }

    row.style.display = '';
    const cell = row.querySelector('td');
    cell.innerHTML = '<div class="ai-loading" style="padding:12px">Scoring this call...</div>';
    btn.disabled = true;
    btn.textContent = '...';

    try {
      const cached = await MEDDIC_CALL_CACHE.where({ key: eventId }).orderBy('created_at', 'desc').limit(1).find();
      if (cached.length) {
        const age = Date.now() - new Date(cached[0].created_at).getTime();
        if (age < MEDDIC_CALL_TTL_MS) {
          renderCallMEDDIC(cell, JSON.parse(cached[0].data));
          btn.textContent = 'Score';
          btn.disabled = false;
          return;
        }
      }

      const callData = await fetchCallWithTranscript(eventId, true);
      if (!callData || !callData.transcript_details) {
        cell.innerHTML = '<div class="empty-state" style="padding:8px;font-size:12px">No transcript available.</div>';
        btn.textContent = 'Score';
        btn.disabled = false;
        return;
      }

      const text = extractTranscriptText(callData.transcript_details);
      if (!text) {
        cell.innerHTML = '<div class="empty-state" style="padding:8px;font-size:12px">Transcript is empty.</div>';
        btn.textContent = 'Score';
        btn.disabled = false;
        return;
      }

      const prompt = `Score this single sales call transcript against MEDDIC. Give brief scores (1-10) for each dimension with one-sentence evidence.

Return ONLY JSON: {"dimensions":[{"name":"Metrics","score":N,"evidence":"..."},{"name":"Economic Buyer","score":N,"evidence":"..."},{"name":"Decision Criteria","score":N,"evidence":"..."},{"name":"Decision Process","score":N,"evidence":"..."},{"name":"Identify Pain","score":N,"evidence":"..."},{"name":"Champion","score":N,"evidence":"..."}],"overall":N}

TRANSCRIPT:
${truncate(text, 6000)}`;

      const response = await streamAI(prompt, { maxTokens: 1000 });
      const jsonMatch = response.match(/\{[\s\S]*\}/);
      if (!jsonMatch) throw new Error('No JSON in response');
      const scorecard = JSON.parse(jsonMatch[0]);

      renderCallMEDDIC(cell, scorecard);
      try {
        await MEDDIC_CALL_CACHE.create({ key: eventId, data: JSON.stringify(scorecard), created_at: new Date().toISOString() });
      } catch (_) {}
    } catch (err) {
      cell.innerHTML = `<div class="empty-state" style="padding:8px;font-size:12px">Error: ${err.message}</div>`;
    }
    btn.textContent = 'Score';
    btn.disabled = false;
  };
}

// ─── Outlook Computation Helpers ───

function computeOutlookMetrics(deals, quota, wonPBR) {
  const totalPipeline = deals.reduce((s, d) => s + (Number(d.pbr) || 0), 0);
  const commitPBR = deals
    .filter(d => d.forecast_category === 'Forecast')
    .reduce((s, d) => s + (Number(d.pbr) || 0), 0);
  const projected = wonPBR + commitPBR;
  const attainment = quota > 0 ? Math.round((projected / quota) * 100) : 0;
  const gap = Math.max(0, quota - projected);
  const coverage = gap > 0 ? totalPipeline / gap : (totalPipeline > 0 ? 999 : 0);
  const status = attainment >= 85 ? 'on_track' : attainment >= 60 ? 'watch' : 'at_risk';
  const statusLabel = status === 'on_track' ? 'On Track' : status === 'watch' ? 'Watch' : 'At Risk';
  const statusColor = status === 'on_track' ? 'var(--success)' : status === 'watch' ? 'var(--warning)' : 'var(--danger)';

  // High-risk deals
  const highRiskDeals = deals.filter(d => {
    const flags = computeHygieneFlags(d);
    const highFlags = flags.filter(f => f.level === 'high').length;
    return highFlags >= 2;
  });

  return { quota, wonPBR, totalPipeline, commitPBR, projected, attainment, gap, coverage, status, statusLabel, statusColor, deals, highRiskDeals };
}

function computeTeamOutlookMetrics(allDeals, quotaMap, wonMap, allRosterNames = new Set()) {
  const groups = {};
  for (const d of allDeals) {
    const owner = d.salesforce_owner_name || 'Unknown';
    if (!groups[owner]) groups[owner] = [];
    groups[owner].push(d);
  }

  const repMetricsMap = {};
  let teamQuota = 0, teamProjected = 0, teamPipeline = 0;

  for (const [sfName, repDeals] of Object.entries(groups)) {
    const quota = quotaMap[sfName] || getQuotaFallback();
    const won = wonMap[sfName] || 0;
    const m = computeOutlookMetrics(repDeals, quota, won);
    repMetricsMap[sfName] = m;
    teamQuota += quota;
    teamProjected += m.projected;
    teamPipeline += m.totalPipeline;
  }

  // Include reps with quota but no deals
  for (const [sfName, quota] of Object.entries(quotaMap)) {
    if (!repMetricsMap[sfName]) {
      repMetricsMap[sfName] = computeOutlookMetrics([], quota, wonMap[sfName] || 0);
      teamQuota += quota;
      teamProjected += wonMap[sfName] || 0;
    }
  }

  // Ensure every roster rep appears (even with no deals AND no BQ quota)
  for (const sfName of allRosterNames) {
    if (!repMetricsMap[sfName]) {
      const quota = quotaMap[sfName] || getQuotaFallback();
      repMetricsMap[sfName] = computeOutlookMetrics([], quota, wonMap[sfName] || 0);
      teamQuota += quota;
      teamProjected += wonMap[sfName] || 0;
    }
  }

  const teamAttainment = teamQuota > 0 ? Math.round((teamProjected / teamQuota) * 100) : 0;
  const teamGap = Math.max(0, teamQuota - teamProjected);
  const teamCoverage = teamGap > 0 ? teamPipeline / teamGap : (teamPipeline > 0 ? 999 : 0);
  const allHighRisk = allDeals.filter(d => {
    const flags = computeHygieneFlags(d);
    return flags.filter(f => f.level === 'high').length >= 2;
  });
  const atRiskPipeline = allHighRisk.reduce((s, d) => s + (Number(d.pbr) || 0), 0);

  return { teamQuota, teamProjected, teamAttainment, teamPipeline, teamCoverage, atRiskPipeline, allHighRisk, repMetricsMap };
}

function outlookScoreColor(score) {
  if (score >= 85) return 'var(--success)';
  if (score >= 65) return 'var(--warning)';
  return 'var(--danger)';
}

function outlookBadgeClass(val, greenThresh = 85, amberThresh = 60) {
  if (val >= greenThresh) return 'green';
  if (val >= amberThresh) return 'amber';
  return 'red';
}

// ─── Rep Outlook sub-tab ───

async function loadRepOutlook(data) {
  const el = document.getElementById('overview-subtab-content');
  if (!el) return;
  try {
    const sfName = data.sfName || Object.values(REP_ROSTER).find(r => r.name)?.sfName || '';
    const [deals, quotas, wonPBR] = await Promise.all([
      fetchNextQuarterPipeline(sfName),
      fetchNextQuarterQuotas(),
      fetchNextQuarterWonPBR(sfName),
    ]);
    const repQuotaRow = quotas.find(q => q.name === sfName && q.role_type === 'rep');
    const quota = repQuotaRow ? Number(repQuotaRow.quarterly_quota) : getQuotaFallback();
    const won = typeof wonPBR === 'number' ? wonPBR : 0;
    const metrics = computeOutlookMetrics(deals, quota, won);
    el.innerHTML = renderRepOutlookView(metrics, !repQuotaRow);
    loadOutlookCoachingNotes(deals);
    loadOutlookMEDDIC(deals);
    loadOutlookRecommendedActions(metrics, data);
  } catch (err) {
    el.innerHTML = `<div class="card"><div class="empty-state">Could not load ${NEXT_QUARTER.label} pipeline: ${err.message}</div></div>`;
  }
}

function renderRepOutlookView(m, isEstimatedQuota) {
  if (!m.deals || m.deals.length === 0) {
    return `<div class="card"><div class="empty-state">No open deals with close dates in ${NEXT_QUARTER.label}.</div></div>`;
  }

  const attColor = m.attainment >= 85 ? 'var(--success)' : m.attainment >= 60 ? 'var(--warning)' : 'var(--danger)';
  const quotaLabel = isEstimatedQuota ? `${NEXT_QUARTER.label} Quota (est.)` : `${NEXT_QUARTER.label} Quota`;

  return `
    <div class="analytics-head">
      <div>
        <div class="analytics-title">${NEXT_QUARTER.label} Outlook</div>
        <div class="analytics-sub">${m.deals.length} deal${m.deals.length !== 1 ? 's' : ''} in pipeline</div>
      </div>
    </div>

    <div class="analytics-grid-4">
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">${quotaLabel}</div>
        <div class="analytics-stat-value">${formatUSD(m.quota)}</div>
      </div>
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">Projected Revenue</div>
        <div class="analytics-stat-value" style="color:${attColor}">${formatUSD(m.projected)}</div>
        <div class="outlook-attainment-bar"><div class="outlook-attainment-fill" style="width:${Math.min(m.attainment, 100)}%;background:${attColor}"></div></div>
        <div class="analytics-stat-sub">${m.attainment}% projected attainment</div>
      </div>
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">Pipeline Coverage</div>
        <div class="analytics-stat-value">${m.coverage >= 999 ? '∞' : m.coverage.toFixed(1) + 'x'}</div>
        <div class="analytics-stat-sub">${formatUSD(m.totalPipeline)} total pipeline</div>
      </div>
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">Status</div>
        <div class="analytics-stat-value"><span class="outlook-status-badge ${m.status}">${m.statusLabel}</span></div>
      </div>
    </div>

    <div class="card" style="margin-bottom:16px">
      <div class="card-title">Key Deals</div>
      <div class="deal-pipeline-grid">${m.deals.map(d => renderOutlookDealCard(d)).join('')}</div>
    </div>

    <div class="card">
      <div class="card-title">Recommended Actions — ${NEXT_QUARTER.label}</div>
      <div id="outlook-actions-container" class="outlook-actions-loading">Generating recommended actions...</div>
    </div>`;
}

function renderOutlookDealCard(d, showRep = false) {
  const pbr = Number(d.pbr) || 0;
  const flags = computeHygieneFlags(d);
  const highFlags = flags.filter(f => f.level === 'high').length;
  const riskLevel = highFlags >= 2 ? 'high' : highFlags === 1 ? 'medium' : flags.length > 0 ? 'medium' : 'low';
  const repLabel = showRep ? ` <span style="font-size:12px;color:var(--text-muted)">— ${d.salesforce_owner_name || ''}</span>` : '';

  return `
    <div class="deal-pipeline-card risk-${riskLevel}">
      <div class="deal-card-header">
        <span class="deal-card-name">${sfLink(d.opp_name, d.opportunity_id)}${repLabel}</span>
        <div class="deal-card-meta">
          <span class="deal-card-pbr">${formatUSD(pbr)}</span>
          <span class="deal-card-stage">${d.current_stage_name || '—'}</span>
          <span class="deal-card-risk-badge ${riskLevel}">${riskLevel}</span>
        </div>
      </div>
      <div style="font-size:12px;color:var(--text-secondary)">Close: ${formatDate(d.close_date)}${d.next_step ? ` · Next: ${d.next_step}` : ''}</div>
      <div class="deal-meddic-chips" id="deal-meddic-${d.opportunity_id}">
        <span class="meddic-chip scoring">Scoring MEDDIC...</span>
      </div>
      ${flags.length > 0 ? `<div class="deal-card-flags">${renderRiskBadge(flags)}</div>` : ''}
      <div class="deal-card-coaching streaming" id="deal-coaching-${d.opportunity_id}">Generating coaching note...</div>
    </div>`;
}

// ─── Outlook AI coaching notes ───

async function loadOutlookCoachingNotes(deals) {
  await Promise.allSettled(deals.map(d => loadSingleDealCoaching(d)));
}

async function loadSingleDealCoaching(deal) {
  const el = document.getElementById(`deal-coaching-${deal.opportunity_id}`);
  if (!el) return;
  const cacheKey = `${deal.opportunity_id}:${NEXT_QUARTER.label}`;

  try {
    const items = await dealCoachingCache.where({ key: cacheKey }).orderBy('created_at', 'desc').limit(1).find();
    if (items.length) {
      const age = Date.now() - new Date(items[0].created_at).getTime();
      if (age < COACHING_TTL_MS) {
        el.textContent = items[0].data;
        el.classList.remove('streaming');
        return;
      }
    }
  } catch (_) {}

  const flags = computeHygieneFlags(deal);
  const flagText = flags.map(f => f.label).join(', ') || 'None';
  const prompt = `You are a sales coach for Shopify Plus reps. Give ONE concise coaching action (2 sentences max) for this deal closing next quarter.
Deal: ${deal.opp_name || 'Unknown'}
Stage: ${deal.current_stage_name || '—'}
PBR: $${(Number(deal.pbr) || 0).toLocaleString()}
Close date: ${toDateStr(deal.close_date) || '—'}
Flags: ${flagText}
Next step: ${deal.next_step || 'Not set'}
Be specific and actionable. No preamble.`;

  try {
    const note = await streamAI(prompt, { maxTokens: 200 });
    el.textContent = note;
    el.classList.remove('streaming');
    try { await dealCoachingCache.create({ key: cacheKey, data: note, created_at: new Date().toISOString() }); } catch (_) {}
  } catch (err) {
    el.textContent = 'Could not generate coaching note.';
    el.classList.remove('streaming');
  }
}

// ─── Outlook AI MEDDIC scoring per deal ───

async function loadOutlookMEDDIC(deals) {
  await Promise.allSettled(deals.map(d => loadSingleDealMEDDIC(d)));
}

async function loadSingleDealMEDDIC(deal) {
  const el = document.getElementById(`deal-meddic-${deal.opportunity_id}`);
  if (!el) return;
  const cacheKey = `${deal.opportunity_id}:${NEXT_QUARTER.label}`;

  // Check cache
  try {
    const items = await outlookMeddicCache.where({ key: cacheKey }).orderBy('created_at', 'desc').limit(1).find();
    if (items.length) {
      const age = Date.now() - new Date(items[0].created_at).getTime();
      if (age < COACHING_TTL_MS) {
        renderMEDDICChips(el, JSON.parse(items[0].data));
        return;
      }
    }
  } catch (_) {}

  const flags = computeHygieneFlags(deal);
  const flagText = flags.map(f => f.label).join(', ') || 'None';
  const prompt = `Score this Shopify Plus deal on the MEDDIC framework (0-100 for each dimension). Return ONLY valid JSON, no markdown.
Deal: ${deal.opp_name || 'Unknown'}
Stage: ${deal.current_stage_name || '—'}
PBR: $${(Number(deal.pbr) || 0).toLocaleString()}
Close date: ${toDateStr(deal.close_date) || '—'}
Forecast category: ${deal.forecast_category || '—'}
Next step: ${deal.next_step || 'Not set'}
Flags: ${flagText}

Score based on: M=Metrics (quantified value prop), E=Economic Buyer (access/engagement), D=Decision Criteria (alignment), D2=Decision Process (understanding), I=Identify Pain (clarity), C=Champion (strength), CE=Compelling Event (urgency).
Return JSON: {"M":score,"E":score,"D":score,"I":score,"C":score,"CE":score}`;

  try {
    const raw = await streamAI(prompt, { maxTokens: 150 });
    const match = raw.match(/\{[^}]+\}/);
    if (match) {
      const scores = JSON.parse(match[0]);
      renderMEDDICChips(el, scores);
      try { await outlookMeddicCache.create({ key: cacheKey, data: JSON.stringify(scores), created_at: new Date().toISOString() }); } catch (_) {}
    } else {
      el.innerHTML = '';
    }
  } catch (_) {
    el.innerHTML = '';
  }
}

function renderMEDDICChips(el, scores) {
  el.innerHTML = Object.entries(scores).map(([k, v]) => {
    const color = outlookScoreColor(Number(v));
    return `<span class="meddic-chip" style="background:${color}22;color:${color}">${k}:${v}</span>`;
  }).join('');
}

// ─── Outlook Recommended Actions (AI) ───

async function loadOutlookRecommendedActions(metrics, data) {
  const el = document.getElementById('outlook-actions-container');
  if (!el) return;
  const sfName = data.sfName || '';
  const cacheKey = `${sfName}:${NEXT_QUARTER.label}`;

  // Check cache
  try {
    const items = await outlookActionsCache.where({ key: cacheKey }).orderBy('created_at', 'desc').limit(1).find();
    if (items.length) {
      const age = Date.now() - new Date(items[0].created_at).getTime();
      if (age < COACHING_TTL_MS) {
        renderOutlookActions(el, JSON.parse(items[0].data));
        return;
      }
    }
  } catch (_) {}

  const topDeals = metrics.deals.slice(0, 10).map(d => {
    const flags = computeHygieneFlags(d);
    return `- ${d.opp_name}: $${(Number(d.pbr) || 0).toLocaleString()}, ${d.current_stage_name || '—'}, close ${toDateStr(d.close_date) || '—'}, forecast: ${d.forecast_category || '—'}, next step: ${d.next_step || 'Not set'}, flags: ${flags.map(f => f.label).join(', ') || 'None'}`;
  }).join('\n');

  const prompt = `You are a senior sales coach for a Shopify Plus rep. Analyze their ${NEXT_QUARTER.label} pipeline and provide 4 prioritized actions.

Context:
- Quota: $${metrics.quota.toLocaleString()}
- Projected: $${metrics.projected.toLocaleString()} (${metrics.attainment}% attainment)
- Gap to quota: $${metrics.gap.toLocaleString()}
- Pipeline coverage: ${metrics.coverage >= 999 ? 'Infinite' : metrics.coverage.toFixed(1) + 'x'}
- Total pipeline: $${metrics.totalPipeline.toLocaleString()}
- High-risk deals: ${metrics.highRiskDeals.length}

Top deals:
${topDeals}

Return ONLY a JSON array of 4 actions. Each action: {"rank":1,"title":"short title","desc":"1-2 sentence explanation","pillar":"M|E|D|I|C|CE"}.
Tag each with the most relevant MEDDIC pillar. Be specific to these deals. No markdown.`;

  try {
    const raw = await streamAI(prompt, { maxTokens: 1000 });
    const match = raw.match(/\[[\s\S]*\]/);
    if (match) {
      const actions = JSON.parse(match[0]);
      renderOutlookActions(el, actions);
      try { await outlookActionsCache.create({ key: cacheKey, data: JSON.stringify(actions), created_at: new Date().toISOString() }); } catch (_) {}
    } else {
      el.textContent = 'Could not parse actions.';
    }
  } catch (err) {
    el.textContent = 'Could not generate recommended actions.';
  }
}

function renderOutlookActions(el, actions) {
  el.innerHTML = actions.map((a, i) => {
    const pillarClass = a.pillar === 'CE' ? 'amber' : 'green';
    return `
      <div class="outlook-action-item">
        <div class="outlook-action-num">${a.rank || i + 1}</div>
        <div class="outlook-action-body">
          <div class="outlook-action-title">${a.title} <span class="outlook-action-pillar ${pillarClass}">${a.pillar}</span></div>
          <div class="outlook-action-desc">${a.desc}</div>
        </div>
      </div>`;
  }).join('');
}

// ─── Revenue trend (async loader) ───

async function loadRevenueTrend(data) {
  const canvas = document.getElementById('chartRevenueTrend');
  if (!canvas) return;
  try {
    const sfName = data.sfName || Object.values(REP_ROSTER).find(r => r.name)?.sfName || '';
    const [repWeeks, allRepsWeeks] = await Promise.all([
      fetchWeeklyWonPBR(sfName),
      fetchAllRepsWeeklyWonPBR(),
    ]);
    // Compute team totals per week from all reps data
    const weekTotals = {};
    for (const row of (allRepsWeeks || [])) {
      const w = row.week;
      weekTotals[w] = (weekTotals[w] || 0) + (Number(row.weekly_won) || 0);
    }
    const repCount = Object.keys(REP_ROSTER).length;

    // Build rep lookup
    const repByWeek = {};
    for (const w of (repWeeks || [])) repByWeek[w.week] = Number(w.weekly_won) || 0;

    // Use union of all weeks so team avg line is consistent across reps
    const allWeeks = [...new Set([
      ...(repWeeks || []).map(w => w.week),
      ...Object.keys(weekTotals),
    ])].sort();

    if (allWeeks.length === 0) {
      canvas.parentElement.innerHTML = '<div style="font-size:12px;color:var(--text-muted);padding:8px">No won deals this quarter yet.</div>';
      return;
    }

    const labels = allWeeks.map(w => formatWeekLabel(w));

    // Cumulative revenue — shows progression toward quota
    let repCum = 0;
    const repData = allWeeks.map(w => { repCum += (repByWeek[w] || 0); return repCum; });
    let teamCum = 0;
    const teamAvgData = allWeeks.map(w => { teamCum += Math.round((weekTotals[w] || 0) / repCount); return teamCum; });

    createLineChart('chartRevenueTrend', {
      labels,
      datasets: [
        repLineDataset(data.repName || 'You', repData, COLORS.primary, COLORS.primaryFill),
        teamAvgLineDataset(teamAvgData),
      ],
      yTickCallback: currencyTick,
    });
  } catch (_) {
    canvas.parentElement.innerHTML = '<div style="font-size:12px;color:var(--text-muted);padding:8px">—</div>';
  }
}

// ════════════════════════════════════════════════════════════════════
//  TEAM / MANAGER VIEW
// ════════════════════════════════════════════════════════════════════

const managerSettings = quick.db.collection('hub_manager_settings');

async function getManagerTarget() {
  try {
    const items = await managerSettings.where({ key: 'quarter_target' }).limit(1).find();
    if (items.length) {
      // One-time cleanup: delete stale manual override (13334001) so BQ takes over
      const val = Number(items[0].value) || 0;
      if (val === 13334001 || val === 13_334_001) {
        await managerSettings.delete(items[0]._id);
        console.log('[Hub] Cleared stale manual quota override');
        return 0;
      }
      return val;
    }
  } catch (_) {}
  return 0;
}

async function saveManagerTarget(value) {
  try {
    const items = await managerSettings.where({ key: 'quarter_target' }).limit(1).find();
    if (items.length) {
      await managerSettings.update(items[0]._id, { value: String(value) });
    } else {
      await managerSettings.create({ key: 'quarter_target', value: String(value) });
    }
  } catch (err) { console.error('[Hub] Failed to save target:', err); }
}

async function renderTeamOverview(el, data) {
  const savedTarget = await getManagerTarget();

  const wonMap = {};
  for (const row of (data.wonPBR || [])) wonMap[row.salesforce_owner_name] = Number(row.won_pbr) || 0;

  const pipelineMap = {};
  const pipelineExclPreQualMap = {};
  for (const row of (data.pipeline || [])) {
    if (row.is_closed) continue;
    const pbr = Number(row.pbr) || 0;
    if (!pipelineMap[row.salesforce_owner_name]) pipelineMap[row.salesforce_owner_name] = 0;
    pipelineMap[row.salesforce_owner_name] += pbr;
    if (row.current_stage_name !== 'Pre-Qualified') {
      if (!pipelineExclPreQualMap[row.salesforce_owner_name]) pipelineExclPreQualMap[row.salesforce_owner_name] = 0;
      pipelineExclPreQualMap[row.salesforce_owner_name] += pbr;
    }
  }

  const callMap = {};
  for (const row of (data.callActivity || [])) callMap[row.rep_email] = row;

  // Dynamic quota resolution
  const quotaRows = data.quotas || [];
  const coachRows = quotaRows.filter(r => r.role_type === 'coach');
  const repQuotaMap = {};
  for (const r of quotaRows.filter(r => r.role_type === 'rep')) {
    repQuotaMap[r.name] = Number(r.quarterly_quota) || 0;
  }

  let bqCoachQuota = 0;
  if (data.selectedTeam) {
    const coachRow = coachRows.find(r => r.user_role === (COACHES[data.selectedTeam]?.leadRole));
    bqCoachQuota = coachRow ? Number(coachRow.quarterly_quota) || 0 : 0;
  } else {
    bqCoachQuota = coachRows.reduce((sum, r) => sum + (Number(r.quarterly_quota) || 0), 0);
  }

  const hardcodedFallback = data.selectedTeam
    ? (COACH_QUOTAS[data.selectedTeam] || 0)
    : Object.values(COACH_QUOTAS).reduce((a, b) => a + b, 0);
  const autoQuota = bqCoachQuota > 0 ? bqCoachQuota : hardcodedFallback;
  const teamQuota = savedTarget > 0 ? savedTarget : autoQuota;
  const quotaSource = savedTarget > 0 ? 'manual' : bqCoachQuota > 0 ? 'BQ' : 'config';
  const teamWon = Object.values(wonMap).reduce((a, b) => a + b, 0);
  const teamPipeline = Object.values(pipelineMap).reduce((a, b) => a + b, 0);
  const teamAttainment = teamQuota > 0 ? (teamWon / teamQuota) * 100 : 0;
  const teamGap = Math.max(0, teamQuota - teamWon);
  const days = daysLeft();

  // Forecast
  const openDeals = (data.pipeline || []).filter(d => !d.is_closed);
  const today = new Date();
  const in30 = new Date(); in30.setDate(today.getDate() + 30);
  let forecastPBR = 0, bestCasePBR = 0, upsidePBR = 0;
  for (const d of openDeals) {
    const pbr = Number(d.pbr) || 0;
    const cs = toDateStr(d.close_date);
    const close = cs ? new Date(cs + 'T00:00:00') : null;
    if (d.forecast_category === 'Forecast') forecastPBR += pbr;
    else if (d.forecast_category === 'Pipeline') {
      if (close && close <= in30) bestCasePBR += pbr;
      else upsidePBR += pbr;
    }
  }
  const commit = teamWon + forecastPBR;
  const bestCase = commit + bestCasePBR;
  const upside = bestCase + upsidePBR;
  const maxFc = Math.max(upside, teamQuota);

  // Submitted forecast commit from BQ (coach-level: LEAD role, summed per coach)
  const forecastCommitRows = data.forecastCommit || [];
  let coachForecastCommit = 0;
  const coachForecastMap = {};
  for (const r of forecastCommitRows) {
    const val = Number(r.total_commit) || 0;
    coachForecastMap[r.owner_name] = val;
    coachForecastCommit += val;
  }

  // Team pipeline excl. Pre-Qualified + coverage
  const teamPipelineExclPQ = Object.values(pipelineExclPreQualMap).reduce((a, b) => a + b, 0);
  const teamCoverage = teamGap > 0 ? (teamPipelineExclPQ / teamGap) : 999;

  // Store team metrics for sub-tab re-renders
  window.__teamMetrics = {
    savedTarget, wonMap, pipelineMap, pipelineExclPreQualMap, callMap, repQuotaMap,
    autoQuota, teamQuota, quotaSource, teamWon, teamPipeline, teamAttainment, teamGap, days,
    openDeals, forecastPBR, bestCasePBR, upsidePBR, commit, bestCase, upside, maxFc,
    coachForecastMap, coachForecastCommit, teamPipelineExclPQ, teamCoverage,
  };

  el.innerHTML = `
    ${subtabNav(true)}
    <div id="overview-subtab-content"></div>
  `;

  renderTeamSubtabContent(data);
}

// ─── Team sub-tab content switcher ───

function renderTeamSubtabContent(data) {
  const el = document.getElementById('overview-subtab-content');
  if (!el) return;
  const tm = window.__teamMetrics || {};

  if (currentSubtab === 'performance') {
    el.innerHTML = renderTeamPerformance(data, tm);
    loadTeamRevenueTrend(data);
  } else if (currentSubtab === 'deals') {
    el.innerHTML = renderRepDeals(repMetrics(data));
  } else if (currentSubtab === 'coaching') {
    el.innerHTML = renderTeamCoaching();
    loadTeamMEDDIC();
    loadTeamMEDDICTrend();
    loadTeamMEDDICDistribution();
  } else if (currentSubtab === 'outlook') {
    el.innerHTML = '<div class="ai-loading">Loading next-quarter pipeline...</div>';
    loadTeamOutlook(data);
  } else if (currentSubtab === 'priorities') {
    el.innerHTML = renderRepPriorities();
    loadPriorities(data);
  } else if (currentSubtab === 'pipeline') {
    el.innerHTML = renderRepPipeline(data, repMetrics(data));
    initPipelineChart(data);
  } else if (currentSubtab === 'activity') {
    el.innerHTML = renderRepActivity(data, repMetrics(data));
    initActivityCharts(data);
  } else if (currentSubtab === 'forecast') {
    renderForecast(data, window.__currentUser, el);
  } else if (currentSubtab === 'health') {
    renderHealth(data, window.__currentUser, el);
  } else if (currentSubtab === 'analytics') {
    el.innerHTML = renderAnalytics();
    loadAnalytics();
  } else if (currentSubtab === 'settings') {
    el.innerHTML = renderSettings(data);
  }
}

// ─── Team Settings sub-tab ───

function renderSettings(data) {
  const fields = [
    { key: 'totalInteractionsPerWeek', label: 'Interactions / week',    step: 1    },
    { key: 'dialerCallsPerDay',        label: 'Dialer calls / day',     step: 1    },
    { key: 'meetingsPerWeek',          label: 'Meetings / week',        step: 1    },
    { key: 'oppsCreatedPerWeek',       label: 'Opps created / week',    step: 1    },
    { key: 'emailsPerWeek',            label: 'Emails / week',          step: 50   },
    { key: 'pipelineCoverage',         label: 'Pipeline coverage (x)',  step: 0.5  },
    { key: 'closedWonPerWeek',         label: 'Closed Won / week',      step: 0.5  },
    { key: 'avgPBRPerDeal',            label: 'Avg PBR / deal ($)',     step: 1000 },
    { key: 'winRateTarget',            label: 'Win rate target (0–1)',  step: 0.01 },
    { key: 'timeToWinDays',            label: 'Time to win (days max)', step: 1    },
    { key: 'outboundMixTarget',        label: 'Outbound mix (0–1)',     step: 0.05 },
  ];

  const inputs = fields.map(f => `
    <div style="display:flex;flex-direction:column;gap:4px">
      <label style="font-size:12px;color:var(--text-secondary)">${f.label}</label>
      <input type="number" value="${BENCHMARKS[f.key]}" step="${f.step}" min="0"
        style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:6px;font-size:14px;font-family:inherit;background:var(--card-bg)"
        onchange="window.saveBenchmark('${f.key}', this.value)">
    </div>
  `).join('');

  // ─── Quota Data card ───
  const quotaRows = data?.quotas || [];
  const bqRepMap = {};
  const bqCoachMap = {};
  for (const q of quotaRows) {
    if (q.role_type === 'rep') bqRepMap[q.name] = Number(q.quarterly_quota) || 0;
    else if (q.role_type === 'coach') bqCoachMap[q.name] = Number(q.quarterly_quota) || 0;
  }

  const fallback = getQuotaFallback();

  // Rep rows
  const repTableRows = Object.values(REP_ROSTER)
    .sort((a, b) => a.name.localeCompare(b.name))
    .map(rep => {
      const bqVal = bqRepMap[rep.sfName];
      const hasBQ = bqVal !== undefined && bqVal > 0;
      const displayVal = hasBQ ? formatUSDFull(bqVal) : formatUSDFull(fallback);
      const badge = hasBQ
        ? '<span style="display:inline-block;padding:1px 6px;border-radius:10px;font-size:10px;font-weight:600;background:var(--success-light);color:var(--success)">BQ</span>'
        : '<span style="display:inline-block;padding:1px 6px;border-radius:10px;font-size:10px;font-weight:600;background:var(--warning-light);color:var(--warning)">Fallback</span>';
      return `<tr>
        <td>${rep.name}</td>
        <td><span style="font-size:11px;color:var(--text-muted)">${rep.team}</span></td>
        <td class="text-right text-mono">${displayVal}</td>
        <td class="text-right">${badge}</td>
      </tr>`;
    }).join('');

  // Coach rows
  const coachTableRows = Object.entries(COACHES).map(([teamId, coach]) => {
    const bqVal = bqCoachMap[coach.name];
    const hasBQ = bqVal !== undefined && bqVal > 0;
    const hardcoded = COACH_QUOTAS[teamId] || 0;
    const displayVal = hasBQ ? formatUSDFull(bqVal) : formatUSDFull(hardcoded);
    const badge = hasBQ
      ? '<span style="display:inline-block;padding:1px 6px;border-radius:10px;font-size:10px;font-weight:600;background:var(--success-light);color:var(--success)">BQ</span>'
      : '<span style="display:inline-block;padding:1px 6px;border-radius:10px;font-size:10px;font-weight:600;background:var(--warning-light);color:var(--warning)">Config</span>';
    return `<tr style="background:var(--bg)">
      <td><strong>${coach.name}</strong></td>
      <td><span style="font-size:11px;color:var(--text-muted)">${teamId} (coach)</span></td>
      <td class="text-right text-mono">${displayVal}</td>
      <td class="text-right">${badge}</td>
    </tr>`;
  }).join('');

  const bqCount = Object.keys(bqRepMap).length;
  const totalReps = Object.keys(REP_ROSTER).length;

  return `
    <div class="card">
      <div class="card-title">Q2 Standards &amp; Benchmarks</div>
      <p style="font-size:13px;color:var(--text-secondary);margin:0 0 16px">
        Targets used across the Standards Tracker, progress bars, risk flags, and AI coaching prompts.
        Changes take effect immediately for all reps.
      </p>
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:16px">
        ${inputs}
      </div>
    </div>

    <div class="card">
      <div class="card-title">Quota Data</div>
      <p style="font-size:13px;color:var(--text-secondary);margin:0 0 12px">
        Quotas are pulled from BigQuery (<code>incentive_compensation_monthly_quotas</code>).
        ${bqCount}/${totalReps} reps have BQ data this quarter. Reps without BQ data use the fallback quota below.
      </p>

      <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;padding:12px 16px;background:var(--bg);border-radius:8px;border:1px solid var(--border)">
        <div style="flex:1">
          <label style="font-size:12px;font-weight:600;color:var(--text-secondary)">Fallback Rep Quota</label>
          <div style="font-size:11px;color:var(--text-muted);margin-top:2px">Used when BQ has no data for a rep. Accepts $, K, M formats.</div>
        </div>
        <div style="display:flex;align-items:center;gap:6px">
          <input id="quotaFallbackInput" type="text" value="${Math.round(fallback).toLocaleString()}"
            style="width:160px;padding:8px 12px;border:1px solid var(--border);border-radius:6px;font-size:14px;font-family:inherit;background:var(--card-bg);text-align:right"
            onchange="window.saveQuotaFallbackHandler(this.value)">
          <span id="quotaFallbackStatus" style="font-size:11px;color:var(--text-muted);min-width:40px"></span>
        </div>
      </div>

      <table class="data-table" style="table-layout:fixed;font-size:13px">
        <colgroup><col style="width:30%"><col style="width:25%"><col style="width:25%"><col style="width:20%"></colgroup>
        <thead><tr><th>Name</th><th>Team</th><th class="text-right">Quarterly Quota</th><th class="text-right">Source</th></tr></thead>
        <tbody>
          ${coachTableRows}
          ${repTableRows}
        </tbody>
      </table>
    </div>
  `;
}

// ─── Team Performance sub-tab ───

function renderTeamPerformance(data, tm) {
  const rosterEntries = Object.entries(REP_ROSTER)
    .filter(([, rep]) => !data.selectedTeam || rep.team === data.selectedTeam);

  const repRows = rosterEntries.map(([email, rep]) => {
    const won = tm.wonMap[rep.sfName] || 0;
    const pipeline = tm.pipelineMap[rep.sfName] || 0;
    const pipelineExclPQ = (tm.pipelineExclPreQualMap || {})[rep.sfName] || 0;
    const repQuota = tm.repQuotaMap[rep.sfName] || getQuotaFallback();
    const att = repQuota > 0 ? (won / repQuota) * 100 : 0;
    const gapVal = Math.max(0, repQuota - won);
    const coverageVal = gapVal > 0 ? pipelineExclPQ / gapVal : 999;
    const c = tm.callMap[email] || {};
    const meetings = Number(c.meetings) || 0;
    const totalCalls = Number(c.total_interactions) || 0;
    return { email, name: rep.name, won, pipeline, attainment: att, gap: gapVal, coverage: coverageVal, meetings, totalCalls };
  }).sort((a, b) => b.attainment - a.attainment);

  const repTableRows = repRows.map(r => {
    const barWidth = Math.min(100, Math.round(r.attainment));
    const barColor = r.attainment >= 80 ? 'var(--accent)' : r.attainment >= 50 ? 'var(--warning)' : 'var(--danger)';
    const coverageFlag = r.coverage < BENCHMARKS.pipelineCoverage ? `<span class="risk-flag risk-medium">${r.coverage.toFixed(1)}x</span>` : `${r.coverage.toFixed(1)}x`;
    const meetingFlag = r.meetings < BENCHMARKS.meetingsPerWeek ? `<span class="risk-flag risk-medium">${r.meetings}</span>` : `${r.meetings}`;
    return `<tr class="rep-row" onclick="document.getElementById('repSelector').value='${r.email}'; window.onRepChange('${r.email}')">
      <td><strong>${r.name}</strong></td>
      <td class="text-right">${formatPct(r.attainment)}<span class="attainment-bar" style="width:${barWidth}px;background:${barColor}"></span></td>
      <td class="text-right text-mono">${formatUSD(r.won)}</td>
      <td class="text-right text-mono">${formatUSD(r.gap)}</td>
      <td class="text-right text-mono">${formatUSD(r.pipeline)}</td>
      <td class="text-right">${coverageFlag}</td>
      <td class="text-right">${r.totalCalls}</td>
      <td class="text-right">${meetingFlag}</td>
    </tr>`;
  }).join('');

  // Team vs Target compare rows
  const coverage = tm.teamGap > 0 ? (tm.teamPipelineExclPQ || tm.teamPipeline) / tm.teamGap : 999;
  const compareMetrics = [
    ['Won PBR', tm.teamWon, tm.teamQuota, formatUSD],
    ['Commit', tm.commit, tm.teamQuota, formatUSD],
    ['Pipeline Coverage', coverage, BENCHMARKS.pipelineCoverage, v => v >= 999 ? '\u221e' : v.toFixed(1) + 'x'],
    ['Attainment', tm.teamAttainment, 100, v => formatPct(v)],
  ];
  const compareRows = compareMetrics.map(([label, actual, target, fmt]) => {
    const isAbove = actual >= target;
    const color = isAbove ? 'var(--success)' : 'var(--danger)';
    const pctFill = target > 0 ? Math.min((actual / target) * 100, 150) : 0;
    return `<div class="compare-row">
      <div class="compare-label">${label}</div>
      <div class="compare-you" style="color:${color}">${fmt(actual)}</div>
      <div class="compare-team">${fmt(target)}</div>
      <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:${pctFill}%;background:${color}"></div></div>
    </div>`;
  }).join('');

  return `
    <!-- Hero Banner -->
    <div class="hero-banner">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px">
        <div class="hero-title">${data.selectedTeam ? (COACHES[data.selectedTeam]?.name || data.selectedTeam) : 'SalesHub'} · ${QUARTER.label}</div>
        <div style="display:flex;align-items:center;gap:6px;font-size:12px;opacity:0.7">
          <label for="managerTarget">Quarter target:</label>
          <input id="managerTarget" type="text" value="${tm.savedTarget > 0 ? Math.round(tm.savedTarget).toLocaleString() : ''}"
            placeholder="${formatUSDFull(tm.autoQuota)}"
            style="width:120px;padding:3px 8px;border:1px solid rgba(255,255,255,0.3);border-radius:4px;background:rgba(255,255,255,0.1);color:#fff;font-size:12px;text-align:right"
            onchange="window.saveManagerTarget(this.value)">
          <span style="font-size:10px;opacity:0.5">${tm.quotaSource}</span>
        </div>
      </div>
      <div class="hero-name">${formatPct(tm.teamAttainment)} team attainment</div>
      <div class="metric-grid metric-grid-compact">
        ${renderMetricCard('Team Won PBR', formatUSD(tm.teamWon), `of ${formatUSDFull(tm.teamQuota)}`)}
        ${renderMetricCard('Team Gap', formatUSD(tm.teamGap), `${tm.days} days left`)}
        ${renderMetricCard('Team Pipeline', formatUSD(tm.teamPipeline), `${tm.openDeals.length} deals`)}
        ${renderMetricCard('Pipe Coverage', tm.teamCoverage < 999 ? `${tm.teamCoverage.toFixed(1)}x` : '—', 'excl. Pre-Qualified')}
        ${renderMetricCard('Forecast', tm.coachForecastCommit > 0 ? formatUSD(tm.coachForecastCommit) : '—', tm.coachForecastCommit > 0 ? `${formatPct((tm.coachForecastCommit / tm.teamQuota) * 100)} of quota` : 'No forecast submitted')}
      </div>
    </div>

    <!-- Forecast Bar -->
    <div class="card">
      <div class="card-title">Team Forecast</div>
      <div class="forecast-bar-container">
        <div class="forecast-bar">
          ${tm.teamWon > 0 ? `<div class="forecast-segment forecast-won" style="width:${(tm.teamWon / tm.maxFc) * 100}%"></div>` : ''}
          ${tm.forecastPBR > 0 ? `<div class="forecast-segment forecast-commit" style="width:${(tm.forecastPBR / tm.maxFc) * 100}%"></div>` : ''}
          ${tm.bestCasePBR > 0 ? `<div class="forecast-segment forecast-bestcase" style="width:${(tm.bestCasePBR / tm.maxFc) * 100}%"></div>` : ''}
          ${tm.upsidePBR > 0 ? `<div class="forecast-segment forecast-upside" style="width:${(tm.upsidePBR / tm.maxFc) * 100}%"></div>` : ''}
        </div>
        <div class="forecast-legend">
          <span class="legend-won">Won: ${formatUSD(tm.teamWon)}</span>
          <span class="legend-commit">Commit: ${formatUSD(tm.commit)}</span>
          <span class="legend-bestcase">Best Case: ${formatUSD(tm.bestCase)}</span>
          <span class="legend-upside">Upside: ${formatUSD(tm.upside)}</span>
        </div>
      </div>
    </div>

    <!-- Revenue Trend + Team vs Target -->
    <div class="overview-two-col">
      <div class="card">
        <div class="card-title">Team Revenue Trend</div>
        <div class="chart-wrap-lg"><canvas id="chartTeamRevenueTrend"></canvas></div>
      </div>
      <div class="card">
        <div class="card-title">Team vs Target</div>
        <div class="compare-header">
          <div class="compare-header-spacer"></div>
          <div class="compare-header-label">Actual</div>
          <div class="compare-header-label">Target</div>
          <div class="compare-header-bar"></div>
        </div>
        ${compareRows}
      </div>
    </div>

    <!-- Rep Performance Table -->
    <div class="card">
      <div class="card-title">Rep Performance</div>
      <table class="data-table team-table" style="table-layout:fixed">
        <colgroup>
          <col style="width:18%"><col style="width:14%"><col style="width:14%"><col style="width:14%">
          <col style="width:14%"><col style="width:10%"><col style="width:8%"><col style="width:8%">
        </colgroup>
        <thead><tr>
          <th>Rep</th><th class="text-right">Attainment</th><th class="text-right">Won PBR</th>
          <th class="text-right">Gap</th><th class="text-right">Pipeline</th><th class="text-right" title="Pipeline excl. Pre-Qualified / Gap">Coverage</th>
          <th class="text-right">Calls</th><th class="text-right">Meetings</th>
        </tr></thead>
        <tbody>${repTableRows}</tbody>
      </table>
      <div class="last-updated" style="margin-top:8px">Click a rep to drill in. Coverage = pipeline excl. Pre-Qualified / gap. Benchmarks: ${BENCHMARKS.meetingsPerWeek} meetings/week, ${BENCHMARKS.pipelineCoverage}x coverage.</div>
    </div>

    <!-- Q2 Standards — Team Overview -->
    ${renderTeamStandardsTable(data)}
  `;
}

function renderTeamStandardsTable(data) {
  const today        = new Date();
  const quarterStart = new Date(QUARTER.start + 'T00:00:00');
  const daysElapsed  = Math.floor((today - quarterStart) / 86400000);
  const weeksElapsed = Math.max(1, Math.ceil((daysElapsed + 1) / 7));

  // Index BQ standards data (TTW + outbound mix) by sfName
  const stdBySfName = {};
  for (const s of (data.teamStandards || [])) {
    stdBySfName[s.salesforce_owner_name] = s;
  }

  // Monday of current week in local time — avoids UTC offset bug from .toISOString()
  const _tToday   = new Date();
  const _tDow     = (_tToday.getDay() + 6) % 7;
  const _tMon     = new Date(_tToday.getFullYear(), _tToday.getMonth(), _tToday.getDate() - _tDow);
  const _tMonStr  = `${_tMon.getFullYear()}-${String(_tMon.getMonth()+1).padStart(2,'0')}-${String(_tMon.getDate()).padStart(2,'0')}`;

  // Derive CW this week, avg PBR, win rate from already-loaded pipeline data
  const allPipeline = data.pipeline || [];
  const pipelineBySf = {};
  for (const d of allPipeline) {
    const sf = d.salesforce_owner_name;
    if (!sf) continue;
    if (!pipelineBySf[sf]) pipelineBySf[sf] = { wonThisWeek: 0, wonPBR: 0, wonCount: 0, closedCount: 0 };
    if (d.is_won) {
      pipelineBySf[sf].wonCount++;
      pipelineBySf[sf].wonPBR += Number(d.pbr) || 0;
      if ((toDateStr(d.close_date) || '') >= _tMonStr) pipelineBySf[sf].wonThisWeek++;
    }
    if (d.is_closed) pipelineBySf[sf].closedCount++;
  }

  const rosterEntries = Object.entries(REP_ROSTER)
    .filter(([, rep]) => !data.selectedTeam || rep.team === data.selectedTeam);

  // Dot helper: returns a colored circle based on attainment ratio
  function dot(val, target, inverted = false) {
    if (val == null || target == null) return '<span style="color:var(--text-muted)">—</span>';
    const pct = inverted
      ? Math.min(100, (target / Math.max(val, 0.01)) * 100)
      : Math.min(100, (val / Math.max(target, 0.01)) * 100);
    const color = pct >= 100 ? 'var(--success)' : pct >= 70 ? 'var(--warning)' : 'var(--danger)';
    return `<span style="color:${color};font-weight:600">${pct >= 100 ? '●' : pct >= 70 ? '◑' : '○'}</span>`;
  }

  // Sort by number of green metrics desc
  const rows = rosterEntries.map(([, rep]) => {
    const pd      = pipelineBySf[rep.sfName] || { wonThisWeek: 0, wonPBR: 0, wonCount: 0, closedCount: 0 };
    const std     = stdBySfName[rep.sfName] || {};
    const cwPerWk = pd.wonThisWeek || 0;
    const avgPBR  = pd.wonCount > 0 ? pd.wonPBR / pd.wonCount : null;
    const winRate = pd.closedCount > 0 ? pd.wonCount / pd.closedCount : null;
    const ttw     = std.avg_time_to_win != null ? Math.round(Number(std.avg_time_to_win)) : null;
    const obMix   = std.outbound_mix    != null ? Number(std.outbound_mix)                : null;

    const greenCount = [
      cwPerWk >= BENCHMARKS.closedWonPerWeek                       ? 1 : 0,
      avgPBR  != null && avgPBR  >= BENCHMARKS.avgPBRPerDeal       ? 1 : 0,
      winRate != null && winRate >= BENCHMARKS.winRateTarget        ? 1 : 0,
      ttw     != null && ttw     <= BENCHMARKS.timeToWinDays        ? 1 : 0,
      obMix   != null && obMix   >= BENCHMARKS.outboundMixTarget    ? 1 : 0,
    ].reduce((a, b) => a + b, 0);

    return { name: rep.name, cwPerWk, avgPBR, winRate, ttw, obMix, greenCount };
  }).sort((a, b) => b.greenCount - a.greenCount);

  const tableRows = rows.map(r => `
    <tr>
      <td><strong>${r.name}</strong></td>
      <td class="text-right" title="Target: ${BENCHMARKS.closedWonPerWeek}/wk">${dot(r.cwPerWk, BENCHMARKS.closedWonPerWeek)} ${r.cwPerWk.toFixed(1)}</td>
      <td class="text-right" title="Target: ${formatUSD(BENCHMARKS.avgPBRPerDeal)}">${dot(r.avgPBR, BENCHMARKS.avgPBRPerDeal)} ${r.avgPBR != null ? formatUSD(r.avgPBR) : '—'}</td>
      <td class="text-right" title="Target: ${Math.round(BENCHMARKS.winRateTarget * 100)}%">${dot(r.winRate, BENCHMARKS.winRateTarget)} ${r.winRate != null ? Math.round(r.winRate * 100) + '%' : '—'}</td>
      <td class="text-right" title="Target: ≤${BENCHMARKS.timeToWinDays}d">${dot(r.ttw, BENCHMARKS.timeToWinDays, true)} ${r.ttw != null ? r.ttw + 'd' : '—'}</td>
      <td class="text-right" title="Target: ${Math.round(BENCHMARKS.outboundMixTarget * 100)}%">${dot(r.obMix, BENCHMARKS.outboundMixTarget)} ${r.obMix != null ? Math.round(r.obMix * 100) + '%' : '—'}</td>
      <td class="text-right">${r.greenCount}/5</td>
    </tr>`).join('');

  return `
    <div class="card">
      <div class="card-title" style="display:flex;justify-content:space-between;align-items:center">
        <span>${QUARTER.label} Standards — Team Tracker</span>
        <span style="font-size:12px;font-weight:400;color:var(--text-muted)">Wk ${weeksElapsed} of 13 · Conversion metrics Q2 to date · ● on track ◑ close ○ behind</span>
      </div>
      <table class="data-table" style="table-layout:fixed">
        <colgroup>
          <col style="width:20%"><col style="width:14%"><col style="width:14%"><col style="width:13%"><col style="width:13%"><col style="width:14%"><col style="width:12%">
        </colgroup>
        <thead><tr>
          <th>Rep</th>
          <th class="text-right">CW this wk</th>
          <th class="text-right">Avg PBR</th>
          <th class="text-right">Win Rate</th>
          <th class="text-right">Time to Win</th>
          <th class="text-right">Outbound Mix</th>
          <th class="text-right">On Track</th>
        </tr></thead>
        <tbody>${tableRows}</tbody>
      </table>
      <div class="last-updated" style="margin-top:8px">CW = deals closed Mon–today. Targets: ${BENCHMARKS.closedWonPerWeek} CW/wk · ${formatUSD(BENCHMARKS.avgPBRPerDeal)} avg PBR · ${Math.round(BENCHMARKS.winRateTarget * 100)}% win rate · ≤${BENCHMARKS.timeToWinDays}d to win · ${Math.round(BENCHMARKS.outboundMixTarget * 100)}% outbound. Adjust targets in Settings.</div>
    </div>
  `;
}

// ─── Team Revenue Trend chart (async loader) ───

async function loadTeamRevenueTrend(data) {
  const canvas = document.getElementById('chartTeamRevenueTrend');
  if (!canvas) return;
  try {
    const allRepsWeekly = await fetchAllRepsWeeklyWonPBR(data.selectedTeam || null);
    // Aggregate per-week totals and build cumulative
    const weekMap = {};
    for (const row of allRepsWeekly) {
      const wk = row.week;
      weekMap[wk] = (weekMap[wk] || 0) + (Number(row.weekly_won) || 0);
    }
    const weeks = Object.keys(weekMap).sort();
    if (weeks.length === 0) return;
    let cumulative = 0;
    const cumData = weeks.map(w => { cumulative += weekMap[w]; return cumulative; });
    const labels = weeks.map(formatWeekLabel);

    // Quota pace line
    const tm = window.__teamMetrics || {};
    const totalWeeks = weeks.length || 1;
    const paceData = weeks.map((_, i) => (tm.teamQuota || 0) * ((i + 1) / totalWeeks));

    createLineChart('chartTeamRevenueTrend', {
      labels,
      datasets: [
        repLineDataset('Team Revenue', cumData),
        { ...teamAvgLineDataset(paceData), label: 'Quota Pace' },
      ],
    }, { scales: { y: { ticks: { callback: currencyTick } } } });
  } catch (err) {
    console.error('[Hub] Team revenue trend failed:', err);
  }
}

// ─── Team Coaching sub-tab ───

function renderTeamCoaching() {
  const teamLabel = window.__appData?.selectedTeam
    ? (COACHES[window.__appData.selectedTeam]?.name || window.__appData.selectedTeam)
    : 'Team';
  return `
    <div class="hero-banner">
      <div class="hero-title">${teamLabel} Coaching · ${QUARTER.label}</div>
      <div class="hero-name">MEDDIC analysis across all reps</div>
    </div>

    <div class="overview-two-col">
      <div class="card">
        <div class="card-title">Team MEDDIC Trend</div>
        <div id="team-meddic-trend-container">
          <div class="empty-state" style="padding:12px;font-size:12px">Loading trend...</div>
        </div>
      </div>
      <div class="card">
        <div class="card-title">MEDDIC Score Distribution</div>
        <div id="team-meddic-distribution">
          <div class="empty-state" style="padding:12px;font-size:12px">Loading...</div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">
        Team MEDDIC Scores
        <button class="refresh-btn" onclick="window.refreshAllMEDDIC()">Refresh all scores</button>
      </div>
      <div id="team-meddic-container">
        <div class="ai-loading">Loading scores...</div>
      </div>
    </div>
  `;
}

// ─── Team Outlook sub-tab ───

async function loadTeamOutlook(data) {
  const el = document.getElementById('overview-subtab-content');
  if (!el) return;

  // If drilled into a rep, show rep view with back button
  if (outlookDrillRep) {
    return loadDrilledRepOutlook(data, el);
  }

  try {
    const selectedTeam = data.selectedTeam || null;
    const [allDeals, quotas, allWonRows] = await Promise.all([
      fetchNextQuarterPipeline('__all__'),
      fetchNextQuarterQuotas(selectedTeam),
      fetchAllRepsNextQuarterWonPBR(selectedTeam),
    ]);

    // Filter by team
    const teamSfNames = new Set(
      Object.entries(REP_ROSTER)
        .filter(([, r]) => !selectedTeam || r.team === selectedTeam)
        .map(([, r]) => r.sfName)
    );
    const filtered = selectedTeam ? allDeals.filter(d => teamSfNames.has(d.salesforce_owner_name)) : allDeals;

    // Build quota and won maps
    const quotaMap = {};
    for (const q of quotas) {
      if (q.role_type === 'rep') quotaMap[q.name] = Number(q.quarterly_quota) || getQuotaFallback();
    }
    const wonMap = {};
    for (const w of allWonRows) {
      wonMap[w.salesforce_owner_name] = Number(w.won_pbr) || 0;
    }

    const tm = computeTeamOutlookMetrics(filtered, quotaMap, wonMap, teamSfNames);
    el.innerHTML = renderTeamOutlookView(tm);
    initOutlookCoverageChart(tm);
    // Load AI for high-risk deals only (limited subset)
    if (tm.allHighRisk.length > 0) {
      loadOutlookCoachingNotes(tm.allHighRisk);
      loadOutlookMEDDIC(tm.allHighRisk);
    }
  } catch (err) {
    el.innerHTML = `<div class="card"><div class="empty-state">Could not load ${NEXT_QUARTER.label} pipeline: ${err.message}</div></div>`;
  }
}

async function loadDrilledRepOutlook(data, el) {
  try {
    const sfName = outlookDrillRep;
    const [deals, quotas, wonPBR] = await Promise.all([
      fetchNextQuarterPipeline(sfName),
      fetchNextQuarterQuotas(),
      fetchNextQuarterWonPBR(sfName),
    ]);
    const repQuota = quotas.find(q => q.name === sfName && q.role_type === 'rep');
    const quota = repQuota ? Number(repQuota.quarterly_quota) : getQuotaFallback();
    const won = typeof wonPBR === 'number' ? wonPBR : 0;
    const metrics = computeOutlookMetrics(deals, quota, won);

    // Rep selector buttons
    const repEntries = Object.entries(REP_ROSTER)
      .filter(([, r]) => !data.selectedTeam || r.team === data.selectedTeam);
    const repSelector = `<div class="outlook-rep-selector" style="margin-bottom:12px">
      ${repEntries.map(([, r]) => `<button class="outlook-rep-btn${r.sfName === sfName ? ' active' : ''}" onclick="window.drillOutlookRep('${r.sfName.replace(/'/g, "\\'")}')">${r.sfName.split(' ')[0]}</button>`).join('')}
    </div>`;

    const backBtn = `<button class="outlook-back-btn" onclick="window.drillOutlookRep(null)">\u2190 All Reps</button>`;
    const repNameLabel = `<div class="analytics-head" style="margin-bottom:0">
      <div>
        <div class="analytics-title">${NEXT_QUARTER.label} Outlook \u2014 ${sfName.split(' ')[0]}</div>
        <div class="analytics-sub">${sfName} \u00b7 ${NEXT_QUARTER.label}</div>
      </div>
    </div>`;

    el.innerHTML = backBtn + repSelector + repNameLabel + renderRepOutlookView(metrics, !repQuota).replace(/^[\s\S]*?<div class="analytics-grid-4">/, '<div class="analytics-grid-4">');
    loadOutlookCoachingNotes(deals);
    loadOutlookMEDDIC(deals);
    loadOutlookRecommendedActions(metrics, { ...data, sfName });
  } catch (err) {
    el.innerHTML = `<div class="card"><div class="empty-state">Could not load rep outlook: ${err.message}</div></div>`;
  }
}

function renderTeamOutlookView(tm) {
  const attColor = tm.teamAttainment >= 85 ? 'var(--success)' : tm.teamAttainment >= 60 ? 'var(--warning)' : 'var(--danger)';

  // Build rep comparison rows sorted by attainment desc
  const repRows = Object.entries(tm.repMetricsMap)
    .sort(([, a], [, b]) => b.attainment - a.attainment)
    .map(([sfName, m]) => {
      const attClass = outlookBadgeClass(m.attainment);
      const statusClass = m.status === 'on_track' ? 'green' : m.status === 'watch' ? 'amber' : 'red';
      return `<tr onclick="window.drillOutlookRep('${sfName.replace(/'/g, "\\'")}')">
        <td><strong>${sfName.split(' ')[0]}</strong></td>
        <td>${formatUSD(m.quota)}</td>
        <td>${formatUSD(m.projected)}</td>
        <td><span class="outlook-badge ${attClass}">${m.attainment}%</span></td>
        <td>${m.coverage >= 999 ? '\u221e' : m.coverage.toFixed(1) + 'x'}</td>
        <td><span class="outlook-badge ${statusClass}">${m.statusLabel}</span></td>
      </tr>`;
    }).join('');

  // High-risk deals across team
  const highRiskHtml = tm.allHighRisk.length > 0
    ? `<div class="card">
        <div class="card-title">High-Risk Deals \u2014 Team</div>
        <div class="deal-pipeline-grid">${tm.allHighRisk.map(d => renderOutlookDealCard(d, true)).join('')}</div>
      </div>`
    : '';

  return `
    <div class="analytics-head">
      <div>
        <div class="analytics-title">${NEXT_QUARTER.label} Outlook \u2014 Team View</div>
        <div class="analytics-sub">Manager \u00b7 ${NEXT_QUARTER.label} Forecast</div>
      </div>
    </div>

    <div class="analytics-grid-4">
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">Team ${NEXT_QUARTER.label} Quota</div>
        <div class="analytics-stat-value">${formatUSD(tm.teamQuota)}</div>
      </div>
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">Projected Revenue</div>
        <div class="analytics-stat-value" style="color:${attColor}">${formatUSD(tm.teamProjected)}</div>
        <div class="outlook-attainment-bar"><div class="outlook-attainment-fill" style="width:${Math.min(tm.teamAttainment, 100)}%;background:${attColor}"></div></div>
        <div class="analytics-stat-sub">${tm.teamAttainment}% projected attainment</div>
      </div>
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">Pipeline Coverage</div>
        <div class="analytics-stat-value">${tm.teamCoverage >= 999 ? '\u221e' : tm.teamCoverage.toFixed(1) + 'x'}</div>
        <div class="analytics-stat-sub">${formatUSD(tm.teamPipeline)} total pipeline</div>
      </div>
      <div class="analytics-stat-card">
        <div class="analytics-stat-label">At-Risk Pipeline</div>
        <div class="analytics-stat-value" style="color:var(--danger)">${formatUSD(tm.atRiskPipeline)}</div>
        <div class="analytics-stat-sub">${tm.allHighRisk.length} high-risk deal${tm.allHighRisk.length !== 1 ? 's' : ''}</div>
      </div>
    </div>

    <div class="analytics-grid-2">
      <div class="card">
        <div class="card-title">Rep ${NEXT_QUARTER.label} Projected Attainment</div>
        <div style="overflow-x:auto">
          <table class="outlook-comparison-table">
            <thead><tr><th>Rep</th><th>Quota</th><th>Projected</th><th>Attainment</th><th>Coverage</th><th>Status</th></tr></thead>
            <tbody>${repRows}</tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="card-title">Pipeline Coverage vs Quota</div>
        <div class="chart-wrap-lg"><canvas id="chartOutlookCoverage"></canvas></div>
      </div>
    </div>

    ${highRiskHtml}`;
}

function initOutlookCoverageChart(tm) {
  const canvas = document.getElementById('chartOutlookCoverage');
  if (!canvas) return;

  const reps = Object.entries(tm.repMetricsMap).sort(([a], [b]) => a.localeCompare(b));
  const labels = reps.map(([sfName]) => sfName.split(' ')[0]);
  const pipelineData = reps.map(([, m]) => m.totalPipeline);
  const quotaData = reps.map(([, m]) => m.quota);

  createBarChart('chartOutlookCoverage', {
    labels,
    datasets: [
      { label: 'Pipeline', data: pipelineData, backgroundColor: COLORS.primaryBar },
      { label: 'Quota', data: quotaData, backgroundColor: 'rgba(239,68,68,0.3)' },
    ],
  }, {
    scales: { y: { ticks: { callback: currencyTick } } },
    plugins: { legend: { display: true, position: 'top' } },
  });
}

window.drillOutlookRep = function(sfName) {
  outlookDrillRep = sfName;
  const data = window.__appData || {};
  const el = document.getElementById('overview-subtab-content');
  if (el) el.innerHTML = '<div class="ai-loading">Loading...</div>';
  loadTeamOutlook(data);
};

// ════════════════════════════════════════════════════════════════════
//  ASYNC LOADERS (shared by both views)
// ════════════════════════════════════════════════════════════════════

async function loadPriorities(data) {
  const container = document.getElementById('priorities-container');
  if (!container) return;
  try {
    await generatePriorities(data, container);
  } catch (err) {
    container.innerHTML = `<div class="empty-state">Could not generate priorities: ${err.message}</div>`;
  }
}

window.refreshPriorities = function() {
  const container = document.getElementById('priorities-container');
  if (container) {
    container.innerHTML = '<div class="ai-loading">Refreshing priorities...</div>';
    loadPriorities(window.__appData || {});
  }
};


window.saveManagerTarget = async function(rawValue) {
  let str = rawValue.replace(/[$,\s]/g, '');
  if (/k$/i.test(str)) str = String(parseFloat(str) * 1000);
  if (/m$/i.test(str)) str = String(parseFloat(str) * 1000000);
  const value = parseFloat(str) || 0;
  await saveManagerTarget(value);
  const data = window.__appData;
  if (data && data.viewMode === 'team') {
    const el = document.getElementById('tab-overview');
    if (el) renderTeamOverview(el, data, {});
  }
};

window.saveBenchmark = async function(field, rawValue) {
  const value = parseFloat(rawValue);
  if (isNaN(value) || value <= 0) return;
  await saveBenchmarkOverride(field, value);
};

window.saveQuotaFallbackHandler = async function(rawValue) {
  const input = document.getElementById('quotaFallbackInput');
  const status = document.getElementById('quotaFallbackStatus');
  let str = (rawValue || '').replace(/[$,\s]/g, '');
  if (!str) {
    // Empty = reset to default
    await saveQuotaFallback(QUOTA_USD);
    if (input) input.value = Math.round(QUOTA_USD).toLocaleString();
    if (status) { status.textContent = 'Reset'; setTimeout(() => status.textContent = '', 2000); }
    return;
  }
  if (/k$/i.test(str)) str = String(parseFloat(str) * 1000);
  if (/m$/i.test(str)) str = String(parseFloat(str) * 1000000);
  const value = parseFloat(str) || 0;
  if (value <= 0) return;
  await saveQuotaFallback(value);
  if (input) input.value = Math.round(value).toLocaleString();
  if (status) { status.textContent = 'Saved'; status.style.color = 'var(--success)'; setTimeout(() => { status.textContent = ''; status.style.color = ''; }, 2000); }
};

// ─── MEDDIC scoring (migrated from calls.js) ───

function extractTranscriptText(details) {
  if (!details || !Array.isArray(details)) return '';
  return details.map(d => {
    if (d.full_transcript && Array.isArray(d.full_transcript)) {
      return d.full_transcript.map(t => `${t.speaker_name || 'Unknown'}: ${t.speaker_text || ''}`).join('\n');
    }
    return '';
  }).filter(Boolean).join('\n\n');
}

function truncate(str, max) {
  if (!str) return '';
  return str.length > max ? str.slice(0, max) + '...' : str;
}

function renderCallMEDDIC(cell, sc) {
  const dims = (sc.dimensions || []).map(d => {
    const color = d.score >= 7 ? 'var(--accent)' : d.score >= 5 ? 'var(--warning)' : 'var(--danger)';
    return `<span style="display:inline-flex;align-items:center;gap:3px;margin-right:10px;font-size:12px">
      <strong style="color:${color}">${d.name.charAt(0)}: ${d.score}</strong>
    </span>`;
  }).join('');

  cell.innerHTML = `<div style="padding:10px 12px;background:#fafafa;border-radius:6px;margin:6px 0">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px">
      <span style="font-size:18px;font-weight:700">${sc.overall || '—'}<span style="font-size:12px;color:var(--text-muted)">/10</span></span>
      <span style="font-size:11px;color:var(--text-secondary)">|</span>
      ${dims}
    </div>
    ${(sc.dimensions || []).filter(d => d.evidence).slice(0, 3).map(d =>
      `<div style="font-size:11px;color:var(--text-secondary);margin-top:2px"><strong>${d.name}:</strong> ${truncate(d.evidence, 120)}</div>`
    ).join('')}
  </div>`;
}

async function getQuarterBaseline(repKey) {
  try {
    const all = await MEDDIC_CACHE.where({ key: repKey }).orderBy('created_at', 'asc').limit(20).find();
    const quarterStart = new Date(QUARTER.start + 'T00:00:00');
    const baseline = all.find(item => new Date(item.created_at) >= quarterStart);
    return baseline ? JSON.parse(baseline.data) : null;
  } catch (_) { return null; }
}

function renderScorecardResult(container, sc, baseline) {
  const baselineDimMap = {};
  if (baseline && baseline.dimensions) {
    for (const d of baseline.dimensions) baselineDimMap[d.name] = d.score;
  }

  const allDims = sc.dimensions || [];
  const lowest = allDims.length > 0 ? allDims.reduce((min, d) => (d.score < min.score ? d : min), allDims[0]) : null;
  const biggestGapHtml = lowest ? `<div class="coaching-gap-callout" style="margin-bottom:16px">Focus area: <strong>${lowest.name}</strong> (${lowest.score}/10) — your biggest coaching opportunity this quarter.</div>` : '';

  const dims = allDims.map(d => {
    const pct = (d.score / 10) * 100;
    const color = d.score >= 7 ? 'var(--accent)' : d.score >= 5 ? 'var(--warning)' : 'var(--danger)';
    const baseScore = baselineDimMap[d.name];
    let deltaBadge = '';
    if (baseScore != null && baseScore !== d.score) {
      const delta = d.score - baseScore;
      const cls = delta > 0 ? 'up' : 'down';
      const sign = delta > 0 ? '+' : '';
      deltaBadge = `<span class="delta-badge ${cls}">${sign}${delta.toFixed(1)}</span>`;
    }
    return `<div class="scorecard-item">
      <div class="scorecard-dim">${d.name}${deltaBadge}</div>
      <div class="scorecard-score" style="color:${color}">${d.score}/10</div>
      <div class="scorecard-bar"><div class="scorecard-fill" style="width:${pct}%;background:${color}"></div></div>
      <div style="font-size:11px;color:var(--text-secondary);margin-top:6px">${d.evidence || ''}</div>
    </div>`;
  }).join('');

  const strengths = (sc.strengths || []).map(s => `<li>${s}</li>`).join('');
  const improvements = (sc.improvements || []).map(s => `<li>${s}</li>`).join('');

  let overallDelta = '';
  if (baseline && baseline.overall != null && sc.overall != null) {
    const d = sc.overall - baseline.overall;
    if (d !== 0) {
      const cls = d > 0 ? 'up' : 'down';
      const sign = d > 0 ? '+' : '';
      overallDelta = `<span class="delta-badge ${cls}" style="font-size:13px">${sign}${d.toFixed(1)} vs start of quarter</span>`;
    }
  }

  container.innerHTML = `
    ${biggestGapHtml}
    <div style="text-align:center;margin-bottom:16px">
      <div style="font-size:36px;font-weight:700">${sc.overall || '—'}<span style="font-size:18px;color:var(--text-muted)">/10</span></div>
      <div style="font-size:13px;color:var(--text-secondary)">Overall MEDDIC Score ${overallDelta}</div>
    </div>
    <div class="scorecard-grid">${dims}</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:16px">
      <div style="padding:12px;background:var(--success-light);border-radius:8px">
        <div style="font-weight:600;font-size:13px;margin-bottom:6px">Strengths</div>
        <ul style="font-size:13px;padding-left:16px;margin:0">${strengths}</ul>
      </div>
      <div style="padding:12px;background:var(--warning-light);border-radius:8px">
        <div style="font-weight:600;font-size:13px;margin-bottom:6px">Areas to Improve</div>
        <ul style="font-size:13px;padding-left:16px;margin:0">${improvements}</ul>
      </div>
    </div>
  `;
}

function renderCompactScorecard(container, sc) {
  const dims = (sc.dimensions || []).map(d => {
    const pct = (d.score / 10) * 100;
    const color = d.score >= 7 ? 'var(--accent)' : d.score >= 5 ? 'var(--warning)' : 'var(--danger)';
    return `<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
      <span style="width:24px;font-size:12px;font-weight:600;color:var(--text-secondary)">${d.name.charAt(0)}</span>
      <div style="flex:1;height:6px;background:#eee;border-radius:3px;overflow:hidden"><div style="width:${pct}%;height:100%;background:${color};border-radius:3px"></div></div>
      <span style="width:28px;font-size:12px;font-weight:600;text-align:right">${d.score}</span>
    </div>`;
  }).join('');

  container.innerHTML = `
    <div style="display:flex;align-items:center;gap:16px">
      <div style="text-align:center;flex-shrink:0">
        <div style="font-size:28px;font-weight:700">${sc.overall || '—'}<span style="font-size:14px;color:var(--text-muted)">/10</span></div>
        <div style="font-size:11px;color:var(--text-secondary)">MEDDIC</div>
      </div>
      <div style="flex:1">${dims}</div>
    </div>
  `;
}

async function scoreMEDDIC(transcripts, container, repKey) {
  const callSummaries = transcripts.map((t, i) => {
    const text = extractTranscriptText(t.transcript_details);
    return `CALL ${i + 1} (${formatDate(t.event_start)}, ${Math.round(t.call_duration_minutes || 0)}min):\n${text || t.summary_text || 'No transcript text available'}`;
  }).join('\n\n---\n\n');

  const prompt = `You are an expert sales coach. Analyze these ${transcripts.length} recent sales call transcripts and score the rep against MEDDIC methodology.

Score each dimension 1-10 with brief evidence from the calls:
- **M**etrics: Did the rep quantify business impact / ROI for the prospect?
- **E**conomic Buyer: Did the rep identify and engage the decision-maker?
- **D**ecision Criteria: Did the rep uncover what the prospect will evaluate?
- **D**ecision Process: Did the rep map the buying process and timeline?
- **I**dentify Pain: Did the rep surface specific business pain points?
- **C**hampion: Did the rep build an internal advocate?

Also provide:
- Overall score (average of 6 dimensions)
- Top 2 strengths
- Top 2 areas for improvement with specific coaching advice

Return ONLY a JSON object, no other text:
{"dimensions":[{"name":"Metrics","score":7,"evidence":"..."},{"name":"Economic Buyer","score":6,"evidence":"..."},{"name":"Decision Criteria","score":5,"evidence":"..."},{"name":"Decision Process","score":4,"evidence":"..."},{"name":"Identify Pain","score":8,"evidence":"..."},{"name":"Champion","score":3,"evidence":"..."}],"overall":5.5,"strengths":["...","..."],"improvements":["...","..."]}

TRANSCRIPTS:
${callSummaries}`;

  const content = await streamAI(prompt, { maxTokens: 2000 });

  try {
    const jsonMatch = content.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON in response');
    const scorecard = JSON.parse(jsonMatch[0]);
    if (container) renderScorecardResult(container, scorecard);

    const week = getISOWeek();
    try {
      await MEDDIC_CACHE.create({ key: repKey, week, data: JSON.stringify(scorecard), created_at: new Date().toISOString() });
    } catch (_) {}

    // Update compact card on Performance sub-tab if visible
    const overviewContainer = document.getElementById('overview-meddic');
    if (overviewContainer) renderCompactScorecard(overviewContainer, scorecard);

    return scorecard;
  } catch (err) {
    if (container) container.innerHTML = `<div style="font-size:13px;white-space:pre-wrap;line-height:1.6">${content || 'No response from AI'}</div>`;
    return null;
  }
}

async function loadCachedMEDDIC(repKey) {
  const container = document.getElementById('meddic-container');
  if (!container) return;
  try {
    const items = await MEDDIC_CACHE.where({ key: repKey }).orderBy('created_at', 'desc').limit(1).find();
    if (items.length) {
      const age = Date.now() - new Date(items[0].created_at).getTime();
      if (age < MEDDIC_TTL_MS) {
        const baseline = await getQuarterBaseline(repKey);
        renderScorecardResult(container, JSON.parse(items[0].data), baseline);
        return;
      }
    }
  } catch (_) {}
  container.innerHTML = '<div class="empty-state">Click "Analyze my calls" to score your recent calls against MEDDIC methodology.</div>';
}

async function loadMEDDICTrend(repKey) {
  const container = document.getElementById('meddic-trend-container');
  if (!container) return;
  try {
    const items = await MEDDIC_CACHE.where({ key: repKey }).orderBy('created_at', 'asc').limit(12).find();
    if (items.length < 2) {
      container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Need at least 2 weekly scores to show trend. Run MEDDIC scoring each week.</div>';
      return;
    }

    const dims = ['Metrics', 'Economic Buyer', 'Decision Criteria', 'Decision Process', 'Identify Pain', 'Champion'];
    const dimLabels = { 'Economic Buyer': 'Econ.Buyer', 'Decision Criteria': 'D.Criteria', 'Decision Process': 'D.Process', 'Identify Pain': 'Id.Pain' };
    const parsed = items.map(i => {
      const data = JSON.parse(i.data);
      const dimMap = {};
      for (const d of (data.dimensions || [])) dimMap[d.name] = d.score;
      return { week: i.week || '?', overall: data.overall || 0, dims: dimMap };
    });

    const labels = parsed.map(p => formatWeekLabel(p.week));
    const overallData = parsed.map(p => p.overall);
    const latest = parsed[parsed.length - 1];
    const prev = parsed[parsed.length - 2];
    const canvasId = 'chartMeddicTrendRep';
    const latestColor = latest.overall >= 7 ? 'var(--accent)' : latest.overall >= 5 ? 'var(--warning)' : 'var(--danger)';

    container.innerHTML = `
      <div style="display:flex;align-items:baseline;gap:16px;margin-bottom:8px">
        <div style="font-size:24px;font-weight:700;color:${latestColor}">${typeof latest.overall === 'number' ? latest.overall.toFixed(1) : latest.overall}</div>
        <div style="font-size:13px">${renderDelta(latest.overall, prev.overall)} vs prior week</div>
        <div style="font-size:11px;color:var(--text-muted);margin-left:auto">${parsed.length} weeks</div>
      </div>
      <div class="chart-wrap-sm"><canvas id="${canvasId}"></canvas></div>
    `;

    const datasets = [
      repLineDataset('Overall', overallData, COLORS.primary, COLORS.primaryFill),
      ...dims.map(dim => dimLineDataset(
        dimLabels[dim] || dim,
        parsed.map(p => p.dims[dim] ?? null),
        MEDDIC_DIM_COLORS[dim]
      )),
    ];

    destroyChart(canvasId);
    createLineChart(canvasId, { labels, datasets, yMax: 10, legendDisplay: true });
  } catch (_) {
    container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Could not load trend data.</div>';
  }
}

async function runMEDDICForRep(repEmail) {
  try {
    const transcripts = await fetchTranscriptDetails(repEmail, 5, true);
    console.log(`[MEDDIC] ${repEmail}: ${transcripts?.length || 0} transcripts found`);
    if (!transcripts || transcripts.length === 0) return { skipped: true, reason: 'No transcribed calls in last 14 days' };
    const result = await scoreMEDDIC(transcripts, null, repEmail);
    if (!result) return { skipped: true, reason: 'AI scoring returned no result' };
    return result;
  } catch (err) {
    console.error(`[MEDDIC] Failed for ${repEmail}:`, err);
    return { skipped: true, reason: 'Scoring failed' };
  }
}

async function getRepMEDDICHistory(repKey) {
  try {
    const items = await MEDDIC_CACHE.where({ key: repKey }).orderBy('created_at', 'desc').limit(2).find();
    return {
      current: items[0] ? JSON.parse(items[0].data) : null,
      currentWeek: items[0]?.week || null,
      previous: items[1] ? JSON.parse(items[1].data) : null,
      previousWeek: items[1]?.week || null,
    };
  } catch (_) {
    return { current: null, currentWeek: null, previous: null, previousWeek: null };
  }
}


// ─── Team MEDDIC table ───

function scoreColor(score) {
  if (score >= 7) return 'var(--accent)';
  if (score >= 5) return 'var(--warning)';
  return 'var(--danger)';
}

function scoreCell(score) {
  if (score == null) return '<td class="text-right text-muted">—</td>';
  return `<td class="text-right" style="font-weight:600;color:${scoreColor(score)}">${score}</td>`;
}

async function loadTeamMEDDIC() {
  const container = document.getElementById('team-meddic-container');
  if (!container) return;

  const dims = ['Metrics', 'Economic Buyer', 'Decision Criteria', 'Decision Process', 'Identify Pain', 'Champion'];
  const dimShort = ['M', 'E', 'D', 'D', 'I', 'C'];

  const appSelectedTeam = window.__appData?.selectedTeam || null;
  const repData = [];
  for (const [email, rep] of Object.entries(REP_ROSTER)) {
    if (appSelectedTeam && rep.team !== appSelectedTeam) continue;
    const history = await getRepMEDDICHistory(email);
    // Fetch historical scores for sparkline
    let trendValues = [];
    try {
      const histItems = await MEDDIC_CACHE.where({ key: email }).orderBy('created_at', 'asc').limit(8).find();
      trendValues = histItems.map(i => { try { return JSON.parse(i.data).overall || 0; } catch (_) { return 0; } }).filter(v => v > 0);
    } catch (_) {}
    repData.push({ email, name: rep.name, trendValues, ...history });
  }

  const hasAnyScores = repData.some(r => r.current);

  const rows = repData.map(r => {
    const sparkline = r.trendValues.length >= 2 ? `<td class="text-right">${renderSparkline(r.trendValues, 60, 20)}</td>` : '<td class="text-right text-muted">—</td>';
    if (!r.current) {
      return `<tr>
        <td><strong>${r.name}</strong></td>
        <td class="text-right text-muted">—</td>
        <td class="text-right text-muted">—</td>
        ${sparkline}
        ${dims.map(() => '<td class="text-right text-muted">—</td>').join('')}
      </tr>`;
    }

    const sc = r.current;
    const dimMap = {};
    for (const d of (sc.dimensions || [])) dimMap[d.name] = d.score;

    let deltaHtml = '<td class="text-right text-muted">—</td>';
    if (r.previous && r.previous.overall != null && sc.overall != null) {
      const delta = sc.overall - r.previous.overall;
      if (delta !== 0) {
        const sign = delta > 0 ? '+' : '';
        const color = delta > 0 ? 'var(--accent)' : 'var(--danger)';
        deltaHtml = `<td class="text-right" style="font-weight:600;color:${color}">${sign}${delta.toFixed(1)}</td>`;
      } else {
        deltaHtml = '<td class="text-right text-muted">0</td>';
      }
    }

    return `<tr class="rep-row" onclick="document.getElementById('repSelector').value='${r.email}'; window.onRepChange('${r.email}')">
      <td><strong>${r.name}</strong></td>
      ${scoreCell(sc.overall)}
      ${deltaHtml}
      ${sparkline}
      ${dims.map(d => scoreCell(dimMap[d])).join('')}
    </tr>`;
  }).join('');

  container.innerHTML = hasAnyScores ? `
    <table class="data-table" style="table-layout:fixed">
      <colgroup>
        <col style="width:18%"><col style="width:9%"><col style="width:7%"><col style="width:8%">
        ${dimShort.map(() => '<col style="width:9.6%">').join('')}
      </colgroup>
      <thead><tr>
        <th>Rep</th><th class="text-right">Overall</th><th class="text-right">Δ</th><th class="text-right">Trend</th>
        ${dimShort.map(d => `<th class="text-right">${d}</th>`).join('')}
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="last-updated" style="margin-top:8px">Click "Refresh all scores" to re-analyze all reps. Δ shows change from previous scoring.</div>
  ` : `
    <div class="empty-state">No MEDDIC scores yet. Click "Refresh all scores" to analyze all reps.</div>
  `;
}

// ─── Team MEDDIC Trend (weekly team avg sparkline) ───

async function loadTeamMEDDICTrend() {
  const container = document.getElementById('team-meddic-trend-container');
  if (!container) return;
  try {
    const appSelectedTeam = window.__appData?.selectedTeam || null;
    const dims = ['Metrics', 'Economic Buyer', 'Decision Criteria', 'Decision Process', 'Identify Pain', 'Champion'];
    const dimLabels = { 'Economic Buyer': 'Econ.Buyer', 'Decision Criteria': 'D.Criteria', 'Decision Process': 'D.Process', 'Identify Pain': 'Id.Pain' };
    const weekScores = {};    // { week: [overallScores] }
    const weekDimScores = {}; // { week: { dimName: [scores] } }

    for (const [email, rep] of Object.entries(REP_ROSTER)) {
      if (appSelectedTeam && rep.team !== appSelectedTeam) continue;
      const items = await MEDDIC_CACHE.where({ key: email }).orderBy('created_at', 'asc').limit(8).find();
      for (const item of items) {
        const week = item.week || 'unknown';
        try {
          const sc = JSON.parse(item.data);
          if (sc.overall) {
            if (!weekScores[week]) weekScores[week] = [];
            weekScores[week].push(sc.overall);
          }
          if (sc.dimensions) {
            if (!weekDimScores[week]) weekDimScores[week] = {};
            for (const d of sc.dimensions) {
              if (!weekDimScores[week][d.name]) weekDimScores[week][d.name] = [];
              weekDimScores[week][d.name].push(d.score);
            }
          }
        } catch (_) {}
      }
    }

    const weeks = Object.keys(weekScores).sort();
    if (weeks.length < 2) {
      container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Not enough data for trend yet. Scores from at least 2 weeks needed.</div>';
      return;
    }

    const weeklyAvgs = weeks.map(w => {
      const scores = weekScores[w];
      return scores.reduce((a, b) => a + b, 0) / scores.length;
    });
    const latest = weeklyAvgs[weeklyAvgs.length - 1];
    const prev = weeklyAvgs[weeklyAvgs.length - 2];
    const labels = weeks.map(formatWeekLabel);
    const canvasId = 'chartMeddicTrendTeam';
    const latestColor = latest >= 7 ? 'var(--accent)' : latest >= 5 ? 'var(--warning)' : 'var(--danger)';

    container.innerHTML = `
      <div style="display:flex;align-items:baseline;gap:16px;margin-bottom:8px">
        <div style="font-size:28px;font-weight:700;color:${latestColor}">${latest.toFixed(1)}</div>
        <div style="font-size:13px">${renderDelta(latest, prev)} vs prior week</div>
        <div style="font-size:11px;color:var(--text-muted);margin-left:auto">${weeks.length} weeks · Team avg</div>
      </div>
      <div class="chart-wrap-sm"><canvas id="${canvasId}"></canvas></div>
    `;

    const dimDatasets = dims.map(dim => {
      const data = weeks.map(w => {
        const arr = weekDimScores[w]?.[dim];
        if (!arr || arr.length === 0) return null;
        return arr.reduce((a, b) => a + b, 0) / arr.length;
      });
      return dimLineDataset(dimLabels[dim] || dim, data, MEDDIC_DIM_COLORS[dim]);
    });

    const datasets = [
      repLineDataset('Team Avg', weeklyAvgs, COLORS.primary, COLORS.primaryFill),
      ...dimDatasets,
    ];

    destroyChart(canvasId);
    createLineChart(canvasId, { labels, datasets, yMax: 10, legendDisplay: true });
  } catch (err) {
    container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Could not load trend.</div>';
  }
}

// ─── Team MEDDIC Distribution (rep scores vs team avg compare-rows) ───

async function loadTeamMEDDICDistribution() {
  const container = document.getElementById('team-meddic-distribution');
  if (!container) return;
  try {
    const appSelectedTeam = window.__appData?.selectedTeam || null;
    const repScores = [];
    for (const [email, rep] of Object.entries(REP_ROSTER)) {
      if (appSelectedTeam && rep.team !== appSelectedTeam) continue;
      const items = await MEDDIC_CACHE.where({ key: email }).orderBy('created_at', 'desc').limit(1).find();
      let score = null;
      if (items.length) { try { score = JSON.parse(items[0].data).overall || null; } catch (_) {} }
      repScores.push({ name: rep.name, email, score });
    }
    const scored = repScores.filter(r => r.score != null);
    if (scored.length === 0) {
      container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">No MEDDIC scores yet.</div>';
      return;
    }
    const teamAvg = scored.reduce((s, r) => s + r.score, 0) / scored.length;
    // Sort worst-first to highlight coaching needs
    scored.sort((a, b) => a.score - b.score);
    const rows = scored.map(r => {
      const isAbove = r.score >= teamAvg;
      const color = isAbove ? 'var(--success)' : 'var(--danger)';
      const pctFill = teamAvg > 0 ? Math.min((r.score / teamAvg) * 100, 150) : 0;
      return `<div class="compare-row">
        <div class="compare-label">${r.name.split(' ')[0]}</div>
        <div class="compare-you" style="color:${color}">${r.score.toFixed(1)}</div>
        <div class="compare-team">${teamAvg.toFixed(1)}</div>
        <div class="compare-bar-wrap"><div class="compare-bar-fill" style="width:${pctFill}%;background:${color}"></div></div>
      </div>`;
    }).join('');
    container.innerHTML = `
      <div class="compare-header">
        <div class="compare-header-spacer"></div>
        <div class="compare-header-label">Rep</div>
        <div class="compare-header-label">Team Avg</div>
        <div class="compare-header-bar"></div>
      </div>
      ${rows}
    `;
  } catch (err) {
    container.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Could not load distribution.</div>';
  }
}

window.refreshAllMEDDIC = async function() {
  const container = document.getElementById('team-meddic-container');
  if (!container) return;

  const reps = Object.entries(REP_ROSTER);
  for (let i = 0; i < reps.length; i++) {
    const [email, rep] = reps[i];
    container.innerHTML = `<div class="ai-loading">Scoring ${rep.name} (${i + 1}/${reps.length})...</div>`;
    const result = await runMEDDICForRep(email);
    if (result?.skipped) {
      container.innerHTML = `<div class="ai-loading">Skipped ${rep.name} — ${result.reason} (${i + 1}/${reps.length})...</div>`;
      await new Promise(r => setTimeout(r, 600));
    }
    // Brief pause between AI calls to avoid rate-limiting
    if (i < reps.length - 1) await new Promise(r => setTimeout(r, 1500));
  }

  container.innerHTML = '<div class="ai-loading">Loading results...</div>';
  await loadTeamMEDDIC();
};
