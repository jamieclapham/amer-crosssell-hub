// Forecast Hub — Won / SF Commit / Upside / Pipeline + WoW movement + rep forecast table
import { REP_ROSTER, STAGE_ORDER, QUARTER, COACHES, COACH_QUOTAS, getQuotaFallback, BENCHMARKS } from '../config.js';
import { formatUSD, formatUSDFull, formatPct, formatDate, daysLeft, toDateStr, getISOWeek, renderDelta, sfLink } from '../ui.js';
import { saveForecastSnapshot, getForecastSnapshots, cleanupOldSnapshots } from '../queries.js';
import { createBarChart, repBarDataset, teamAvgBarDataset, currencyTick, computeTeamAvgByStage, COLORS } from '../chart-utils.js';
import { streamAI } from '../ai-stream.js';

const FORECAST_RISK_CACHE = quick.db.collection('coaching_hub_forecast_risk');
const RISK_CACHE_TTL = 12 * 60 * 60 * 1000; // 12 hours

export async function renderForecast(data, user, targetEl = null) {
  const el = targetEl || document.getElementById('tab-forecast');
  if (!el) return;

  // Auto-save a snapshot for this week (idempotent)
  const week = getISOWeek();
  const fc = data.viewMode === 'team'
    ? buildTeamForecast(data)
    : categorizePipeline(data.pipeline, data.wonPBR);

  const snapshotPayload = {
    won: fc.won,
    commit: fc.commit,
    upside: fc.upside,
    upsidePBR: fc.upsidePBR,
    deals: (data.pipeline || []).filter(d => !d.is_closed).map(d => ({
      opp_id: d.opportunity_id,
      name: d.opp_name,
      pbr: Number(d.pbr) || 0,
      category: d.forecast_category,
      close_date: toDateStr(d.close_date),
      owner: d.salesforce_owner_name,
    })),
  };
  saveForecastSnapshot({ ...data, snapshotPayload }, week);
  cleanupOldSnapshots(13);

  if (data.viewMode === 'team') {
    await renderTeamForecast(el, data, fc);
  } else {
    await renderRepForecast(el, data, fc);
  }
}

export function categorizePipeline(pipeline, wonPBR) {
  const won = Number(wonPBR) || 0;
  const openDeals = (pipeline || []).filter(d => !d.is_closed);

  // Upside = deals with merchant_intent = 'Committed - At Risk'
  let upsidePBR = 0;
  const upsideDeals = [];
  for (const d of openDeals) {
    if (d.merchant_intent === 'Committed - At Risk') {
      upsidePBR += Number(d.pbr) || 0;
      upsideDeals.push(d);
    }
  }

  const totalPipeline = openDeals.reduce((s, d) => s + (Number(d.pbr) || 0), 0);

  return {
    won, upsidePBR, totalPipeline,
    commit: won, // placeholder — actual commit comes from BQ forecastCommit
    upside: won + upsidePBR,
    upsideDeals,
  };
}

function buildTeamForecast(data) {
  const wonMap = {};
  for (const row of (data.wonPBR || [])) wonMap[row.salesforce_owner_name] = Number(row.won_pbr) || 0;
  const teamWon = Object.values(wonMap).reduce((a, b) => a + b, 0);
  return categorizePipeline(data.pipeline, teamWon);
}

// ─── Relative time formatting ───

function relativeTime(dateStr) {
  if (!dateStr) return null;
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return null;
  const now = new Date();
  const diffMs = now - d;
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays === 1) return 'yesterday';
  if (diffDays < 7) return `${diffDays}d ago`;
  return `${Math.floor(diffDays / 7)}w ago`;
}

// ─── Rep Forecast View ───

async function renderRepForecast(el, data, fc) {
  const days = daysLeft();

  const quotaRows = data.quotas || [];
  const sfName = data.sfName || '';
  const repQuotaRow = quotaRows.find(q => q.name === sfName && q.role_type === 'rep');
  const quota = repQuotaRow ? Number(repQuotaRow.quarterly_quota) : getQuotaFallback();

  // SF Commit from BQ
  const sfCommit = Number(data.forecastCommit) || 0;
  const gap = Math.max(0, quota - fc.won);
  const openPipeline = fc.totalPipeline;

  const maxVal = Math.max(sfCommit, fc.upside, quota);
  const wonPct = maxVal > 0 ? (fc.won / maxVal) * 100 : 0;
  const commitGapPct = maxVal > 0 ? (Math.max(0, sfCommit - fc.won) / maxVal) * 100 : 0;
  const upsidePct = maxVal > 0 ? (fc.upsidePBR / maxVal) * 100 : 0;

  // Get last week's snapshot for WoW comparison
  const entity = data.sfName || 'unknown';
  const snapshots = await getForecastSnapshots(entity, 2);
  const lastWeek = snapshots.length >= 2 ? snapshots[1] : null;

  const wowHtml = lastWeek ? renderWoWMovement(fc, lastWeek, sfCommit) : '<div class="empty-state" style="padding:12px;font-size:12px">Forecast movement will appear next week once we have comparison data.</div>';

  el.innerHTML = `
    <div class="hero-banner">
      <div class="hero-title">${data.repName || 'Your'} Forecast · ${QUARTER.label}</div>
      <div class="hero-name">${sfCommit > 0 ? formatUSD(sfCommit) : '—'} SF commit · ${sfCommit > 0 ? formatPct((sfCommit / quota) * 100) : '—'} of quota</div>
      <div class="forecast-bar-container" style="margin-bottom:16px">
        <div class="forecast-bar">
          ${wonPct > 0 ? `<div class="forecast-segment forecast-won" style="width:${wonPct}%">${formatUSD(fc.won)}</div>` : ''}
          ${commitGapPct > 0 ? `<div class="forecast-segment forecast-commit" style="width:${commitGapPct}%">${formatUSD(Math.max(0, sfCommit - fc.won))}</div>` : ''}
          ${upsidePct > 0 ? `<div class="forecast-segment forecast-upside" style="width:${upsidePct}%">${formatUSD(fc.upsidePBR)}</div>` : ''}
        </div>
        <div class="forecast-legend">
          <span class="legend-won">Won: ${formatUSD(fc.won)}</span>
          <span class="legend-commit">SF Commit: ${sfCommit > 0 ? formatUSD(sfCommit) : '—'}</span>
          <span class="legend-upside">Upside: ${formatUSD(fc.upside)}</span>
        </div>
      </div>
      <div class="metric-grid metric-grid-compact">
        <div class="metric-card">
          <div class="metric-label">Won</div>
          <div class="metric-value">${formatUSD(fc.won)}</div>
          <div class="metric-sub">${formatPct((fc.won / quota) * 100)} of quota</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">SF Commit</div>
          <div class="metric-value">${sfCommit > 0 ? formatUSD(sfCommit) : '—'}</div>
          <div class="metric-sub">${sfCommit > 0 ? `${formatPct((sfCommit / quota) * 100)} of quota` : 'No forecast submitted'}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Upside</div>
          <div class="metric-value">${formatUSD(fc.upsidePBR)}</div>
          <div class="metric-sub">${fc.upsideDeals.length} deals at risk</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Pipeline</div>
          <div class="metric-value">${formatUSD(openPipeline)}</div>
          <div class="metric-sub">${days} days left</div>
        </div>
      </div>
    </div>

    <!-- WoW Movement -->
    <div class="card">
      <div class="card-title">Week-over-Week Movement</div>
      ${wowHtml}
    </div>

    <!-- Pipeline by Stage -->
    ${renderPipelineByStage(data.pipeline)}

    ${renderDealSection('Committed - At Risk Deals', fc.upsideDeals)}
  `;

  initPipelineStageChart(data.pipeline, data.teamPipeline || []);
}

// ─── WoW Movement ───

function renderWoWMovement(current, lastWeek, sfCommit) {
  const metrics = [
    { label: 'Won', cur: current.won || 0, prev: lastWeek.won || 0 },
    { label: 'SF Commit', cur: sfCommit || current.commit || 0, prev: lastWeek.commit || 0 },
  ];

  const cells = metrics.map(m => {
    const delta = m.cur - m.prev;
    const sign = delta > 0 ? '+' : '';
    const color = delta > 0 ? 'var(--accent)' : delta < 0 ? 'var(--danger)' : 'var(--text-muted)';
    return `<div class="metric-card" style="background:var(--card-bg);box-shadow:var(--card-shadow)">
      <div class="metric-label">${m.label}</div>
      <div class="metric-value">${formatUSD(m.cur)}</div>
      <div class="metric-sub" style="color:${color};font-weight:600">${sign}${formatUSD(delta)}</div>
    </div>`;
  }).join('');

  return `<div style="display:grid;grid-template-columns:repeat(2,1fr);gap:14px">${cells}</div>
    <div class="last-updated" style="margin-top:6px">Compared to last week (${lastWeek.week || 'prev'})</div>`;
}

// ─── Manager Team Forecast ───

async function renderTeamForecast(el, data, fc) {
  const wonMap = {};
  for (const row of (data.wonPBR || [])) wonMap[row.salesforce_owner_name] = Number(row.won_pbr) || 0;

  // Build per-rep quota map from BQ data
  const quotaRows = data.quotas || [];
  const repQuotaMap = {};
  for (const q of quotaRows) {
    if (q.role_type === 'rep') repQuotaMap[q.name] = Number(q.quarterly_quota) || 0;
  }

  // Pipeline maps
  const pipelineMap = {};
  const pipelineExclPreQualMap = {};
  const upsideMap = {}; // merchant_intent = 'Committed - At Risk'
  for (const d of (data.pipeline || [])) {
    if (d.is_closed) continue;
    const pbr = Number(d.pbr) || 0;
    const owner = d.salesforce_owner_name;
    pipelineMap[owner] = (pipelineMap[owner] || 0) + pbr;
    if (d.current_stage_name !== 'Pre-Qualified') {
      pipelineExclPreQualMap[owner] = (pipelineExclPreQualMap[owner] || 0) + pbr;
    }
    if (d.merchant_intent === 'Committed - At Risk') {
      upsideMap[owner] = (upsideMap[owner] || 0) + pbr;
    }
  }

  // Roster filtered by selected coach team
  const rosterEntries = Object.entries(REP_ROSTER)
    .filter(([, rep]) => !data.selectedTeam || rep.team === data.selectedTeam);

  // Per-rep forecast details (commit + last submitted)
  const repForecastDetails = data.repForecastDetails || [];
  const repForecastMap = {};
  for (const r of repForecastDetails) {
    repForecastMap[r.owner_name] = {
      commit: Number(r.commit_value) || 0,
      lastSubmitted: r.last_submitted || null,
      repSubmitted: r.rep_submitted === true || r.rep_submitted === 'true',
    };
  }

  // Team quota = coach quota (NOT rollup of rep quotas — set independently by Finance)
  const coachRows = quotaRows.filter(r => r.role_type === 'coach');
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
  const teamQuota = bqCoachQuota > 0 ? bqCoachQuota : hardcodedFallback;
  const teamWon = fc.won;
  const teamPipeline = fc.totalPipeline;
  const teamUpside = fc.upsidePBR;
  // Hero uses coach LEAD forecast (authoritative team-level commit, not rollup of per-rep values)
  const forecastCommitRows = data.forecastCommit || [];
  let teamSfCommit = 0;
  for (const r of forecastCommitRows) teamSfCommit += Number(r.total_commit) || 0;
  const days = daysLeft();

  // Forecast bar
  const maxVal = Math.max(teamSfCommit, fc.upside, teamQuota);
  const wonPct = maxVal > 0 ? (teamWon / maxVal) * 100 : 0;
  const commitGapPct = maxVal > 0 ? (Math.max(0, teamSfCommit - teamWon) / maxVal) * 100 : 0;
  const upsidePct = maxVal > 0 ? (teamUpside / maxVal) * 100 : 0;

  // WoW movement
  const snapshots = await getForecastSnapshots('team', 2);
  const lastWeek = snapshots.length >= 2 ? snapshots[1] : null;
  const wowHtml = lastWeek
    ? renderWoWMovement(fc, lastWeek, teamSfCommit)
    : '<div class="empty-state" style="padding:12px;font-size:12px">Forecast movement will appear next week once we have comparison data.</div>';

  const repRows = rosterEntries.map(([email, rep]) => {
    const won = wonMap[rep.sfName] || 0;
    const repQuota = repQuotaMap[rep.sfName] || getQuotaFallback();
    const gap = Math.max(0, repQuota - won);
    const pipeline = pipelineMap[rep.sfName] || 0;
    const pipelineExclPQ = pipelineExclPreQualMap[rep.sfName] || 0;
    const coverage = gap > 0 ? pipelineExclPQ / gap : 999;
    const upside = upsideMap[rep.sfName] || 0;

    const fd = repForecastMap[rep.sfName] || {};
    const sfCommit = fd.commit || 0;
    const lastSubmitted = fd.lastSubmitted;
    const relTime = relativeTime(lastSubmitted);

    // Submission flags only — forecast risk comes from AI
    const repSubmitted = fd.repSubmitted || false;
    const flags = [];
    if (sfCommit <= 0) {
      flags.push({ label: 'Not submitted', level: 'high' });
    } else {
      if (!repSubmitted) flags.push({ label: 'Coach submitted', level: 'medium' });
      if (lastSubmitted) {
        const daysSince = Math.floor((new Date() - new Date(lastSubmitted)) / 86400000);
        if (daysSince > 7) flags.push({ label: `${daysSince}d stale`, level: 'high' });
      }
    }

    // Per-rep deal breakdown for AI prompt
    const repDeals = (data.pipeline || []).filter(d => !d.is_closed && d.salesforce_owner_name === rep.sfName);
    const stageBreakdown = {};
    for (const d of repDeals) {
      const stage = d.current_stage_name || 'Unknown';
      stageBreakdown[stage] = (stageBreakdown[stage] || 0) + (Number(d.pbr) || 0);
    }
    const merchantIntentBreakdown = {};
    for (const d of repDeals) {
      const mi = d.merchant_intent || 'null';
      merchantIntentBreakdown[mi] = (merchantIntentBreakdown[mi] || 0) + (Number(d.pbr) || 0);
    }

    return { email, name: rep.name, sfName: rep.sfName, won, sfCommit, gap, pipeline, pipelineExclPQ, coverage, upside, lastSubmitted, relTime, flags, stageBreakdown, merchantIntentBreakdown, repQuota };
  }).sort((a, b) => {
    // Sort by gap desc (biggest gaps first)
    return b.gap - a.gap;
  });

  const repTableRows = repRows.map(r => {
    const flagBadges = r.flags.length > 0
      ? r.flags.map(f => `<span class="risk-flag risk-${f.level}">${f.label}</span>`).join(' ')
      : '';

    const submittedHtml = r.relTime
      ? `<span style="font-size:12px">${r.relTime}</span>`
      : r.sfCommit > 0
        ? `<span style="font-size:12px;color:var(--text-muted)">—</span>`
        : `<span class="risk-flag risk-high">Not submitted</span>`;

    return `<tr class="rep-row" onclick="document.getElementById('repSelector').value='${r.email}'; window.onRepChange('${r.email}')">
      <td><strong>${r.name}</strong></td>
      <td class="text-right text-mono">${formatUSD(r.won)}</td>
      <td class="text-right text-mono">${r.sfCommit > 0 ? formatUSD(r.sfCommit) : '<span class="text-muted">—</span>'}</td>
      <td class="text-right text-mono">${formatUSD(r.upside)}</td>
      <td class="text-right text-mono">${formatUSD(r.pipeline)}</td>
      <td class="text-right">${r.coverage < 999 ? r.coverage.toFixed(1) + 'x' : '—'}</td>
      <td class="text-right">${submittedHtml} ${flagBadges}</td>
      <td class="ai-risk-cell" id="ai-risk-${r.sfName.replace(/[^a-zA-Z0-9]/g, '_')}"><span class="text-muted" style="font-size:11px">Analyzing...</span></td>
    </tr>`;
  }).join('');

  el.innerHTML = `
    <div class="hero-banner">
      <div class="hero-title">${data.selectedTeam ? (COACHES[data.selectedTeam]?.name || data.selectedTeam) : 'Team'} Forecast · ${QUARTER.label}</div>
      <div class="hero-name">${teamSfCommit > 0 ? formatUSD(teamSfCommit) : '—'} SF commit · ${teamSfCommit > 0 ? formatPct((teamSfCommit / teamQuota) * 100) : '—'} of quota</div>
      <div class="forecast-bar-container" style="margin-bottom:16px">
        <div class="forecast-bar">
          ${wonPct > 0 ? `<div class="forecast-segment forecast-won" style="width:${wonPct}%">${formatUSD(teamWon)}</div>` : ''}
          ${commitGapPct > 0 ? `<div class="forecast-segment forecast-commit" style="width:${commitGapPct}%">${formatUSD(Math.max(0, teamSfCommit - teamWon))}</div>` : ''}
          ${upsidePct > 0 ? `<div class="forecast-segment forecast-upside" style="width:${upsidePct}%">${formatUSD(teamUpside)}</div>` : ''}
        </div>
        <div class="forecast-legend">
          <span class="legend-won">Won: ${formatUSD(teamWon)}</span>
          <span class="legend-commit">SF Commit: ${teamSfCommit > 0 ? formatUSD(teamSfCommit) : '—'}</span>
          <span class="legend-upside">Upside: ${formatUSD(teamUpside)}</span>
        </div>
      </div>
      <div class="metric-grid metric-grid-compact">
        <div class="metric-card">
          <div class="metric-label">Won</div>
          <div class="metric-value">${formatUSD(teamWon)}</div>
          <div class="metric-sub">${formatPct((teamWon / teamQuota) * 100)} of quota</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">SF Commit</div>
          <div class="metric-value">${teamSfCommit > 0 ? formatUSD(teamSfCommit) : '—'}</div>
          <div class="metric-sub">${teamSfCommit > 0 ? `${formatPct((teamSfCommit / teamQuota) * 100)} of quota` : 'No forecast'}</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Upside</div>
          <div class="metric-value">${formatUSD(teamUpside)}</div>
          <div class="metric-sub">${fc.upsideDeals.length} deals at risk</div>
        </div>
        <div class="metric-card">
          <div class="metric-label">Pipeline</div>
          <div class="metric-value">${formatUSD(teamPipeline)}</div>
          <div class="metric-sub">${days} days left</div>
        </div>
      </div>
    </div>

    <!-- WoW Movement -->
    <div class="card">
      <div class="card-title">Week-over-Week Movement</div>
      ${wowHtml}
    </div>

    <!-- Forecast by Reps -->
    <div class="card">
      <div class="card-title">Forecast by Rep</div>
      <table class="data-table team-table" style="table-layout:fixed">
        <colgroup>
          <col style="width:13%"><col style="width:10%"><col style="width:10%"><col style="width:9%">
          <col style="width:10%"><col style="width:8%"><col style="width:14%"><col style="width:26%">
        </colgroup>
        <thead><tr>
          <th>Rep</th>
          <th class="text-right">Won</th>
          <th class="text-right">SF Commit</th>
          <th class="text-right">Upside</th>
          <th class="text-right">Pipeline</th>
          <th class="text-right">Coverage</th>
          <th class="text-right">Submitted</th>
          <th>Forecast Insight</th>
        </tr></thead>
        <tbody>${repTableRows}</tbody>
      </table>
      <div class="last-updated" style="margin-top:8px">Click a rep to drill in. Coverage = pipeline excl. Pre-Qualified / gap. Forecast insight is AI-generated (cached 12h).</div>
    </div>

    <!-- Pipeline by Stage (Team) -->
    ${renderPipelineByStage(data.pipeline)}

    ${renderTeamDealSection('Committed - At Risk Deals', fc.upsideDeals)}
  `;

  initPipelineStageChart(data.pipeline, data.pipeline);

  // Fire AI forecast risk analysis async (fills in after table renders)
  generateForecastRisk(repRows, teamQuota, days);
}

// ─── AI Forecast Risk ───

async function generateForecastRisk(repRows, teamQuota, daysLeft) {
  const cacheKey = `team:${QUARTER.label}`;

  // Check cache
  try {
    const cached = await FORECAST_RISK_CACHE.where({ key: cacheKey }).orderBy('created_at', 'desc').limit(1).find();
    if (cached.length) {
      const age = Date.now() - new Date(cached[0].created_at).getTime();
      if (age < RISK_CACHE_TTL) {
        const riskMap = JSON.parse(cached[0].data);
        applyForecastRisk(riskMap);
        return;
      }
    }
  } catch (_) {}

  // Build prompt
  const repSummaries = repRows.map(r => {
    const stageStr = Object.entries(r.stageBreakdown).map(([s, v]) => `${s}: ${formatUSD(v)}`).join(', ');
    const miStr = Object.entries(r.merchantIntentBreakdown).map(([m, v]) => `${m}: ${formatUSD(v)}`).join(', ');
    return `${r.name}: Won ${formatUSD(r.won)}, SF Commit ${formatUSD(r.sfCommit)}, Gap ${formatUSD(r.gap)}, Quota ${formatUSD(r.repQuota)}, Pipeline ${formatUSD(r.pipeline)} (excl Pre-Qual: ${formatUSD(r.pipelineExclPQ)}), Coverage ${r.coverage < 999 ? r.coverage.toFixed(1) + 'x' : 'N/A'}, Upside (At-Risk) ${formatUSD(r.upside)}\n    Stages: ${stageStr || 'no deals'}\n    Merchant Intent: ${miStr || 'no deals'}`;
  }).join('\n\n');

  const totalQuarterDays = Math.max(1, (new Date(QUARTER.end + 'T00:00:00') - new Date(QUARTER.start + 'T00:00:00')) / 86400000);
  const elapsedDays = Math.max(1, totalQuarterDays - daysLeft);
  const pctElapsed = Math.round((elapsedDays / totalQuarterDays) * 100);

  const prompt = `You are an expert AMER SMB Cross-Sell sales forecasting analyst at Shopify. Analyze each rep's forecast and classify their commit as one of:

- "At risk" — commit unlikely to be met given pipeline reality (not enough closable deals, poor stage mix, gap too large relative to time remaining)
- "Conservative" — commit appears sandbagged; pipeline supports a higher number (strong stage mix, high coverage, merchant intent signals)
- "On track" — commit is realistic and well-supported by pipeline within ~5% accuracy

CRITICAL CONTEXT — QUARTER TIMING:
We are ${pctElapsed}% through the quarter (${elapsedDays} of ${totalQuarterDays} days elapsed, ${daysLeft} remaining).
${pctElapsed < 33 ? 'EARLY QUARTER: Low won revenue is NORMAL at this stage. Do NOT flag reps as "At risk" just because they have a large gap — focus on whether their pipeline and stage mix can realistically support their commit over the remaining weeks. A large gap early in the quarter with strong pipeline is expected.' : pctElapsed < 66 ? 'MID QUARTER: Reps should be building momentum. Some gap is normal but pipeline should be converting. Focus on whether stage progression supports the commit timeline.' : 'LATE QUARTER: Most of the commit should be won or in final stages (Negotiation/Closing). Large gaps with early-stage pipeline are genuine risks.'}

For each rep, return a verdict and ONE concise sentence (max 15 words) explaining why — name specific gaps, stages, or deal patterns. Be direct and actionable. Think like a sales director reviewing forecast.

QUARTER: ${QUARTER.label} | ${daysLeft} days remaining (${pctElapsed}% elapsed)
TEAM QUOTA: ${formatUSD(teamQuota)}

REP DATA:
${repSummaries}

Return ONLY a JSON object mapping rep name to { "verdict": "At risk|Conservative|On track", "reason": "one sentence" }. Example:
{"James Sheehan": {"verdict": "At risk", "reason": "$400K gap with only 0.8x coverage in closable stages."}}`;

  try {
    const response = await streamAI(prompt, { maxTokens: 1500 });
    const jsonMatch = response.match(/\{[\s\S]*\}/);
    if (!jsonMatch) throw new Error('No JSON in response');
    const riskMap = JSON.parse(jsonMatch[0]);

    applyForecastRisk(riskMap);

    // Cache
    try {
      await FORECAST_RISK_CACHE.create({ key: cacheKey, data: JSON.stringify(riskMap), created_at: new Date().toISOString() });
    } catch (_) {}
  } catch (err) {
    console.error('[Forecast] AI risk analysis failed:', err);
    // Show fallback
    for (const r of repRows) {
      const cell = document.getElementById(`ai-risk-${r.sfName.replace(/[^a-zA-Z0-9]/g, '_')}`);
      if (cell) cell.innerHTML = '<span class="text-muted" style="font-size:11px">—</span>';
    }
  }
}

function applyForecastRisk(riskMap) {
  const verdictColors = {
    'At risk': 'var(--danger)',
    'Conservative': 'var(--warning, #e6a117)',
    'On track': 'var(--success)',
  };

  for (const [name, risk] of Object.entries(riskMap)) {
    const cellId = `ai-risk-${name.replace(/[^a-zA-Z0-9]/g, '_')}`;
    const cell = document.getElementById(cellId);
    if (!cell) continue;
    const color = verdictColors[risk.verdict] || 'var(--text-muted)';
    cell.innerHTML = `<span class="risk-flag" style="background:${color};color:#fff;font-size:10px">${risk.verdict}</span> <span style="font-size:11px;color:var(--text-secondary)">${risk.reason}</span>`;
  }
}

// ─── Deal Section ───

function renderDealSection(title, deals) {
  if (!deals || deals.length === 0) return '';
  const rows = deals.sort((a, b) => (Number(b.pbr) || 0) - (Number(a.pbr) || 0)).map(d => `
    <tr>
      <td>${sfLink(d.opp_name, d.opportunity_id)}</td>
      <td>${d.current_stage_name || '—'}</td>
      <td class="text-right text-mono">${formatUSD(Number(d.pbr) || 0)}</td>
      <td>${formatDate(d.close_date)}</td>
      <td class="text-truncate">${d.next_step || '<span class="text-muted">—</span>'}</td>
    </tr>
  `).join('');

  return `<div class="card">
    <div class="card-title">${title} (${deals.length})</div>
    <table class="data-table" style="table-layout:fixed">
      <colgroup><col style="width:28%"><col style="width:18%"><col style="width:16%"><col style="width:14%"><col style="width:24%"></colgroup>
      <thead><tr><th>Deal</th><th>Stage</th><th class="text-right">PBR</th><th>Close</th><th>Next Step</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

// ─── Team Deal Section (with Rep column, top 15) ───

function renderTeamDealSection(title, deals) {
  if (!deals || deals.length === 0) return '';
  const sorted = [...deals].sort((a, b) => (Number(b.pbr) || 0) - (Number(a.pbr) || 0)).slice(0, 15);
  const rows = sorted.map(d => `
    <tr>
      <td>${d.salesforce_owner_name || '—'}</td>
      <td>${sfLink(d.opp_name, d.opportunity_id)}</td>
      <td>${d.current_stage_name || '—'}</td>
      <td class="text-right text-mono">${formatUSD(Number(d.pbr) || 0)}</td>
      <td>${formatDate(d.close_date)}</td>
    </tr>
  `).join('');

  return `<div class="card">
    <div class="card-title">${title} (${deals.length})</div>
    <table class="data-table" style="table-layout:fixed">
      <colgroup><col style="width:18%"><col style="width:28%"><col style="width:18%"><col style="width:16%"><col style="width:14%"></colgroup>
      <thead><tr><th>Rep</th><th>Deal</th><th>Stage</th><th class="text-right">PBR</th><th>Close</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`;
}

// ─── Pipeline by Stage ───

function renderPipelineByStage(pipeline) {
  const openDeals = (pipeline || []).filter(d => !d.is_closed);
  if (openDeals.length === 0) return '';

  const totalPBR = openDeals.reduce((s, d) => s + (Number(d.pbr) || 0), 0);

  return `<div class="card">
    <div class="card-title">Pipeline by Stage</div>
    <div class="chart-wrap-lg"><canvas id="chartPipelineStage"></canvas></div>
    <div class="last-updated" style="margin-top:8px">${openDeals.length} open deals · ${formatUSD(totalPBR)} total pipeline</div>
  </div>`;
}

function initPipelineStageChart(pipeline, teamPipeline) {
  const openDeals = (pipeline || []).filter(d => !d.is_closed);
  if (openDeals.length === 0) return;

  const stageMap = {};
  for (const d of openDeals) {
    const stage = d.current_stage_name || 'Unknown';
    stageMap[stage] = (stageMap[stage] || 0) + (Number(d.pbr) || 0);
  }

  const repCount = Object.keys(REP_ROSTER).length;
  const teamAvgStages = computeTeamAvgByStage(teamPipeline || [], repCount);

  const stageNames = [...new Set([...STAGE_ORDER, ...Object.keys(stageMap)])].filter(s => stageMap[s] || teamAvgStages[s]);

  createBarChart('chartPipelineStage', {
    labels: stageNames,
    datasets: [
      repBarDataset('Pipeline', stageNames.map(s => stageMap[s] || 0), COLORS.primaryBar),
      teamAvgBarDataset(stageNames.map(s => teamAvgStages[s] || 0)),
    ],
    yTickCallback: currencyTick,
  });
}