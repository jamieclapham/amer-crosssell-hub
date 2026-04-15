// AI Weekly Priorities Engine
// Analyzes rep data and generates actionable priorities using Claude.
// Uses shared SSE streaming via ai-stream.js.

import { QUARTER, BENCHMARKS, REP_ROSTER, getQuotaFallback } from './config.js';
import { formatUSD, daysLeft, computeDealRisks, toDateStr, computeHygieneScore, computeHygieneFlags } from './ui.js';
import { streamAI } from './ai-stream.js';

const PRIORITIES_CACHE = quick.db.collection('hub_priorities');
const CACHE_TTL_MS = 12 * 60 * 60 * 1000; // 12 hours

export async function generatePriorities(data, container) {
  // Check cache first
  const cacheKey = data.viewMode === 'team' ? 'team' : (data.sfName || 'unknown');
  try {
    const cached = await PRIORITIES_CACHE.where({ key: cacheKey }).orderBy('created_at', 'desc').limit(1).find();
    if (cached.length) {
      const age = Date.now() - new Date(cached[0].created_at).getTime();
      if (age < CACHE_TTL_MS) {
        renderPriorities(container, JSON.parse(cached[0].data));
        return;
      }
    }
  } catch (_) { /* cache miss */ }

  // Build prompt based on view mode
  const prompt = data.viewMode === 'team'
    ? buildTeamPrompt(data)
    : buildRepPrompt(data);

  // Stream AI response
  const response = await streamPriorities(prompt);

  // Parse and render
  try {
    const jsonMatch = response.match(/\[[\s\S]*\]/);
    if (!jsonMatch) throw new Error('No JSON array in response');
    const priorities = JSON.parse(jsonMatch[0]);
    renderPriorities(container, priorities);

    // Cache result
    try {
      await PRIORITIES_CACHE.create({ key: cacheKey, data: JSON.stringify(priorities), created_at: new Date().toISOString() });
    } catch (_) {}
  } catch {
    // Fallback: render raw text
    container.innerHTML = `<div style="font-size:13px;white-space:pre-wrap;line-height:1.6">${response}</div>`;
  }
}

// --- Build prompts ---

function buildRepPrompt(data) {
  const wonPBR = Number(data.wonPBR) || 0;
  // Resolve rep quota from BQ (fallback to hardcoded)
  const quotaRows = data.quotas || [];
  const sfName = data.sfName || '';
  const repQuotaRow = quotaRows.find(q => q.name === sfName && q.role_type === 'rep');
  const quota = repQuotaRow ? Number(repQuotaRow.quarterly_quota) : getQuotaFallback();
  const gap = Math.max(0, quota - wonPBR);
  const attPct = quota > 0 ? ((wonPBR / quota) * 100).toFixed(1) : 0;
  const days = daysLeft();
  const calls = data.callActivity || {};

  // Format pipeline data
  const openDeals = (data.pipeline || []).filter(d => !d.is_closed);
  const forecastDeals = openDeals.filter(d => d.forecast_category === 'Forecast');
  const pipelineDeals = openDeals.filter(d => d.forecast_category === 'Pipeline');

  const formatDeal = d => {
    const risks = computeDealRisks(d);
    const riskStr = risks.length > 0 ? ` [${risks.map(r => r.label).join(', ')}]` : '';
    return `  - ${d.opp_name}: ${formatUSD(Number(d.pbr) || 0)} PBR, stage: ${d.current_stage_name}, closing: ${toDateStr(d.close_date) || 'no date'}, next step: ${d.next_step || 'NONE'}${riskStr}`;
  };

  const forecastList = forecastDeals.map(formatDeal).join('\n') || '  (none)';
  const pipelineList = pipelineDeals.map(formatDeal).join('\n') || '  (none)';

  const openPipelinePBR = openDeals.reduce((s, d) => s + (Number(d.pbr) || 0), 0);
  const coverage = gap > 0 ? (openPipelinePBR / gap).toFixed(1) : 'N/A';

  const dialerCalls = Number(calls.dialer_calls) || 0;
  const meetingsCount = Number(calls.meetings) || 0;
  const connectedCalls = Number(calls.connected_calls) || 0;

  return `You are an expert sales manager coaching an Account Executive on the AMER SMB Cross-Sell team at Shopify.

The rep's goal is to hit their quarterly PBR (Projected Billed Revenue) target. Based on the data below, identify 3-5 highest-impact actions for this week, ranked by their effect on quarterly attainment.

Think like a sales manager: What matters most for hitting the number? Be specific — name deals, amounts, and concrete actions. No generic advice like "follow up on deals" — say exactly which deal, what action, and why it's urgent.

REP: ${data.repName}
QUARTER: ${QUARTER.label} | ${days} days remaining
ATTAINMENT: ${formatUSD(wonPBR)} / ${formatUSD(quota)} = ${attPct}% | Gap: ${formatUSD(gap)}
PIPELINE COVERAGE: ${coverage}x (target: ${BENCHMARKS.pipelineCoverage}x)

FORECAST-STAGE DEALS (high confidence):
${forecastList}

PIPELINE-STAGE DEALS:
${pipelineList}

ACTIVITY (last 7 days):
  Dialer calls: ${dialerCalls} (benchmark: ${BENCHMARKS.dialerCallsPerDay * 5}/week)${dialerCalls < BENCHMARKS.dialerCallsPerDay * 5 ? ' ⚠️ BELOW TARGET' : ''}
  Meetings: ${meetingsCount} (benchmark: ${BENCHMARKS.meetingsPerWeek}/week)${meetingsCount < BENCHMARKS.meetingsPerWeek ? ' ⚠️ BELOW TARGET' : ''}
  Connected calls: ${connectedCalls}

SALESFORCE HEALTH:
  Hygiene score: ${computeHygieneScore(openDeals)}/100
  Deals with no next step: ${openDeals.filter(d => !d.next_step || d.next_step.trim() === '').length}
  Overdue close dates: ${openDeals.filter(d => computeHygieneFlags(d).some(f => f.type === 'overdue')).length}

Return a JSON array of priorities:
[
  { "rank": 1, "headline": "Short action headline", "detail": "Specific explanation with deal name, amount, and why this week", "urgency": "high|medium|low" },
  ...
]

Only return the JSON array, no other text.`;
}

function buildTeamPrompt(data) {
  const days = daysLeft();
  const wonMap = {};
  for (const row of (data.wonPBR || [])) wonMap[row.salesforce_owner_name] = Number(row.won_pbr) || 0;

  const pipelineMap = {};
  for (const d of (data.pipeline || [])) {
    if (d.is_closed) continue;
    if (!pipelineMap[d.salesforce_owner_name]) pipelineMap[d.salesforce_owner_name] = [];
    pipelineMap[d.salesforce_owner_name].push(d);
  }

  // Build per-rep quota map from BQ data
  const quotaRows = data.quotas || [];
  const repQuotaMap = {};
  for (const q of quotaRows) {
    if (q.role_type === 'rep') repQuotaMap[q.name] = Number(q.quarterly_quota) || 0;
  }
  const teamQuota = Object.values(REP_ROSTER).reduce((sum, rep) => sum + (repQuotaMap[rep.sfName] || getQuotaFallback()), 0);

  const repSummaries = Object.entries(REP_ROSTER).map(([email, rep]) => {
    const won = wonMap[rep.sfName] || 0;
    const repQuota = repQuotaMap[rep.sfName] || getQuotaFallback();
    const att = repQuota > 0 ? ((won / repQuota) * 100).toFixed(1) : 0;
    const gap = Math.max(0, repQuota - won);
    const deals = pipelineMap[rep.sfName] || [];
    const pipePBR = deals.reduce((s, d) => s + (Number(d.pbr) || 0), 0);
    const topDeals = deals.slice(0, 3).map(d => `${d.opp_name} (${formatUSD(Number(d.pbr) || 0)})`).join(', ');
    const hygieneScore = computeHygieneScore(deals);
    const flagged = deals.filter(d => computeHygieneFlags(d).length > 0).length;
    return `${rep.name}: ${att}% attain, gap ${formatUSD(gap)}, pipeline ${formatUSD(pipePBR)} (${deals.length} deals, hygiene: ${hygieneScore}/100, ${flagged} flagged). Top: ${topDeals || 'none'}`;
  }).join('\n');

  return `You are an expert sales manager overseeing the AMER SMB Cross-Sell team (~63 reps across 6 coaches) at Shopify.

Analyze the team's performance data and identify 3-5 highest-priority actions for the manager this week. Focus on:
- Which reps need the most attention (coaching, deal support, pipeline building)
- Which deals across the team are at highest risk or highest value
- Activity gaps that need addressing
- Salesforce hygiene issues (reps with low hygiene scores, deals missing next steps or with overdue close dates)

QUARTER: ${QUARTER.label} | ${days} days remaining
QUOTA: ${formatUSD(teamQuota)} team total (per-rep quotas vary)

TEAM STATUS:
${repSummaries}

Return a JSON array of priorities:
[
  { "rank": 1, "headline": "Short action headline", "detail": "Specific explanation naming reps, deals, and actions", "urgency": "high|medium|low" },
  ...
]

Only return the JSON array, no other text.`;
}

// --- Streaming (delegates to shared ai-stream.js) ---

async function streamPriorities(prompt) {
  return streamAI(prompt, { maxTokens: 2000 });
}

// --- Render ---

function renderPriorities(container, priorities) {
  if (!priorities || priorities.length === 0) {
    container.innerHTML = '<div class="empty-state">No priorities generated.</div>';
    return;
  }

  container.innerHTML = priorities.map(p => `
    <div class="priority-card">
      <div class="priority-rank">${p.rank}</div>
      <div class="priority-content">
        <div class="priority-headline">
          ${p.headline}
          <span class="urgency-badge urgency-${p.urgency || 'medium'}">${(p.urgency || 'medium').toUpperCase()}</span>
        </div>
        <div class="priority-detail">${p.detail}</div>
      </div>
    </div>
  `).join('');
}
