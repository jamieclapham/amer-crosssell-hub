// Account Research / One-Sheeter — AI-generated merchant briefing
import { streamAI } from '../ai-stream.js';
import { formatUSD, toDateStr } from '../ui.js';

const RESEARCH_CACHE = quick.db.collection('hub_research');
const CACHE_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

export function renderAccountResearch(container, appData) {
  const deals = (appData.pipeline || []).filter(d => !d.is_closed);
  const dealOptions = deals.map(d =>
    `<option value="${d.opportunity_id}">${d.opp_name || '—'} (${formatUSD(Number(d.pbr) || 0)})</option>`
  ).join('');

  container.innerHTML = `
    <div class="toolkit-form">
      <label>Select Deal</label>
      <select id="research-deal-select">
        <option value="">Choose a deal...</option>
        ${dealOptions}
      </select>
      <button class="refresh-btn" style="align-self:flex-start;padding:8px 16px" onclick="window.generateResearch()">Generate One-Sheeter</button>
    </div>
    <div id="research-result"></div>
  `;

  window.generateResearch = async function() {
    const dealId = document.getElementById('research-deal-select').value;
    const resultDiv = document.getElementById('research-result');

    if (!dealId) {
      resultDiv.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Please select a deal.</div>';
      return;
    }

    const deal = deals.find(d => d.opportunity_id === dealId);
    if (!deal) return;

    // Check cache
    try {
      const cached = await RESEARCH_CACHE.where({ key: dealId }).orderBy('created_at', 'desc').limit(1).find();
      if (cached.length) {
        const age = Date.now() - new Date(cached[0].created_at).getTime();
        if (age < CACHE_TTL_MS) {
          resultDiv.innerHTML = `<div class="toolkit-result">${cached[0].data}</div>`;
          return;
        }
      }
    } catch (_) {}

    resultDiv.innerHTML = '<div class="ai-loading">Researching account...</div>';

    // Build context from deal + recent calls
    const recentCalls = (appData.recentCalls || [])
      .filter(c => c.call_title && deal.opp_name && c.call_title.toLowerCase().includes(deal.opp_name.split(' ')[0].toLowerCase()))
      .slice(0, 3)
      .map(c => `- ${c.call_title} (${toDateStr(c.event_start)}): ${c.summary_text || 'No summary'}`)
      .join('\n');

    const prompt = `Create a pre-call intelligence one-sheeter for a Shopify Plus sales opportunity.

DEAL:
- Merchant: ${deal.opp_name || 'Unknown'}
- Stage: ${deal.current_stage_name || 'Unknown'}
- PBR: ${formatUSD(Number(deal.pbr) || 0)}
- Close date: ${toDateStr(deal.close_date) || 'Not set'}
- Next step: ${deal.next_step || 'None'}
- Forecast: ${deal.forecast_category || 'Unknown'}

${recentCalls ? `RECENT CALL NOTES:\n${recentCalls}` : ''}

Generate a concise briefing (300-400 words) with these sections:
1. MERCHANT PROFILE — What we know about this business
2. KEY PAIN POINTS — Based on stage and any call notes
3. VALUE PROPOSITION — Why Shopify Plus fits
4. RECOMMENDED APPROACH — Specific talking points and strategy
5. RISK FACTORS — What could derail this deal

Be specific and actionable. No generic advice.`;

    try {
      const response = await streamAI(prompt, { maxTokens: 1500 });
      resultDiv.innerHTML = `<div class="toolkit-result">${response}</div>`;

      try {
        await RESEARCH_CACHE.create({ key: dealId, data: response, created_at: new Date().toISOString() });
      } catch (_) {}

      logToolUsage('account_research', appData.repEmail, dealId);
    } catch (err) {
      resultDiv.innerHTML = `<div class="empty-state" style="padding:12px;font-size:12px">Error: ${err.message}</div>`;
    }
  };
}

async function logToolUsage(tool, repEmail, oppId) {
  try {
    const usage = quick.db.collection('hub_tool_usage');
    await usage.create({ tool, rep_email: repEmail || '', opp_id: oppId || '', created_at: new Date().toISOString() });
  } catch (_) {}
}
