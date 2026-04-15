// Objection Handler — AI-powered rebuttals for common sales objections
import { streamAI } from '../ai-stream.js';
import { formatUSD } from '../ui.js';

export function renderObjectionHandler(container, appData) {
  const deals = (appData.pipeline || []).filter(d => !d.is_closed);
  const dealOptions = deals.map(d =>
    `<option value="${d.opportunity_id}">${d.opp_name || '—'}</option>`
  ).join('');

  container.innerHTML = `
    <div class="toolkit-form">
      <label>The Objection</label>
      <textarea id="objection-text" placeholder="Type the objection you're hearing, e.g. 'We're happy with our current payment provider' or 'Shopify Plus is too expensive for us'"></textarea>
      <label>Deal Context (optional — helps tailor the response)</label>
      <select id="objection-deal-select">
        <option value="">No specific deal</option>
        ${dealOptions}
      </select>
      <button class="refresh-btn" style="align-self:flex-start;padding:8px 16px" onclick="window.handleObjection()">Get Rebuttal</button>
    </div>
    <div id="objection-result"></div>
  `;

  window.handleObjection = async function() {
    const objection = document.getElementById('objection-text').value.trim();
    const dealId = document.getElementById('objection-deal-select').value;
    const resultDiv = document.getElementById('objection-result');

    if (!objection) {
      resultDiv.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Please enter an objection.</div>';
      return;
    }

    const deal = dealId ? deals.find(d => d.opportunity_id === dealId) : null;
    resultDiv.innerHTML = '<div class="ai-loading">Building rebuttal...</div>';

    const dealContext = deal ? `\nDEAL CONTEXT:
- Merchant: ${deal.opp_name}
- Stage: ${deal.current_stage_name}
- PBR: ${formatUSD(Number(deal.pbr) || 0)}
- Current category: ${deal.forecast_category}` : '';

    const prompt = `You are a senior Shopify Plus sales coach. A rep is facing this objection from a prospect:

"${objection}"
${dealContext}

Provide a structured rebuttal (200-300 words):

1. ACKNOWLEDGE — Show you understand their concern (1-2 sentences)
2. REFRAME — Shift the perspective (2-3 sentences)
3. EVIDENCE — Specific Shopify data points, merchant success stories, or competitive advantages (2-3 bullets)
4. RESPONSE SCRIPT — Exact words the rep can use (3-4 sentences, conversational tone)
5. FOLLOW-UP QUESTION — One question to keep the conversation going

Be specific to Shopify Plus. Reference real capabilities (checkout extensibility, B2B, multi-store, Shopify Payments rates, etc.) where relevant.`;

    try {
      const response = await streamAI(prompt, { maxTokens: 1200 });
      resultDiv.innerHTML = `<div class="toolkit-result">${response}</div>`;
      logToolUsage('objection_handler', appData.repEmail, dealId);
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
