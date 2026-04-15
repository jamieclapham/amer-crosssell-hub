// Email Composer — AI-drafted emails based on deal context
import { streamAI } from '../ai-stream.js';
import { formatUSD, toDateStr } from '../ui.js';

const EMAIL_CACHE = quick.db.collection('hub_email_drafts');
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days

export function renderEmailComposer(container, appData) {
  const deals = (appData.pipeline || []).filter(d => !d.is_closed);
  const dealOptions = deals.map(d =>
    `<option value="${d.opportunity_id}">${d.opp_name || '—'} (${formatUSD(Number(d.pbr) || 0)})</option>`
  ).join('');

  container.innerHTML = `
    <div class="toolkit-form">
      <label>Select Deal</label>
      <select id="email-deal-select">
        <option value="">Choose a deal...</option>
        ${dealOptions}
      </select>
      <label>Email Type</label>
      <select id="email-type-select">
        <option value="follow-up">Follow-up after meeting</option>
        <option value="intro">Introduction / first outreach</option>
        <option value="proposal">Proposal / next steps</option>
        <option value="closing">Closing / urgency push</option>
      </select>
      <label>Additional Context (optional)</label>
      <textarea id="email-notes" placeholder="Any specific points to mention, objections to address, or context..."></textarea>
      <button class="refresh-btn" style="align-self:flex-start;padding:8px 16px" onclick="window.generateEmail()">Generate Email</button>
    </div>
    <div id="email-result"></div>
  `;

  window.generateEmail = async function() {
    const dealId = document.getElementById('email-deal-select').value;
    const emailType = document.getElementById('email-type-select').value;
    const notes = document.getElementById('email-notes').value;
    const resultDiv = document.getElementById('email-result');

    if (!dealId) {
      resultDiv.innerHTML = '<div class="empty-state" style="padding:12px;font-size:12px">Please select a deal.</div>';
      return;
    }

    const deal = deals.find(d => d.opportunity_id === dealId);
    if (!deal) return;

    // Check cache
    const cacheKey = `${dealId}:${emailType}`;
    try {
      const cached = await EMAIL_CACHE.where({ key: cacheKey }).orderBy('created_at', 'desc').limit(1).find();
      if (cached.length) {
        const age = Date.now() - new Date(cached[0].created_at).getTime();
        if (age < CACHE_TTL_MS && !notes) {
          resultDiv.innerHTML = renderEmailResult(cached[0].data);
          return;
        }
      }
    } catch (_) {}

    resultDiv.innerHTML = '<div class="ai-loading">Composing email...</div>';

    const prompt = `You are a senior enterprise sales rep at Shopify writing an email to a prospect.

DEAL CONTEXT:
- Merchant: ${deal.opp_name || 'Unknown'}
- Stage: ${deal.current_stage_name || 'Unknown'}
- PBR: ${formatUSD(Number(deal.pbr) || 0)}
- Close date: ${toDateStr(deal.close_date) || 'Not set'}
- Next step: ${deal.next_step || 'None defined'}
- Forecast category: ${deal.forecast_category || 'Unknown'}

EMAIL TYPE: ${emailType}
${notes ? `ADDITIONAL CONTEXT: ${notes}` : ''}

Write a professional, concise email (150-250 words). Use a warm but direct tone. Include:
- Clear subject line
- Personalized opening
- Value proposition tied to their business
- Clear call to action

Format as:
Subject: [subject line]

[email body]`;

    try {
      const response = await streamAI(prompt, { maxTokens: 1000 });
      resultDiv.innerHTML = renderEmailResult(response);

      // Cache result (only if no custom notes)
      if (!notes) {
        try {
          await EMAIL_CACHE.create({ key: cacheKey, data: response, created_at: new Date().toISOString() });
        } catch (_) {}
      }

      // Log usage
      logToolUsage('email_composer', appData.repEmail, dealId);
    } catch (err) {
      resultDiv.innerHTML = `<div class="empty-state" style="padding:12px;font-size:12px">Error: ${err.message}</div>`;
    }
  };
}

function renderEmailResult(text) {
  return `<div class="toolkit-result">
    <div style="display:flex;justify-content:flex-end;margin-bottom:8px">
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('email-text').textContent).then(()=>this.textContent='Copied!').catch(()=>{})">Copy to clipboard</button>
    </div>
    <div id="email-text">${text}</div>
  </div>`;
}

async function logToolUsage(tool, repEmail, oppId) {
  try {
    const usage = quick.db.collection('hub_tool_usage');
    await usage.create({ tool, rep_email: repEmail || '', opp_id: oppId || '', created_at: new Date().toISOString() });
  } catch (_) {}
}
