// AI Toolkit Hub — External tools + inline AI tools (Email Composer, Account Research, Objection Handler)
import { renderEmailComposer } from '../ai-tools/email-composer.js';
import { renderAccountResearch } from '../ai-tools/account-research.js';
import { renderObjectionHandler } from '../ai-tools/objection-handler.js';
import { REP_ROSTER } from '../config.js';

const TOOL_USAGE = quick.db.collection('hub_tool_usage');

export function renderAITools() {
  const el = document.getElementById('tab-toolkit');
  if (!el) return;

  const appData = window.__appData || {};
  const isManager = window.__currentUser?.isManager;

  el.innerHTML = `
    ${isManager ? '<div id="toolkit-usage-card"></div>' : ''}

    <div class="card toolkit-section">
      <div class="card-title">Inline AI Tools</div>
      <p style="font-size:13px;color:var(--text-secondary);margin-bottom:16px">
        AI-powered tools that use your pipeline data. Results are generated in real-time.
      </p>

      <!-- Email Composer -->
      <div class="toolkit-panel">
        <div class="toolkit-panel-header" onclick="window.toggleToolPanel('email')">
          <div style="display:flex;align-items:center">
            <span class="toolkit-panel-icon">📧</span>
            <div>
              <div class="toolkit-panel-title">Email Composer</div>
              <div class="toolkit-panel-desc">AI-drafted emails tailored to your deal context</div>
            </div>
          </div>
          <span id="email-toggle" style="font-size:18px;color:var(--text-muted)">+</span>
        </div>
        <div class="toolkit-panel-body collapsed" id="email-panel"></div>
      </div>

      <!-- Account Research -->
      <div class="toolkit-panel">
        <div class="toolkit-panel-header" onclick="window.toggleToolPanel('research')">
          <div style="display:flex;align-items:center">
            <span class="toolkit-panel-icon">🔍</span>
            <div>
              <div class="toolkit-panel-title">Account Research</div>
              <div class="toolkit-panel-desc">Pre-call intelligence and merchant one-sheeters</div>
            </div>
          </div>
          <span id="research-toggle" style="font-size:18px;color:var(--text-muted)">+</span>
        </div>
        <div class="toolkit-panel-body collapsed" id="research-panel"></div>
      </div>

      <!-- Objection Handler -->
      <div class="toolkit-panel">
        <div class="toolkit-panel-header" onclick="window.toggleToolPanel('objection')">
          <div style="display:flex;align-items:center">
            <span class="toolkit-panel-icon">💬</span>
            <div>
              <div class="toolkit-panel-title">Objection Handler</div>
              <div class="toolkit-panel-desc">Real-time rebuttals for common sales objections</div>
            </div>
          </div>
          <span id="objection-toggle" style="font-size:18px;color:var(--text-muted)">+</span>
        </div>
        <div class="toolkit-panel-body collapsed" id="objection-panel"></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">External Tools</div>
      <p style="font-size:13px;color:var(--text-secondary);margin-bottom:16px">
        Standalone AI tools. Click any card to open in a new tab.
      </p>
      <div class="tools-grid">
        <a class="tool-card" href="https://plus-business-case.quick.shopify.io" target="_blank">
          <div class="tool-icon"><img src="img/shopify-plus.png" alt="Shopify Plus" style="height:32px"></div>
          <div class="tool-title">Plus Business Case Generator</div>
          <div class="tool-desc">Generate CEO-ready Shopify Plus upgrade analyses with live data.</div>
        </a>
        <a class="tool-card" href="https://sp-upgrade-tool.quick.shopify.io/" target="_blank">
          <div class="tool-icon"><img src="img/shopify-logo.svg" alt="Shopify" style="height:32px"></div>
          <div class="tool-title">Advanced Plan & SP Upgrade</div>
          <div class="tool-desc">Upgrade Basic & Grow merchants to Advanced plan not on Shopify Payments and sell them Shopify Payments. Enter a Shop ID to show them the fee savings and make the case for Advanced Plan & Shopify Payments.</div>
        </a>
        <a class="tool-card" href="https://sp-pitch-builder.quick.shopify.io" target="_blank">
          <div class="tool-icon"><img src="img/shop-pay-logo.png" alt="Shopify Payments" style="height:48px"></div>
          <div class="tool-title">Shopify Payments Pitch Builder</div>
          <div class="tool-desc">Build a tailored Shopify Payments business case with live merchant data.</div>
        </a>
        <a class="tool-card" href="https://company-finder.quick.shopify.io" target="_blank">
          <div class="tool-icon">🏢</div>
          <div class="tool-title">Company Finder</div>
          <div class="tool-desc">Search Salesforce for existing companies before creating accounts.</div>
        </a>
        <a class="tool-card" href="https://payment-pain-calculator.quick.shopify.io" target="_blank">
          <div class="tool-icon">💳</div>
          <div class="tool-title">Retail Payments Pain Calculator</div>
          <div class="tool-desc">Analyze how much retail stores lose by not using POS Pro with Retail Payments.</div>
        </a>
        <a class="tool-card" href="https://sp-supportability-checker.quick.shopify.io/" target="_blank">
          <div class="tool-icon">✅</div>
          <div class="tool-title">SP Supportability Checker</div>
          <div class="tool-desc">Check whether a merchant is eligible for Shopify Payments support.</div>
        </a>
        <a class="tool-card" href="https://shopid-analyser.quick.shopify.io/" target="_blank">
          <div class="tool-icon">🔎</div>
          <div class="tool-title">ShopID Analyser</div>
          <div class="tool-desc">Analyse a merchant's ShopID to surface insights and opportunities.</div>
        </a>
      </div>
    </div>
  `;

  // Panel toggle + lazy initialization
  const initialized = {};
  window.toggleToolPanel = function(tool) {
    const panelMap = { email: 'email-panel', research: 'research-panel', objection: 'objection-panel' };
    const panel = document.getElementById(panelMap[tool]);
    const toggle = document.getElementById(`${tool}-toggle`);
    if (!panel) return;

    const isCollapsed = panel.classList.contains('collapsed');
    panel.classList.toggle('collapsed');
    if (toggle) toggle.textContent = isCollapsed ? '−' : '+';

    // Lazy init on first expand
    if (isCollapsed && !initialized[tool]) {
      initialized[tool] = true;
      if (tool === 'email') renderEmailComposer(panel, appData);
      else if (tool === 'research') renderAccountResearch(panel, appData);
      else if (tool === 'objection') renderObjectionHandler(panel, appData);
    }
  };

  // Manager usage analytics
  if (isManager) loadToolUsage();
}

async function loadToolUsage() {
  const container = document.getElementById('toolkit-usage-card');
  if (!container) return;

  try {
    const sevenDaysAgo = new Date();
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7);
    const items = await TOOL_USAGE.find();
    const recent = items.filter(i => new Date(i.created_at) >= sevenDaysAgo);

    const byTool = {};
    const byRep = {};
    for (const item of recent) {
      byTool[item.tool] = (byTool[item.tool] || 0) + 1;
      if (item.rep_email) byRep[item.rep_email] = (byRep[item.rep_email] || 0) + 1;
    }

    const toolNames = { email_composer: 'Email Composer', account_research: 'Account Research', objection_handler: 'Objection Handler' };
    const toolRows = Object.entries(byTool).map(([tool, count]) =>
      `<span style="display:inline-block;margin-right:16px;font-size:13px">${toolNames[tool] || tool}: <strong>${count}</strong></span>`
    ).join('');

    const repRows = Object.entries(byRep)
      .sort((a, b) => b[1] - a[1])
      .map(([email, count]) => {
        const rep = REP_ROSTER[email];
        return `<span style="display:inline-block;margin-right:16px;font-size:13px">${rep ? rep.name : email}: <strong>${count}</strong></span>`;
      }).join('');

    container.innerHTML = `<div class="card">
      <div class="card-title">Tool Usage (Last 7 Days)</div>
      <div style="margin-bottom:8px">${toolRows || '<span class="text-muted" style="font-size:13px">No usage yet</span>'}</div>
      ${repRows ? `<div style="font-size:12px;color:var(--text-secondary);margin-top:4px">By rep: ${repRows}</div>` : ''}
      <div class="last-updated">Total: ${recent.length} uses across ${Object.keys(byRep).length} reps</div>
    </div>`;
  } catch (_) {
    container.innerHTML = '';
  }
}
