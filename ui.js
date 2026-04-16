// SalesHub — Shared UI helpers
import { QUARTER, SALESFORCE_BASE_URL } from './config.js';

// --- Tab switching ---

const TABS = ['overview', 'tiering', 'coaching', 'toolkit'];

export function switchTab(tabName) {
  for (const t of TABS) {
    const btn = document.getElementById(`tab-btn-${t}`);
    const pane = document.getElementById(`tab-${t}`);
    if (!btn || !pane) continue;
    if (t === tabName) {
      btn.classList.add('active');
      pane.style.display = '';
    } else {
      btn.classList.remove('active');
      pane.style.display = 'none';
    }
  }
  // Log tab view for analytics
  try { quick.db.collection('coaching_hub_tab_views').create({ email: window.__currentUser?.email || '', tab: tabName, subtab: null, created_at: new Date().toISOString() }); } catch (_) {}
}

// --- Formatters ---

export function formatUSD(n) {
  if (n == null || isNaN(n)) return '$0';
  const abs = Math.abs(Math.round(n));
  let formatted;
  if (abs >= 1000000) formatted = `$${(abs / 1000000).toFixed(abs >= 10000000 ? 1 : 2)}M`;
  else if (abs >= 1000) formatted = `$${(abs / 1000).toFixed(abs >= 100000 ? 0 : 1)}K`;
  else formatted = `$${abs.toLocaleString()}`;
  return n < 0 ? `-${formatted}` : formatted;
}

export function formatUSDFull(n) {
  if (n == null || isNaN(n)) return '$0';
  const abs = Math.abs(Math.round(n));
  if (abs >= 1000000) return `$${(abs / 1000000).toFixed(2)}M`;
  return `$${Math.round(n).toLocaleString()}`;
}

export function formatPct(n) {
  if (n == null || isNaN(n)) return '0%';
  return `${Math.round(n)}%`;
}

export function formatDate(d) {
  if (!d) return '—';
  // BigQuery may return {value: '2026-03-28'} or a plain string
  let str = d;
  if (typeof d === 'object' && d !== null) str = d.value || d.v || String(d);
  if (typeof str !== 'string') return '—';
  const date = new Date(str + (str.includes('T') ? '' : 'T00:00:00'));
  if (isNaN(date.getTime())) return '—';
  return date.toLocaleDateString('en-GB', { day: 'numeric', month: 'short' });
}

export function sfLink(name, opportunityId) {
  const display = name || '—';
  if (!opportunityId) return `<strong>${display}</strong>`;
  return `<a href="${SALESFORCE_BASE_URL}/${opportunityId}/view" target="_blank" rel="noopener" class="sf-link"><strong>${display}</strong></a>`;
}

export function daysLeft() {
  const end = new Date(QUARTER.end + 'T23:59:59');
  const now = new Date();
  return Math.max(0, Math.ceil((end - now) / 86400000));
}

// --- Loading states ---

export function showSpinner(containerId) {
  const el = document.getElementById(containerId);
  if (!el) return;
  el.innerHTML = '<div class="spinner"><div class="spinner-dot"></div><div class="spinner-dot"></div><div class="spinner-dot"></div></div>';
}

export function hideSpinner(containerId) {
  const el = document.getElementById(containerId);
  if (!el) return;
  const spinner = el.querySelector('.spinner');
  if (spinner) spinner.remove();
}

// --- Status bar ---

export function setStatus(type, html) {
  const bar = document.getElementById('status-bar');
  if (!bar) return;
  bar.className = `status-bar status-${type}`;
  bar.innerHTML = html;
  bar.style.display = html ? '' : 'none';
}

// --- Reusable components ---

export function renderMetricCard(label, value, subtext, colorClass) {
  return `<div class="metric-card ${colorClass || ''}">
    <div class="metric-label">${label}</div>
    <div class="metric-value">${value}</div>
    ${subtext ? `<div class="metric-sub">${subtext}</div>` : ''}
  </div>`;
}

export function renderProgressBar(current, target, label) {
  const pct = target > 0 ? Math.min(100, Math.round((current / target) * 100)) : 0;
  const cls = pct >= 100 ? 'bar-green' : pct >= 60 ? 'bar-yellow' : 'bar-red';
  return `<div class="progress-row">
    <span class="progress-label">${label}</span>
    <div class="progress-track"><div class="progress-fill ${cls}" style="width:${pct}%"></div></div>
    <span class="progress-value">${current} / ${target}</span>
  </div>`;
}

export function renderRiskBadge(flags) {
  if (!flags || flags.length === 0) return '';
  return flags.map(f => `<span class="risk-flag risk-${f.level}">${f.label}</span>`).join(' ');
}

// Extract a date string from a BQ value (could be string, {value:...}, or Date)
export function toDateStr(d) {
  if (!d) return null;
  if (typeof d === 'string') return d;
  if (typeof d === 'object' && d.value) return d.value;
  if (typeof d === 'object' && d.v) return d.v;
  return String(d);
}

// ISO week string: "2026-W12"
export function getISOWeek(date = new Date()) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
  return `${d.getUTCFullYear()}-W${String(weekNo).padStart(2, '0')}`;
}

// --- Score gauge (0-100, circular arc) ---

export function renderScoreGauge(score, label) {
  const pct = Math.max(0, Math.min(100, Math.round(score)));
  const color = pct >= 80 ? 'var(--accent)' : pct >= 60 ? 'var(--warning)' : 'var(--danger)';
  // SVG arc: 0-100 maps to 0-270 degrees
  const angle = (pct / 100) * 270;
  const rad = (angle - 135) * Math.PI / 180;
  const r = 40;
  const cx = 50, cy = 50;
  const x1 = cx + r * Math.cos(-135 * Math.PI / 180);
  const y1 = cy + r * Math.sin(-135 * Math.PI / 180);
  const x2 = cx + r * Math.cos(rad);
  const y2 = cy + r * Math.sin(rad);
  const largeArc = angle > 180 ? 1 : 0;

  return `<div class="score-gauge">
    <svg width="100" height="100" viewBox="0 0 100 100">
      <circle cx="${cx}" cy="${cy}" r="${r}" fill="none" stroke="#eee" stroke-width="8"
        stroke-dasharray="212 71" stroke-dashoffset="-35" stroke-linecap="round"/>
      ${pct > 0 ? `<path d="M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}"
        fill="none" stroke="${color}" stroke-width="8" stroke-linecap="round"/>` : ''}
      <text x="${cx}" y="${cy + 2}" text-anchor="middle" font-size="22" font-weight="700" fill="${color}">${pct}</text>
      <text x="${cx}" y="${cy + 14}" text-anchor="middle" font-size="9" fill="var(--text-muted)">${label || ''}</text>
    </svg>
  </div>`;
}

// --- Trend bars (weekly bar chart) ---

export function renderTrendBars(weeklyData, metric, benchmark) {
  if (!weeklyData || weeklyData.length === 0) return '<div class="empty-state" style="padding:16px;font-size:12px">No trend data yet.</div>';
  const max = Math.max(benchmark || 1, ...weeklyData.map(w => Number(w[metric]) || 0));
  const bars = weeklyData.map(w => {
    const val = Number(w[metric]) || 0;
    const height = Math.max(2, (val / max) * 60);
    const color = benchmark && val >= benchmark ? 'var(--accent)' : benchmark && val >= benchmark * 0.6 ? 'var(--warning)' : 'var(--danger)';
    const weekLabel = (w.week || '').replace(/^\d{4}-W/, 'W');
    return `<div class="trend-bar-col">
      <div class="trend-bar-value">${val}</div>
      <div class="trend-bar" style="height:${height}px;background:${color}"></div>
      <div class="trend-bar-label">${weekLabel}</div>
    </div>`;
  }).join('');

  return `<div class="trend-bars-container">
    ${benchmark ? `<div class="trend-benchmark" style="bottom:${(benchmark / max) * 60 + 18}px"></div>` : ''}
    <div class="trend-bars">${bars}</div>
  </div>`;
}

// --- Deal hygiene helpers ---

export function computeHygieneFlags(deal) {
  const flags = [];
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const closeStr = toDateStr(deal.close_date);
  const pbr = Number(deal.pbr) || 0;

  if (!closeStr) {
    flags.push({ level: 'high', label: 'No close date', type: 'no_close_date' });
  } else {
    const close = new Date(closeStr + 'T00:00:00');
    if (!isNaN(close.getTime()) && close < today) {
      flags.push({ level: 'high', label: 'Overdue', type: 'overdue' });
    }
  }

  if (!deal.next_step || (typeof deal.next_step === 'string' && deal.next_step.trim() === '')) {
    flags.push({ level: 'medium', label: 'No next step', type: 'no_next_step' });
  } else if (deal.updated_at) {
    // Next step exists but opportunity hasn't been updated in 14+ days → stale
    const updated = new Date(deal.updated_at.value || deal.updated_at);
    if (!isNaN(updated.getTime())) {
      const daysSinceUpdate = (today - updated) / 86400000;
      if (daysSinceUpdate >= 14) {
        flags.push({ level: 'medium', label: `Stale (${Math.floor(daysSinceUpdate)}d)`, type: 'stale_next_step' });
      }
    }
  }

  if (deal.forecast_category === 'Pipeline' && closeStr) {
    const close = new Date(closeStr + 'T00:00:00');
    const daysOut = (close - today) / 86400000;
    if (daysOut >= 0 && daysOut < 7) {
      flags.push({ level: 'high', label: 'Pipeline closing <7d', type: 'forecast_misaligned' });
    }
  }

  if (pbr > 0 && pbr < 10000) {
    flags.push({ level: 'low', label: 'Low PBR', type: 'low_pbr' });
  }

  return flags;
}

export function computeHygieneScore(deals) {
  if (!deals || deals.length === 0) return null; // No deals = no score

  // Flag-based scoring: penalize based on flags detected by computeHygieneFlags().
  // Weight = flaggedDeals/totalDeals so a rep with half their pipeline flagged is
  // penalised far more than one with a single bad deal in a clean portfolio.
  const PENALTY = { high: 15, medium: 8, low: 3 };
  const flaggedCount = deals.filter(d => computeHygieneFlags(d).length > 0).length;
  const flagRate = flaggedCount / deals.length;

  let totalPenalty = 0;
  for (const deal of deals) {
    const flags = computeHygieneFlags(deal);
    for (const flag of flags) {
      totalPenalty += (PENALTY[flag.level] || 5) * flagRate;
    }
  }

  return Math.max(0, Math.round(100 - totalPenalty));
}

// --- Delta badge ---

export function renderDelta(current, previous, suffix = '') {
  if (previous == null || current == null) return '<span class="text-muted">—</span>';
  const delta = current - previous;
  if (delta === 0) return '<span class="text-muted">0</span>';
  const sign = delta > 0 ? '+' : '';
  const color = delta > 0 ? 'var(--success)' : 'var(--danger)';
  return `<span style="font-weight:600;color:${color}">${sign}${typeof current === 'number' && current % 1 !== 0 ? delta.toFixed(1) : delta}${suffix}</span>`;
}

// --- Sparkline (mini trend line) ---

export function renderSparkline(values, width = 80, height = 24) {
  if (!values || values.length < 2) return '';
  const max = Math.max(...values);
  const min = Math.min(...values);
  const range = max - min || 1;
  const step = width / (values.length - 1);
  const points = values.map((v, i) => `${i * step},${height - ((v - min) / range) * (height - 4) - 2}`).join(' ');
  const color = values[values.length - 1] >= values[0] ? 'var(--success)' : 'var(--danger)';
  return `<svg width="${width}" height="${height}" style="vertical-align:middle"><polyline points="${points}" fill="none" stroke="${color}" stroke-width="2" stroke-linecap="round"/></svg>`;
}

export function computeDealRisks(deal) {
  const flags = [];
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const closeStr = toDateStr(deal.close_date);
  if (closeStr) {
    const close = new Date(closeStr + 'T00:00:00');
    if (!isNaN(close.getTime())) {
      if (close < today) flags.push({ level: 'high', label: 'Overdue' });
      else if ((close - today) / 86400000 <= 7) flags.push({ level: 'medium', label: 'Closing soon' });
      if (deal.forecast_category === 'Pipeline' && (close - today) / 86400000 < 14) {
        flags.push({ level: 'medium', label: 'Pipeline < 14d' });
      }
    }
  }
  if (!deal.next_step || (typeof deal.next_step === 'string' && deal.next_step.trim() === '')) {
    flags.push({ level: 'medium', label: 'No next step' });
  }
  return flags;
}
