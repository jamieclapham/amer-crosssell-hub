// Chart.js utility module — shared defaults, lifecycle, and factory functions
// Requires Chart.js 4.4.1 loaded globally via CDN in index.html

// ─── Chart Instance Registry ───

const chartInstances = {};

export function destroyChart(id) {
  if (chartInstances[id]) {
    try { chartInstances[id].destroy(); } catch (_) {}
    delete chartInstances[id];
  }
}

export function destroyAllCharts() {
  for (const id of Object.keys(chartInstances)) destroyChart(id);
}

function registerChart(id, chart) {
  destroyChart(id);
  chartInstances[id] = chart;
}

// ─── Light-Theme Defaults ───

const DEFAULTS = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: {
    legend: {
      display: true,
      labels: { color: '#666', boxWidth: 10, font: { size: 11 } },
    },
    tooltip: {
      backgroundColor: '#1a1a1a',
      titleColor: '#fff',
      bodyColor: '#fff',
      cornerRadius: 6,
      padding: 8,
    },
  },
  scales: {
    x: {
      grid: { color: 'rgba(0,0,0,0.06)' },
      ticks: { color: '#999', font: { size: 11 } },
      border: { color: '#e5e5e5' },
    },
    y: {
      grid: { color: 'rgba(0,0,0,0.06)' },
      ticks: { color: '#999', font: { size: 11 } },
      border: { color: '#e5e5e5' },
    },
  },
};

// ─── Color Palette ───

export const COLORS = {
  primary:      'rgba(13, 148, 136, 1)',
  primaryFill:  'rgba(13, 148, 136, 0.08)',
  primaryBar:   'rgba(13, 148, 136, 0.7)',
  calls:        'rgba(13, 148, 136, 1)',
  callsFill:    'rgba(13, 148, 136, 0.08)',
  callsBar:     'rgba(13, 148, 136, 0.7)',
  meetings:     'rgba(249, 115, 22, 0.8)',
  meetingsFill: 'rgba(249, 115, 22, 0.08)',
  meetingsBar:  'rgba(249, 115, 22, 0.7)',
  teamAvg:      'rgba(139, 145, 176, 0.6)',
  teamAvgBar:   'rgba(139, 145, 176, 0.3)',
  success:      'rgba(34, 197, 94, 0.7)',
  danger:       'rgba(239, 68, 68, 0.7)',
  warning:      'rgba(245, 158, 11, 0.7)',
};

// ─── Factory: Line Chart ───

export function createLineChart(canvasId, { labels, datasets, yTickCallback, legendDisplay = true, xRotation = 0, yMax = null }) {
  const el = document.getElementById(canvasId);
  if (!el) return null;

  const chart = new Chart(el, {
    type: 'line',
    data: { labels, datasets },
    options: {
      ...DEFAULTS,
      plugins: {
        ...DEFAULTS.plugins,
        legend: { ...DEFAULTS.plugins.legend, display: legendDisplay },
      },
      scales: {
        x: {
          ...DEFAULTS.scales.x,
          ticks: {
            ...DEFAULTS.scales.x.ticks,
            maxRotation: xRotation,
            minRotation: xRotation,
          },
        },
        y: {
          ...DEFAULTS.scales.y,
          beginAtZero: true,
          ...(yMax != null ? { max: yMax } : {}),
          ticks: {
            ...DEFAULTS.scales.y.ticks,
            ...(yTickCallback ? { callback: yTickCallback } : {}),
          },
        },
      },
    },
  });

  registerChart(canvasId, chart);
  return chart;
}

// ─── Factory: Bar Chart ───

export function createBarChart(canvasId, { labels, datasets, yTickCallback, legendDisplay = true, stacked = false }) {
  const el = document.getElementById(canvasId);
  if (!el) return null;

  const chart = new Chart(el, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      ...DEFAULTS,
      plugins: {
        ...DEFAULTS.plugins,
        legend: { ...DEFAULTS.plugins.legend, display: legendDisplay },
      },
      scales: {
        x: {
          ...DEFAULTS.scales.x,
          stacked,
        },
        y: {
          ...DEFAULTS.scales.y,
          beginAtZero: true,
          stacked,
          ticks: {
            ...DEFAULTS.scales.y.ticks,
            ...(yTickCallback ? { callback: yTickCallback } : {}),
          },
        },
      },
    },
  });

  registerChart(canvasId, chart);
  return chart;
}

// ─── Dataset Builders (reduce boilerplate in tab files) ───

export function repLineDataset(label, data, color = COLORS.primary, fillColor = COLORS.primaryFill) {
  return {
    label,
    data,
    borderColor: color,
    backgroundColor: fillColor,
    fill: true,
    tension: 0.4,
    pointRadius: 3,
    pointBackgroundColor: color,
    borderWidth: 2,
  };
}

export function teamAvgLineDataset(data) {
  return {
    label: 'Team Avg',
    data,
    borderColor: COLORS.teamAvg,
    backgroundColor: 'transparent',
    borderDash: [4, 3],
    tension: 0.4,
    pointRadius: 2,
    pointBackgroundColor: COLORS.teamAvg,
    borderWidth: 1.5,
    fill: false,
  };
}

export function repBarDataset(label, data, color = COLORS.primaryBar) {
  return { label, data, backgroundColor: color, borderRadius: 4 };
}

export function teamAvgBarDataset(data) {
  return { label: 'Team Avg', data, backgroundColor: COLORS.teamAvgBar, borderRadius: 4 };
}

// ─── MEDDIC Dimension Colors & Dataset Builder ───

export const MEDDIC_DIM_COLORS = {
  'Metrics':           'rgba(13, 148, 136, 0.45)',
  'Economic Buyer':    'rgba(249, 115, 22, 0.45)',
  'Decision Criteria': 'rgba(34, 197, 94, 0.45)',
  'Decision Process':  'rgba(245, 158, 11, 0.45)',
  'Identify Pain':     'rgba(239, 68, 68, 0.45)',
  'Champion':          'rgba(6, 182, 212, 0.45)',
};

export function dimLineDataset(label, data, color) {
  return {
    label,
    data,
    borderColor: color,
    backgroundColor: 'transparent',
    fill: false,
    tension: 0.4,
    pointRadius: 2,
    pointBackgroundColor: color,
    borderWidth: 1.5,
  };
}

// ─── Helpers ───

export function formatWeekLabel(weekStr) {
  return (weekStr || '').replace(/^\d{4}-/, '');
}

export const currencyTick = v => {
  if (v >= 1_000_000) return '$' + (v / 1_000_000).toFixed(1) + 'M';
  if (v >= 1_000) return '$' + Math.round(v / 1_000) + 'k';
  return '$' + v;
};

// Compute weekly team avg for a metric from all-reps trending data
export function computeWeeklyTeamAvg(allRepsData, metric, repEmails) {
  const weekMap = {};
  const repCount = repEmails instanceof Set ? repEmails.size : repEmails.length;
  if (!repCount) return {};
  for (const row of allRepsData) {
    if (repEmails instanceof Set ? !repEmails.has(row.rep_email) : !repEmails.includes(row.rep_email)) continue;
    const week = row.week;
    if (!weekMap[week]) weekMap[week] = 0;
    weekMap[week] += Number(row[metric]) || 0;
  }
  const result = {};
  for (const [week, total] of Object.entries(weekMap)) {
    result[week] = Math.round(total / repCount);
  }
  return result;
}

// Compute team avg pipeline PBR per stage
export function computeTeamAvgByStage(allRepsPipeline, repCount) {
  if (!repCount) return {};
  const stageMap = {};
  for (const d of allRepsPipeline) {
    if (d.is_closed) continue;
    const stage = d.current_stage_name || 'Unknown';
    stageMap[stage] = (stageMap[stage] || 0) + (Number(d.pbr) || 0);
  }
  for (const stage of Object.keys(stageMap)) {
    stageMap[stage] = Math.round(stageMap[stage] / repCount);
  }
  return stageMap;
}
