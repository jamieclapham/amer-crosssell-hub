// SalesHub — BigQuery Queries + Caching
// Uses quick.dw.querySync() and quick.db for caching.
// PBR source: raw_salesforce_banff.opportunity.Projected_Billed_Revenue__c (deal-level, not product-level).
// All opp queries filter: Type = 'Existing Business', IsDeleted = FALSE, record_type = 'Sales'.
// Team-wide queries resolve reps from sales_users_daily_snapshot (role-based, always current).
// IMPORTANT: quick.dw.querySync() returns { results: [...] } — always extract .results

import { QUARTER, NEXT_QUARTER, REP_ROSTER, ROLE_PATTERN } from './config.js';

// Helper: subquery returning today's active rep names from sales_users_daily_snapshot.
// team = null → all teams, 'D2CRETAIL1' or 'D2CRETAIL2' → specific coach team.
// Uses MAX(date) instead of CURRENT_DATE() because snapshot may lag by 1 day.
function teamRepSubquery(team = null) {
  const pattern = team ? `${ROLE_PATTERN}${team.replace('D2CRETAIL', '')}` : ROLE_PATTERN;
  return `(
    SELECT u.name
    FROM \`shopify-dw.sales.sales_users_daily_snapshot\` u
    WHERE u.date = (SELECT MAX(date) FROM \`shopify-dw.sales.sales_users_daily_snapshot\`)
      AND u.is_active = TRUE
      AND u.user_role LIKE '%${pattern}%'
  )`;
}

const cache = quick.db.collection('hub_cache');
const CACHE_TTL_MS = 1 * 60 * 60 * 1000; // 1 hour

let authDone = false;

async function ensureAuth() {
  if (authDone) return;
  await quick.auth.requestScopes(['https://www.googleapis.com/auth/bigquery']);
  authDone = true;
}

async function query(sql) {
  const res = await quick.dw.querySync(sql);
  return res.results || [];
}

// --- Caching layer ---

async function getCachedOrFetch(cacheKey, fetchFn) {
  try {
    const items = await cache.where({ key: cacheKey }).orderBy('fetched_at', 'desc').limit(1).find();
    if (items.length) {
      const age = Date.now() - new Date(items[0].fetched_at).getTime();
      if (age < CACHE_TTL_MS) return JSON.parse(items[0].data);
    }
  } catch (_) { /* cache miss */ }
  const data = await fetchFn();
  try {
    await cache.create({ key: cacheKey, data: JSON.stringify(data), fetched_at: new Date().toISOString() });
  } catch (_) { /* ignore cache write errors */ }
  return data;
}

export async function clearCache() {
  try {
    const all = await cache.find();
    for (const item of all) await cache.delete(item._id);
  } catch (_) { /* ignore */ }
}

// --- Pipeline data (open opps with PBR) ---

export async function fetchPipelineData(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `pipeline:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.opportunity_id,
        o.name AS opp_name,
        o.current_stage_name,
        o.forecast_category,
        o.close_date,
        o.next_step,
        o.is_closed,
        o.is_won,
        o.merchant_intent,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      ORDER BY pbr DESC`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Won PBR this quarter ---

export async function fetchWonPBR(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `won:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].won_pbr) : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Call activity summary ---

export async function fetchCallActivity(repEmail, days = 7, forceRefresh = false) {
  await ensureAuth();
  const key = `calls:${repEmail}:${days}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        COUNT(DISTINCT sc.event_id) AS total_interactions,
        COUNTIF(sc.platform = 'salesloft_dialer') AS dialer_calls,
        COUNTIF(sc.platform IN ('google_meet','salesloft_conversation','salesloft')) AS meetings,
        COUNTIF(sc.call_disposition = 'Connected') AS connected_calls,
        COUNTIF(sc.has_transcript) AS transcribed,
        ROUND(AVG(IF(sc.call_duration_minutes > 0, sc.call_duration_minutes, NULL)), 1) AS avg_duration_min
      FROM \`shopify-dw.sales.sales_calls\` sc,
        UNNEST(sc.attendee_details) AS a
      WHERE a.is_shopify_employee = TRUE
        AND LOWER(a.attendee_email) = '${repEmail.toLowerCase()}'
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)`;
    const rows = await query(sql);
    return rows.length ? rows[0] : { total_interactions: 0, dialer_calls: 0, meetings: 0, connected_calls: 0, transcribed: 0, avg_duration_min: 0 };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Recent calls with summaries ---

export async function fetchRecentCalls(repEmail, days = 14, forceRefresh = false) {
  await ensureAuth();
  const key = `recent_calls:${repEmail}:${days}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        sc.event_id,
        sc.event_start,
        sc.call_title,
        sc.platform,
        sc.call_duration_minutes,
        sc.call_disposition,
        sc.has_transcript,
        sc.transcript_summary.text AS summary_text
      FROM \`shopify-dw.sales.sales_calls\` sc,
        UNNEST(sc.attendee_details) AS a
      WHERE a.is_shopify_employee = TRUE
        AND LOWER(a.attendee_email) = '${repEmail.toLowerCase()}'
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ORDER BY sc.event_start DESC
      LIMIT 30`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Transcript details for MEDDIC scoring ---

export async function fetchTranscriptDetails(repEmail, limit = 5, forceRefresh = false) {
  await ensureAuth();
  const key = `transcripts:${repEmail}:${limit}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        sc.event_id,
        sc.event_start,
        sc.call_duration_minutes,
        sc.transcript_summary.text AS summary_text,
        sc.transcript_details
      FROM \`shopify-dw.sales.sales_calls\` sc,
        UNNEST(sc.attendee_details) AS a
      WHERE a.is_shopify_employee = TRUE
        AND LOWER(a.attendee_email) = '${repEmail.toLowerCase()}'
        AND sc.has_transcript = TRUE
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      ORDER BY sc.event_start DESC
      LIMIT ${limit}`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- All reps aggregated data (manager view) ---

export async function fetchAllRepsPipeline(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_pipeline:${team}` : 'all_reps_pipeline';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        o.opportunity_id,
        o.name AS opp_name,
        o.current_stage_name,
        o.forecast_category,
        o.close_date,
        o.next_step,
        o.is_closed,
        o.is_won,
        o.merchant_intent,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      ORDER BY o.salesforce_owner_name, pbr DESC`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsWonPBR(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_won:${team}` : 'all_reps_won';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsCallActivity(days = 7, team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_calls:${days}:${team}` : `all_reps_calls:${days}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        LOWER(a.attendee_email) AS rep_email,
        COUNT(DISTINCT sc.event_id) AS total_interactions,
        COUNTIF(sc.platform = 'salesloft_dialer') AS dialer_calls,
        COUNTIF(sc.platform IN ('google_meet','salesloft_conversation','salesloft')) AS meetings,
        COUNTIF(sc.call_disposition = 'Connected') AS connected_calls,
        COUNTIF(sc.has_transcript) AS transcribed
      FROM \`shopify-dw.sales.sales_calls\` sc,
        UNNEST(sc.attendee_details) AS a
      WHERE a.is_shopify_employee = TRUE
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      GROUP BY rep_email`;
    const rows = await query(sql);
    const teamEmails = new Set(
      Object.entries(REP_ROSTER)
        .filter(([, r]) => !team || r.team === team)
        .map(([email]) => email)
    );
    return rows.filter(r => teamEmails.has(r.rep_email));
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Email activity summary (single rep) ---

export async function fetchEmailActivity(repEmail, days = 7, forceRefresh = false) {
  await ensureAuth();
  const key = `emails:${repEmail}:${days}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        COUNT(*) AS total_emails,
        COUNTIF(is_inbound = FALSE) AS outbound_emails,
        COUNTIF(is_inbound = TRUE) AS inbound_replies,
        COUNTIF(opens_count > 0 AND is_inbound = FALSE) AS opened,
        COUNTIF(clicks_count > 0 AND is_inbound = FALSE) AS clicked,
        SAFE_DIVIDE(COUNTIF(opens_count > 0 AND is_inbound = FALSE), COUNTIF(is_inbound = FALSE)) AS open_rate,
        SAFE_DIVIDE(COUNTIF(is_inbound = TRUE), COUNTIF(is_inbound = FALSE)) AS reply_rate
      FROM \`shopify-dw.sales.sales_emails\`
      WHERE LOWER(from_email_address) = '${repEmail.toLowerCase()}'
        AND DATE(delivered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)`;
    const rows = await query(sql);
    return rows.length ? rows[0] : { total_emails: 0, outbound_emails: 0, inbound_replies: 0, opened: 0, clicked: 0, open_rate: null, reply_rate: null };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Email activity: all reps (manager view) ---

export async function fetchAllRepsEmailActivity(days = 7, team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_emails:${days}:${team}` : `all_reps_emails:${days}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        LOWER(from_email_address) AS rep_email,
        COUNT(*) AS total_emails,
        COUNTIF(is_inbound = FALSE) AS outbound_emails,
        COUNTIF(is_inbound = TRUE) AS inbound_replies,
        COUNTIF(opens_count > 0 AND is_inbound = FALSE) AS opened,
        COUNTIF(clicks_count > 0 AND is_inbound = FALSE) AS clicked,
        SAFE_DIVIDE(COUNTIF(opens_count > 0 AND is_inbound = FALSE), COUNTIF(is_inbound = FALSE)) AS open_rate,
        SAFE_DIVIDE(COUNTIF(is_inbound = TRUE), COUNTIF(is_inbound = FALSE)) AS reply_rate
      FROM \`shopify-dw.sales.sales_emails\`
      WHERE DATE(delivered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      GROUP BY rep_email`;
    const rows = await query(sql);
    const teamEmails = new Set(
      Object.entries(REP_ROSTER)
        .filter(([, r]) => !team || r.team === team)
        .map(([email]) => email)
    );
    return rows.filter(r => teamEmails.has(r.rep_email));
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Email activity: weekly trending ---

export async function fetchEmailTrending(repEmail, weeks = 8, forceRefresh = false) {
  await ensureAuth();
  const isAll = repEmail === '__all__';
  const key = `email_trend:${repEmail}:${weeks}`;
  const fetchFn = async () => {
    const emailFilter = isAll
      ? ''
      : `AND LOWER(from_email_address) = '${repEmail.toLowerCase()}'`;
    const sql = `
      SELECT
        ${isAll ? 'LOWER(from_email_address) AS rep_email,' : ''}
        FORMAT_DATE('%G-W%V', DATE(delivered_at)) AS week,
        COUNT(*) AS total,
        COUNTIF(is_inbound = FALSE) AS outbound,
        COUNTIF(is_inbound = TRUE) AS inbound,
        COUNTIF(opens_count > 0 AND is_inbound = FALSE) AS opened
      FROM \`shopify-dw.sales.sales_emails\`
      WHERE DATE(delivered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${weeks * 7} DAY)
        ${emailFilter}
      GROUP BY ${isAll ? 'rep_email, ' : ''}week
      ORDER BY week`;
    const rows = await query(sql);
    if (isAll) {
      const teamEmails = new Set(Object.keys(REP_ROSTER));
      return rows.filter(r => teamEmails.has(r.rep_email));
    }
    return rows;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Health Check: Deal hygiene flags ---

export async function fetchDealHygieneFlags(sfOwnerName, team = null, forceRefresh = false) {
  await ensureAuth();
  const isAll = sfOwnerName === '__all__';
  const key = `hygiene:${sfOwnerName}${team ? ':' + team : ''}`;
  const fetchFn = async () => {
    const ownerFilter = isAll
      ? `AND o.salesforce_owner_name IN ${teamRepSubquery(team)}`
      : `AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'`;
    const sql = `
      SELECT
        o.salesforce_owner_name,
        o.opportunity_id,
        o.name AS opp_name,
        o.current_stage_name,
        o.forecast_category,
        o.close_date,
        o.next_step,
        o.is_closed,
        o.is_won,
        o.updated_at,
        DATE(raw.CreatedDate) AS created_date,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        ${ownerFilter}
        AND o.is_closed = FALSE
        AND o.is_won = FALSE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      ORDER BY pbr DESC`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Health Check: Activity trending (weekly breakdown) ---

export async function fetchActivityTrending(repEmail, weeks = 8, forceRefresh = false) {
  await ensureAuth();
  const isAll = repEmail === '__all__';
  const key = `activity_trend:${repEmail}:${weeks}`;
  const fetchFn = async () => {
    const emailFilter = isAll
      ? '' // no email filter — will filter client-side to team
      : `AND LOWER(a.attendee_email) = '${repEmail.toLowerCase()}'`;
    const sql = `
      SELECT
        ${isAll ? 'LOWER(a.attendee_email) AS rep_email,' : ''}
        FORMAT_DATE('%G-W%V', DATE(sc.event_start)) AS week,
        COUNT(DISTINCT sc.event_id) AS total,
        COUNTIF(sc.platform IN ('google_meet','salesloft_conversation','salesloft')) AS meetings,
        COUNTIF(sc.platform = 'salesloft_dialer') AS dialer,
        COUNTIF(sc.call_disposition = 'Connected') AS connected,
        COUNTIF(sc.has_transcript) AS transcribed
      FROM \`shopify-dw.sales.sales_calls\` sc,
        UNNEST(sc.attendee_details) AS a
      WHERE a.is_shopify_employee = TRUE
        ${emailFilter}
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${weeks * 7} DAY)
      GROUP BY ${isAll ? 'rep_email, ' : ''}week
      ORDER BY week`;
    const rows = await query(sql);
    if (isAll) {
      const teamEmails = new Set(Object.keys(REP_ROSTER));
      return rows.filter(r => teamEmails.has(r.rep_email));
    }
    return rows;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Coaching: Single call transcript for per-call MEDDIC ---

export async function fetchCallWithTranscript(eventId, forceRefresh = false) {
  await ensureAuth();
  const key = `call_transcript:${eventId}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        sc.event_id,
        sc.event_start,
        sc.call_title,
        sc.call_duration_minutes,
        sc.platform,
        sc.call_disposition,
        sc.transcript_summary.text AS summary_text,
        sc.transcript_details
      FROM \`shopify-dw.sales.sales_calls\` sc
      WHERE sc.event_id = '${eventId.replace(/'/g, "\\'")}'
      LIMIT 1`;
    const rows = await query(sql);
    return rows.length ? rows[0] : null;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Coaching: All reps activity comparison ---

export async function fetchRepActivityComparison(days = 7, forceRefresh = false) {
  await ensureAuth();
  const key = `rep_comparison:${days}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        LOWER(a.attendee_email) AS rep_email,
        COUNT(DISTINCT sc.event_id) AS total,
        COUNTIF(sc.platform IN ('google_meet','salesloft_conversation','salesloft')) AS meetings,
        COUNTIF(sc.call_disposition = 'Connected') AS connected,
        COUNTIF(sc.has_transcript) AS transcribed,
        ROUND(AVG(IF(sc.call_duration_minutes > 0, sc.call_duration_minutes, NULL)), 1) AS avg_duration
      FROM \`shopify-dw.sales.sales_calls\` sc,
        UNNEST(sc.attendee_details) AS a
      WHERE a.is_shopify_employee = TRUE
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      GROUP BY rep_email`;
    const rows = await query(sql);
    const teamEmails = new Set(Object.keys(REP_ROSTER));
    return rows.filter(r => teamEmails.has(r.rep_email));
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- AI Toolkit: Merchant context for AI tools ---

export async function fetchMerchantContext(sfOwnerName, oppName, forceRefresh = false) {
  await ensureAuth();
  const key = `merchant:${sfOwnerName}:${oppName}`;
  const fetchFn = async () => {
    // Get deal details
    const dealSql = `
      SELECT
        o.opportunity_id, o.name AS opp_name, o.current_stage_name,
        o.forecast_category, o.close_date, o.next_step,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'
        AND o.name = '${oppName.replace(/'/g, "\\'")}'
      LIMIT 1`;
    const deal = await query(dealSql);
    return { deal: deal[0] || null };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Quota: Dynamic from BQ (rep + coach) ---

export async function fetchQuotas(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `quotas:${team}` : 'quotas:all';
  const fetchFn = async () => {
    const rolePattern = team
      ? `AMER-SALES-%-SMB-ALL-X-ALL-D2CRETAIL${team.replace('D2CRETAIL', '')}`
      : 'AMER-SALES-%-SMB-ALL-X-ALL-D2CRETAIL%';
    // Note: REPLACE normalizes curly apostrophe (U+2019 \u2019) to straight quote
    // because worker_current uses \u2019 but sales_users_daily_snapshot uses '
    const sql = `
      SELECT
        u.name,
        u.user_role,
        CASE WHEN u.user_role LIKE '%LEAD%' THEN 'coach' ELSE 'rep' END AS role_type,
        SUM(q.amount) AS quarterly_quota
      FROM \`shopify-dw.sales.sales_users_daily_snapshot\` u
      JOIN \`shopify-dw.people.worker_current\` w
        ON LOWER(REPLACE(u.name, "'", "\u2019")) = LOWER(w.worker_full_name)
      JOIN \`shopify-dw.people.incentive_compensation_monthly_quotas\` q
        ON w.worker_id = q.worker_id
      WHERE u.date = (SELECT MAX(date) FROM \`shopify-dw.sales.sales_users_daily_snapshot\`)
        AND u.is_active = TRUE
        AND u.user_role LIKE '${rolePattern}'
        AND q.metric = 'billed_revenue'
        AND q.month BETWEEN '${QUARTER.start}' AND '${QUARTER.end}'
      GROUP BY u.name, u.user_role, role_type
      ORDER BY role_type, u.name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Forecast: Snapshot management (quick.db, not BQ) ---

const forecastSnapshots = quick.db.collection('hub_forecast_snapshots');

export async function saveForecastSnapshot(data, weekLabel) {
  const key = data.viewMode === 'team' ? `${weekLabel}:team` : `${weekLabel}:${data.sfName || 'unknown'}`;
  // Idempotent: skip if already saved this week
  try {
    const existing = await forecastSnapshots.where({ key }).limit(1).find();
    if (existing.length) return;
  } catch (_) {}

  try {
    await forecastSnapshots.create({
      key,
      week: weekLabel,
      entity: data.viewMode === 'team' ? 'team' : (data.sfName || 'unknown'),
      data: JSON.stringify(data.snapshotPayload),
      created_at: new Date().toISOString(),
    });
  } catch (_) {}
}

export async function getForecastSnapshots(entity, weeksBack = 8) {
  try {
    const items = await forecastSnapshots
      .where({ entity })
      .orderBy('created_at', 'desc')
      .limit(weeksBack)
      .find();
    return items.map(i => ({ week: i.week, ...JSON.parse(i.data) }));
  } catch (_) {
    return [];
  }
}

// --- Weekly won PBR for revenue trend chart ---

export async function fetchWeeklyWonPBR(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `weekly_won:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        FORMAT_DATE('%G-W%V', o.close_date) AS week,
        SUM(raw.Projected_Billed_Revenue__c) AS weekly_won,
        COUNT(DISTINCT o.opportunity_id) AS deal_count
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON raw.Id = o.opportunity_id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      GROUP BY week
      ORDER BY week`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- All reps weekly won PBR (for revenue trend team avg) ---

export async function fetchAllRepsWeeklyWonPBR(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_weekly_won:${team}` : 'all_weekly_won';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        FORMAT_DATE('%G-W%V', o.close_date) AS week,
        SUM(raw.Projected_Billed_Revenue__c) AS weekly_won,
        COUNT(DISTINCT o.opportunity_id) AS deal_count
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON raw.Id = o.opportunity_id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      GROUP BY o.salesforce_owner_name, week
      ORDER BY week`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Next-quarter open pipeline (for Outlook sub-tab) ---

export async function fetchNextQuarterPipeline(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const isAll = sfOwnerName === '__all__';
  const key = `next_quarter_pipeline:${sfOwnerName}`;
  const fetchFn = async () => {
    const ownerFilter = isAll
      ? `AND o.salesforce_owner_name IN ${teamRepSubquery()}`
      : `AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'`;
    const sql = `
      SELECT
        o.salesforce_owner_name,
        o.opportunity_id,
        o.name AS opp_name,
        o.current_stage_name,
        o.forecast_category,
        o.close_date,
        o.next_step,
        o.is_closed,
        o.is_won,
        o.updated_at,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        ${ownerFilter}
        AND o.is_closed = FALSE
        AND o.is_won = FALSE
        AND o.close_date >= '${NEXT_QUARTER.start}'
        AND o.close_date <= '${NEXT_QUARTER.end}'
      ORDER BY pbr DESC`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Next-quarter quotas (for Outlook sub-tab) ---

export async function fetchNextQuarterQuotas(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `next_quotas:${team}` : 'next_quotas:all';
  const fetchFn = async () => {
    const rolePattern = team
      ? `AMER-SALES-%-SMB-ALL-X-ALL-D2CRETAIL${team.replace('D2CRETAIL', '')}`
      : 'AMER-SALES-%-SMB-ALL-X-ALL-D2CRETAIL%';
    const sql = `
      SELECT
        u.name,
        u.user_role,
        CASE WHEN u.user_role LIKE '%LEAD%' THEN 'coach' ELSE 'rep' END AS role_type,
        SUM(q.amount) AS quarterly_quota
      FROM \`shopify-dw.sales.sales_users_daily_snapshot\` u
      JOIN \`shopify-dw.people.worker_current\` w
        ON LOWER(REPLACE(u.name, "'", "\u2019")) = LOWER(w.worker_full_name)
      JOIN \`shopify-dw.people.incentive_compensation_monthly_quotas\` q
        ON w.worker_id = q.worker_id
      WHERE u.date = (SELECT MAX(date) FROM \`shopify-dw.sales.sales_users_daily_snapshot\`)
        AND u.is_active = TRUE
        AND u.user_role LIKE '${rolePattern}'
        AND q.metric = 'billed_revenue'
        AND q.month BETWEEN '${NEXT_QUARTER.start}' AND '${NEXT_QUARTER.end}'
      GROUP BY u.name, u.user_role, role_type
      ORDER BY role_type, u.name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Next-quarter won PBR (for Outlook sub-tab) ---

export async function fetchNextQuarterWonPBR(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `next_won:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT
        COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'
        AND o.is_won = TRUE
        AND o.close_date >= '${NEXT_QUARTER.start}'
        AND o.close_date <= '${NEXT_QUARTER.end}'`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].won_pbr) : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- All reps next-quarter won PBR (for Outlook team view) ---

export async function fetchAllRepsNextQuarterWonPBR(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_next_won:${team}` : 'all_next_won';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw
        ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND o.is_won = TRUE
        AND o.close_date >= '${NEXT_QUARTER.start}'
        AND o.close_date <= '${NEXT_QUARTER.end}'
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Submitted forecast commit from Salesforce (modelled_salesforce_forecast) ---

export async function fetchForecastCommit(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `forecast_commit:${sfOwnerName}`;
  const fetchFn = async () => {
    // Rep-level: SUM of CommitForecast rows for this rep.
    // Coaches submit on behalf of reps (is_coach_forecast = TRUE on REP role rows).
    // Rep's own submissions would be is_coach_forecast = FALSE — currently none exist.
    const sql = `
      SELECT
        COALESCE(SUM(f.value), 0) AS total_commit
      FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\` f
      WHERE f.forecast_owner_rep_role LIKE '%AMER-SALES-REP-SMB-ALL-X-ALL-D2CRETAIL%'
        AND f.measure = 'Closed Won'
        AND f.value_type = 'Projected Billed Revenue'
        AND f.forecast_item_category = 'CommitForecast'
        AND f.is_coach_forecast = TRUE
        AND f.period_end_date = '${QUARTER.end}'
        AND f.owner_name = '${sfOwnerName.replace(/'/g, "\\'")}'
        AND f.source_timestamp = (
          SELECT MAX(source_timestamp)
          FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\`
          WHERE period_end_date = '${QUARTER.end}'
            AND forecast_owner_rep_role LIKE '%AMER-SALES-REP-SMB-ALL-X-ALL-D2CRETAIL%'
            AND forecast_item_category = 'CommitForecast'
            AND is_coach_forecast = TRUE
        )`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].total_commit) || 0 : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsForecastCommit(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_forecast_commit:${team}` : 'all_forecast_commit';
  const fetchFn = async () => {
    // Coach-level forecast: LEAD role, is_coach_forecast = FALSE (coach's own submission as forecast owner).
    const leadPattern = team
      ? `AMER-SALES-LEAD-SMB-ALL-X-ALL-D2CRETAIL${team.replace('D2CRETAIL', '')}`
      : 'AMER-SALES-LEAD-SMB-ALL-X-ALL-D2CRETAIL%';
    const sql = `
      SELECT
        f.owner_name,
        f.forecast_owner_rep_role,
        SUM(f.value) AS total_commit
      FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\` f
      WHERE f.forecast_owner_rep_role LIKE '%${leadPattern}%'
        AND f.measure = 'Closed Won'
        AND f.value_type = 'Projected Billed Revenue'
        AND f.forecast_item_category = 'CommitForecast'
        AND f.is_coach_forecast = FALSE
        AND f.period_end_date = '${QUARTER.end}'
        AND f.source_timestamp = (
          SELECT MAX(source_timestamp)
          FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\`
          WHERE period_end_date = '${QUARTER.end}'
            AND forecast_owner_rep_role LIKE '%${leadPattern}%'
            AND forecast_item_category = 'CommitForecast'
            AND is_coach_forecast = FALSE
        )
      GROUP BY f.owner_name, f.forecast_owner_rep_role`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Per-rep forecast details (commit + last submission timestamp) ---

export async function fetchAllRepsForecastDetails(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `rep_forecast_details:${team}` : 'rep_forecast_details';
  const fetchFn = async () => {
    const teamSuffix = team ? team.replace('D2CRETAIL', '') : '%';
    const rolePattern = `AMER-SALES-REP-SMB-ALL-X-ALL-D2CRETAIL${teamSuffix}`;
    // Coach-submitted forecasts for each rep (is_coach_forecast = TRUE on REP role).
    // Also check if a rep-submitted row exists (is_coach_forecast = FALSE) to flag submitter.
    const sql = `
      WITH latest AS (
        SELECT MAX(source_timestamp) AS ts
        FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\`
        WHERE period_end_date = '${QUARTER.end}'
          AND forecast_owner_rep_role LIKE '%${rolePattern}%'
          AND forecast_item_category = 'CommitForecast'
      )
      SELECT
        coach.owner_name,
        COALESCE(SUM(coach.value), 0) AS commit_value,
        MAX(coach.last_modified_date) AS last_submitted,
        MAX(CASE WHEN rep.owner_name IS NOT NULL THEN TRUE ELSE FALSE END) AS rep_submitted
      FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\` coach
      CROSS JOIN latest
      LEFT JOIN \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\` rep
        ON rep.owner_name = coach.owner_name
        AND rep.period_end_date = '${QUARTER.end}'
        AND rep.forecast_owner_rep_role LIKE '%${rolePattern}%'
        AND rep.forecast_item_category = 'CommitForecast'
        AND rep.is_coach_forecast = FALSE
        AND rep.source_timestamp = latest.ts
        AND rep.measure = 'Closed Won'
        AND rep.value_type = 'Projected Billed Revenue'
      WHERE coach.forecast_owner_rep_role LIKE '%${rolePattern}%'
        AND coach.measure = 'Closed Won'
        AND coach.value_type = 'Projected Billed Revenue'
        AND coach.period_end_date = '${QUARTER.end}'
        AND coach.is_coach_forecast = TRUE
        AND coach.forecast_item_category = 'CommitForecast'
        AND coach.source_timestamp = latest.ts
      GROUP BY coach.owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function cleanupOldSnapshots(maxWeeks = 13) {
  try {
    const all = await forecastSnapshots.orderBy('created_at', 'asc').find();
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - maxWeeks * 7);
    for (const item of all) {
      if (new Date(item.created_at) < cutoff) {
        await forecastSnapshots.delete(item._id);
      }
    }
  } catch (_) {}
}

// --- Q2 Standards: conversion metrics (won count, avg PBR, time to win, win rate, outbound mix) ---
// NOTE: outbound mix uses Source_Most_Recent__c on raw_salesforce_banff.opportunity.
// Values treated as outbound: 'AE PROSPECTING', 'OUTBOUND', 'COLD OUTREACH', 'SELF-SOURCED', 'AE SOURCED'.
// Run `SELECT DISTINCT UPPER(TRIM(Source_Most_Recent__c)) FROM raw_salesforce_banff.opportunity LIMIT 100`
// to confirm/adjust these values against your actual Salesforce data.

export async function fetchStandardsMetrics(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `standards_v6:${sfOwnerName}`;
  const fetchFn = async () => {
    const safeName = sfOwnerName.replace(/'/g, "\\'");
    const sql = `
      WITH won AS (
        SELECT
          COUNT(*)                                                AS won_count,
          SUM(sf.Projected_Billed_Revenue__c)                     AS won_pbr,
          AVG(DATE_DIFF(o.close_date, DATE(sf.CreatedDate), DAY)) AS avg_time_to_win,
          COUNTIF(sf.LeadSource = 'Outbound')                     AS outbound_won
        FROM \`shopify-dw.sales.sales_opportunities\` o
        JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
        WHERE o.record_type = 'Sales'
          AND o.salesforce_owner_name = '${safeName}'
          AND sf.Type = 'Existing Business'
          AND sf.IsDeleted = FALSE
          AND o.is_won = TRUE
          AND o.close_date >= '${QUARTER.start}'
          AND o.close_date <= '${QUARTER.end}'
      ),
      closed_lost AS (
        SELECT COUNT(*) AS lost_count
        FROM \`shopify-dw.sales.sales_opportunities\` o
        JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
        WHERE o.record_type = 'Sales'
          AND o.salesforce_owner_name = '${safeName}'
          AND sf.Type = 'Existing Business'
          AND sf.IsDeleted = FALSE
          AND o.is_won = FALSE
          AND o.forecast_category = 'Omitted'
          AND o.close_date >= '${QUARTER.start}'
          AND o.close_date <= '${QUARTER.end}'
      )
      SELECT
        w.won_count,
        w.won_pbr,
        w.avg_time_to_win,
        cl.lost_count,
        SAFE_DIVIDE(w.won_count, w.won_count + cl.lost_count) AS win_rate,
        SAFE_DIVIDE(w.outbound_won, w.won_count)              AS outbound_mix
      FROM won w, closed_lost cl`;
    const rows = await query(sql);
    return rows.length ? rows[0] : {
      won_count: 0, won_pbr: 0, avg_time_to_win: null, lost_count: 0, win_rate: null, outbound_mix: null,
    };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Q2 Standards: opps created in the current ISO week ---

export async function fetchOppsCreated(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `opps_created_v5:${sfOwnerName}`;
  const fetchFn = async () => {
    const safeName = sfOwnerName.replace(/'/g, "\\'");
    const sql = `
      SELECT COUNT(*) AS opps_created
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
      WHERE o.record_type = 'Sales'
        AND o.salesforce_owner_name = '${safeName}'
        AND sf.Type = 'Existing Business'
        AND sf.IsDeleted = FALSE
        AND DATE(sf.CreatedDate) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].opps_created) || 0 : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Q2 Standards: opps created this week, all reps ---

export async function fetchAllRepsOppsCreated(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_opps_created:${team}` : 'all_reps_opps_created';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        COUNT(*) AS opps_created
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
      WHERE o.record_type = 'Sales'
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND sf.Type = 'Existing Business'
        AND sf.IsDeleted = FALSE
        AND DATE(sf.CreatedDate) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// --- Q2 Standards: team-wide avg time-to-win + outbound mix in one query ---
// Returns array keyed by salesforce_owner_name (only metrics not derivable from pipeline data).

export async function fetchAllRepsStandardsMetrics(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_standards_v6:${team}` : 'all_reps_standards_v6';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        COUNT(*)                                                    AS won_count,
        AVG(DATE_DIFF(o.close_date, DATE(sf.CreatedDate), DAY))     AS avg_time_to_win,
        SAFE_DIVIDE(COUNTIF(sf.LeadSource = 'Outbound'), COUNT(*))  AS outbound_mix
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
      WHERE o.record_type = 'Sales'
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND sf.Type = 'Existing Business'
        AND sf.IsDeleted = FALSE
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}
