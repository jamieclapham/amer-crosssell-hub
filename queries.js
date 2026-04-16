// Coaching Hub — BigQuery Queries + Caching
// UNIFIED DATA LAYER: Combines CRM tables (post-Apr 10) + DW tables (historical + GMeet)
//
// Call data: sales_calls (GMeet + historical SL) UNION base__crm_calls (Twilio dialer, CRM native)
// Email data: sales_emails (historical + Mozart) UNION base__crm_emails (CRM native sequences)
// Transcripts: sales_calls.transcript_details (GMeet) + sales_calls.transcript_summary.text
// User resolution: sdp-ingest-snapshots-prod.unicorn.users (CRM user → name/email)
//
// All queries verified against live BQ INFORMATION_SCHEMA on 2026-04-16.
// Column names are exact. No guessing.

import { QUARTER, NEXT_QUARTER, REP_ROSTER, ROLE_PATTERN } from './config.js';

// ─── Helpers ───

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

function esc(s) { return (s || '').replace(/'/g, "\\'"); }

// ─── Cache layer (quick.db, 1hr TTL) ───

const cache = quick.db.collection('coaching_hub_cache');
const CACHE_TTL_MS = 1 * 60 * 60 * 1000;

let authDone = false;

async function ensureAuth() {
  if (authDone) return;
  await quick.auth.requestScopes(['https://www.googleapis.com/auth/bigquery']);
  authDone = true;
}

async function query(sql) {
  const res = await quick.dw.querySync(sql, [], { timeoutMs: 120000 });
  return res.results || [];
}

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
  } catch (_) { /* ignore write errors */ }
  return data;
}

export async function clearCache() {
  try {
    const all = await cache.find();
    for (const item of all) await cache.delete(item._id);
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════
//  PIPELINE & REVENUE QUERIES (same as Jamie's — proven correct)
// ═══════════════════════════════════════════════════════════════

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
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'
      ORDER BY pbr DESC`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchWonPBR(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `won:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales'
        AND raw.Type = 'Existing Business'
        AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}'
        AND o.close_date <= '${QUARTER.end}'`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].won_pbr) : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

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
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
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
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
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

// ═══════════════════════════════════════════════════════════════
//  UNIFIED CALL ACTIVITY (sales_calls + base__crm_calls)
//  Fixes Jamie's issue #2: includes Twilio CRM-native calls
// ═══════════════════════════════════════════════════════════════

// Helper: CRM user email subquery (unicorn.users → email for crm_calls.user_id)
function crmUserEmailCTE() {
  return `crm_users AS (
    SELECT id, LOWER(email) AS email, name
    FROM \`sdp-ingest-snapshots-prod.unicorn.users\`
    WHERE NOT longboat_is_deleted
  )`;
}

export async function fetchCallActivity(repEmail, days = 7, forceRefresh = false) {
  await ensureAuth();
  const key = `calls_unified:${repEmail}:${days}`;
  const lowerEmail = repEmail.toLowerCase();
  const fetchFn = async () => {
    // UNION: sales_calls (GMeet + SL) + base__crm_calls (Twilio CRM)
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_calls AS (
        SELECT
          CAST(sc.event_id AS STRING) AS call_id,
          sc.platform,
          sc.call_disposition AS disposition,
          sc.has_transcript,
          sc.call_duration_minutes AS duration_min,
          'dw' AS source_table
        FROM \`shopify-dw.sales.sales_calls\` sc,
          UNNEST(sc.attendee_details) AS a
        WHERE a.is_shopify_employee = TRUE
          AND LOWER(a.attendee_email) = '${lowerEmail}'
          AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      crm_calls AS (
        SELECT
          CAST(cc.crm_call_id AS STRING) AS call_id,
          COALESCE(cc.platform, cc.source) AS platform,
          cc.disposition,
          cc.has_transcript,
          ROUND(cc.duration_seconds / 60.0, 1) AS duration_min,
          'crm' AS source_table
        FROM \`shopify-dw.base.base__crm_calls\` cc
        JOIN crm_users cu ON cc.user_id = cu.id
        WHERE cu.email = '${lowerEmail}'
          AND DATE(cc.occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
          AND NOT cc.longboat_is_deleted
      ),
      all_calls AS (
        SELECT * FROM dw_calls
        UNION ALL
        SELECT * FROM crm_calls
      )
      SELECT
        COUNT(*) AS total_interactions,
        COUNTIF(platform IN ('twilio_1p_crm', 'twilio', 'salesloft_dialer')) AS dialer_calls,
        COUNTIF(platform IN ('google_meet', 'salesloft_conversation', 'salesloft')) AS meetings,
        COUNTIF(LOWER(disposition) = 'connected') AS connected_calls,
        COUNTIF(has_transcript) AS transcribed,
        ROUND(AVG(IF(duration_min > 0, duration_min, NULL)), 1) AS avg_duration_min
      FROM all_calls`;
    const rows = await query(sql);
    return rows.length ? rows[0] : { total_interactions: 0, dialer_calls: 0, meetings: 0, connected_calls: 0, transcribed: 0, avg_duration_min: 0 };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsCallActivity(days = 7, team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_calls_unified:${days}:${team}` : `all_reps_calls_unified:${days}`;
  const fetchFn = async () => {
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_calls AS (
        SELECT
          LOWER(a.attendee_email) AS rep_email,
          CAST(sc.event_id AS STRING) AS call_id,
          sc.platform,
          sc.call_disposition AS disposition,
          sc.has_transcript,
          sc.call_duration_minutes AS duration_min
        FROM \`shopify-dw.sales.sales_calls\` sc,
          UNNEST(sc.attendee_details) AS a
        WHERE a.is_shopify_employee = TRUE
          AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      crm_calls AS (
        SELECT
          cu.email AS rep_email,
          CAST(cc.crm_call_id AS STRING) AS call_id,
          COALESCE(cc.platform, cc.source) AS platform,
          cc.disposition,
          cc.has_transcript,
          ROUND(cc.duration_seconds / 60.0, 1) AS duration_min
        FROM \`shopify-dw.base.base__crm_calls\` cc
        JOIN crm_users cu ON cc.user_id = cu.id
        WHERE DATE(cc.occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
          AND NOT cc.longboat_is_deleted
      ),
      all_calls AS (
        SELECT * FROM dw_calls
        UNION ALL
        SELECT * FROM crm_calls
      )
      SELECT
        rep_email,
        COUNT(*) AS total_interactions,
        COUNTIF(platform IN ('twilio_1p_crm', 'twilio', 'salesloft_dialer')) AS dialer_calls,
        COUNTIF(platform IN ('google_meet', 'salesloft_conversation', 'salesloft')) AS meetings,
        COUNTIF(LOWER(disposition) = 'connected') AS connected_calls,
        COUNTIF(has_transcript) AS transcribed
      FROM all_calls
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

// ═══════════════════════════════════════════════════════════════
//  RECENT CALLS + TRANSCRIPTS (for coaching tab)
//  Uses sales_calls for GMeet (has transcript_summary.text)
//  + base__crm_calls for Twilio dialer calls (no summary, but has duration/disposition)
// ═══════════════════════════════════════════════════════════════

export async function fetchRecentCalls(repEmail, days = 14, forceRefresh = false) {
  await ensureAuth();
  const key = `recent_calls_unified:${repEmail}:${days}`;
  const lowerEmail = repEmail.toLowerCase();
  const fetchFn = async () => {
    // Primary source: sales_calls (has transcript summaries)
    // Secondary: crm_calls (Twilio dialer — no summary but captures dialer activity)
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_calls AS (
        SELECT
          CAST(sc.event_id AS STRING) AS call_id,
          sc.event_start AS occurred_at,
          sc.call_title AS title,
          sc.platform,
          sc.call_duration_minutes AS duration_min,
          sc.call_disposition AS disposition,
          sc.has_transcript,
          sc.transcript_summary.text AS summary_text,
          'dw' AS source_table
        FROM \`shopify-dw.sales.sales_calls\` sc,
          UNNEST(sc.attendee_details) AS a
        WHERE a.is_shopify_employee = TRUE
          AND LOWER(a.attendee_email) = '${lowerEmail}'
          AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      crm_calls AS (
        SELECT
          CAST(cc.crm_call_id AS STRING) AS call_id,
          cc.occurred_at,
          cc.title,
          COALESCE(cc.platform, cc.source) AS platform,
          ROUND(cc.duration_seconds / 60.0, 1) AS duration_min,
          cc.disposition,
          cc.has_transcript,
          CAST(NULL AS STRING) AS summary_text,
          'crm' AS source_table
        FROM \`shopify-dw.base.base__crm_calls\` cc
        JOIN crm_users cu ON cc.user_id = cu.id
        WHERE cu.email = '${lowerEmail}'
          AND DATE(cc.occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
          AND NOT cc.longboat_is_deleted
      )
      SELECT * FROM dw_calls
      UNION ALL
      SELECT * FROM crm_calls
      ORDER BY occurred_at DESC
      LIMIT 30`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchTranscriptDetails(repEmail, limit = 5, forceRefresh = false) {
  await ensureAuth();
  const key = `transcripts:${repEmail}:${limit}`;
  const lowerEmail = repEmail.toLowerCase();
  const fetchFn = async () => {
    // Only sales_calls has transcript_details (GMeet transcripts)
    // Jayson Brown's approach: use these transcripts to recreate SL-style summaries
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
        AND LOWER(a.attendee_email) = '${lowerEmail}'
        AND sc.has_transcript = TRUE
        AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
      ORDER BY sc.event_start DESC
      LIMIT ${limit}`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

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
      WHERE sc.event_id = '${esc(eventId)}'
      LIMIT 1`;
    const rows = await query(sql);
    return rows.length ? rows[0] : null;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  UNIFIED EMAIL ACTIVITY (sales_emails + base__crm_emails)
//  Fixes Jamie's issue #3: includes CRM-native sequence emails
// ═══════════════════════════════════════════════════════════════

export async function fetchEmailActivity(repEmail, days = 7, forceRefresh = false) {
  await ensureAuth();
  const key = `emails_unified:${repEmail}:${days}`;
  const lowerEmail = repEmail.toLowerCase();
  const fetchFn = async () => {
    // UNION: sales_emails (SL + Mozart + SF) + base__crm_emails (CRM native sequences)
    // CRM filter: source IN ('crm') AND step_execution_id IS NOT NULL = intentional sequence emails
    // Also include source='crm' one-off emails (step_execution_id IS NULL but source='crm')
    // Exclude gmail noise (source='gmail' is 73% of crm_emails volume — auto-replies, calendar)
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_emails AS (
        SELECT
          CAST(e.email_id AS STRING) AS eid,
          CASE WHEN e.is_inbound THEN 'inbound' ELSE 'outbound' END AS direction,
          e.opens_count,
          e.clicks_count,
          'dw' AS src
        FROM \`shopify-dw.sales.sales_emails\` e
        WHERE LOWER(e.from_email_address) = '${lowerEmail}'
          AND DATE(e.delivered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
          AND NOT COALESCE(e.is_scale_outreach, FALSE)
      ),
      crm_emails AS (
        SELECT
          CAST(ce.crm_email_id AS STRING) AS eid,
          ce.direction,
          ce.opens_count,
          ce.clicks_count,
          'crm' AS src
        FROM \`shopify-dw.base.base__crm_emails\` ce
        JOIN crm_users cu ON ce.sender_id = cu.id
        WHERE cu.email = '${lowerEmail}'
          AND ce.source IN ('crm')
          AND ce.status IN ('sent', 'delivered', 'queued')
          AND NOT ce.longboat_is_deleted
          AND DATE(COALESCE(ce.delivered_at, ce.queued_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      all_emails AS (
        SELECT * FROM dw_emails
        UNION ALL
        SELECT * FROM crm_emails
      )
      SELECT
        COUNT(*) AS total_emails,
        COUNTIF(direction = 'outbound') AS outbound_emails,
        COUNTIF(direction = 'inbound') AS inbound_replies,
        COUNTIF(opens_count > 0 AND direction = 'outbound') AS opened,
        COUNTIF(clicks_count > 0 AND direction = 'outbound') AS clicked,
        SAFE_DIVIDE(COUNTIF(opens_count > 0 AND direction = 'outbound'), COUNTIF(direction = 'outbound')) AS open_rate,
        SAFE_DIVIDE(COUNTIF(direction = 'inbound'), COUNTIF(direction = 'outbound')) AS reply_rate
      FROM all_emails`;
    const rows = await query(sql);
    return rows.length ? rows[0] : { total_emails: 0, outbound_emails: 0, inbound_replies: 0, opened: 0, clicked: 0, open_rate: null, reply_rate: null };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsEmailActivity(days = 7, team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_emails_unified:${days}:${team}` : `all_reps_emails_unified:${days}`;
  const fetchFn = async () => {
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_emails AS (
        SELECT
          LOWER(e.from_email_address) AS rep_email,
          CASE WHEN e.is_inbound THEN 'inbound' ELSE 'outbound' END AS direction,
          e.opens_count,
          e.clicks_count
        FROM \`shopify-dw.sales.sales_emails\` e
        WHERE DATE(e.delivered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
          AND NOT COALESCE(e.is_scale_outreach, FALSE)
      ),
      crm_emails AS (
        SELECT
          cu.email AS rep_email,
          ce.direction,
          ce.opens_count,
          ce.clicks_count
        FROM \`shopify-dw.base.base__crm_emails\` ce
        JOIN crm_users cu ON ce.sender_id = cu.id
        WHERE ce.source IN ('crm')
          AND ce.status IN ('sent', 'delivered', 'queued')
          AND NOT ce.longboat_is_deleted
          AND DATE(COALESCE(ce.delivered_at, ce.queued_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      all_emails AS (
        SELECT * FROM dw_emails
        UNION ALL
        SELECT * FROM crm_emails
      )
      SELECT
        rep_email,
        COUNT(*) AS total_emails,
        COUNTIF(direction = 'outbound') AS outbound_emails,
        COUNTIF(direction = 'inbound') AS inbound_replies,
        COUNTIF(opens_count > 0 AND direction = 'outbound') AS opened,
        COUNTIF(clicks_count > 0 AND direction = 'outbound') AS clicked,
        SAFE_DIVIDE(COUNTIF(opens_count > 0 AND direction = 'outbound'), COUNTIF(direction = 'outbound')) AS open_rate,
        SAFE_DIVIDE(COUNTIF(direction = 'inbound'), COUNTIF(direction = 'outbound')) AS reply_rate
      FROM all_emails
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

// ═══════════════════════════════════════════════════════════════
//  ACTIVITY TRENDING (weekly breakdown — unified calls)
// ═══════════════════════════════════════════════════════════════

export async function fetchActivityTrending(repEmail, weeks = 8, forceRefresh = false) {
  await ensureAuth();
  const isAll = repEmail === '__all__';
  const key = `activity_trend_unified:${repEmail}:${weeks}`;
  const fetchFn = async () => {
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_calls AS (
        SELECT
          LOWER(a.attendee_email) AS rep_email,
          DATE(sc.event_start) AS call_date,
          sc.platform,
          sc.call_disposition AS disposition,
          sc.has_transcript
        FROM \`shopify-dw.sales.sales_calls\` sc,
          UNNEST(sc.attendee_details) AS a
        WHERE a.is_shopify_employee = TRUE
          AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${weeks * 7} DAY)
          ${!isAll ? `AND LOWER(a.attendee_email) = '${repEmail.toLowerCase()}'` : ''}
      ),
      crm_calls AS (
        SELECT
          cu.email AS rep_email,
          DATE(cc.occurred_at) AS call_date,
          COALESCE(cc.platform, cc.source) AS platform,
          cc.disposition,
          cc.has_transcript
        FROM \`shopify-dw.base.base__crm_calls\` cc
        JOIN crm_users cu ON cc.user_id = cu.id
        WHERE NOT cc.longboat_is_deleted
          AND DATE(cc.occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${weeks * 7} DAY)
          ${!isAll ? `AND cu.email = '${repEmail.toLowerCase()}'` : ''}
      ),
      all_calls AS (
        SELECT * FROM dw_calls UNION ALL SELECT * FROM crm_calls
      )
      SELECT
        ${isAll ? 'rep_email,' : ''}
        FORMAT_DATE('%G-W%V', call_date) AS week,
        COUNT(*) AS total,
        COUNTIF(platform IN ('google_meet','salesloft_conversation','salesloft')) AS meetings,
        COUNTIF(platform IN ('twilio_1p_crm','twilio','salesloft_dialer')) AS dialer,
        COUNTIF(LOWER(disposition) = 'connected') AS connected,
        COUNTIF(has_transcript) AS transcribed
      FROM all_calls
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

// ═══════════════════════════════════════════════════════════════
//  EMAIL TRENDING (weekly breakdown — unified emails)
// ═══════════════════════════════════════════════════════════════

export async function fetchEmailTrending(repEmail, weeks = 8, forceRefresh = false) {
  await ensureAuth();
  const isAll = repEmail === '__all__';
  const key = `email_trend_unified:${repEmail}:${weeks}`;
  const fetchFn = async () => {
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_emails AS (
        SELECT
          LOWER(from_email_address) AS rep_email,
          DATE(delivered_at) AS email_date,
          CASE WHEN is_inbound THEN 'inbound' ELSE 'outbound' END AS direction,
          opens_count
        FROM \`shopify-dw.sales.sales_emails\`
        WHERE DATE(delivered_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${weeks * 7} DAY)
          AND NOT COALESCE(is_scale_outreach, FALSE)
          ${!isAll ? `AND LOWER(from_email_address) = '${repEmail.toLowerCase()}'` : ''}
      ),
      crm_emails AS (
        SELECT
          cu.email AS rep_email,
          DATE(COALESCE(ce.delivered_at, ce.queued_at)) AS email_date,
          ce.direction,
          ce.opens_count
        FROM \`shopify-dw.base.base__crm_emails\` ce
        JOIN crm_users cu ON ce.sender_id = cu.id
        WHERE ce.source IN ('crm')
          AND ce.status IN ('sent', 'delivered', 'queued')
          AND NOT ce.longboat_is_deleted
          AND DATE(COALESCE(ce.delivered_at, ce.queued_at)) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${weeks * 7} DAY)
          ${!isAll ? `AND cu.email = '${repEmail.toLowerCase()}'` : ''}
      ),
      all_emails AS (
        SELECT * FROM dw_emails UNION ALL SELECT * FROM crm_emails
      )
      SELECT
        ${isAll ? 'rep_email,' : ''}
        FORMAT_DATE('%G-W%V', email_date) AS week,
        COUNT(*) AS total,
        COUNTIF(direction = 'outbound') AS outbound,
        COUNTIF(direction = 'inbound') AS inbound,
        COUNTIF(opens_count > 0 AND direction = 'outbound') AS opened
      FROM all_emails
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

// ═══════════════════════════════════════════════════════════════
//  DEAL HYGIENE FLAGS
// ═══════════════════════════════════════════════════════════════

export async function fetchDealHygieneFlags(sfOwnerName, team = null, forceRefresh = false) {
  await ensureAuth();
  const isAll = sfOwnerName === '__all__';
  const key = `hygiene:${sfOwnerName}${team ? ':' + team : ''}`;
  const fetchFn = async () => {
    const ownerFilter = isAll
      ? `AND o.salesforce_owner_name IN ${teamRepSubquery(team)}`
      : `AND o.salesforce_owner_name = '${esc(sfOwnerName)}'`;
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
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
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

// ═══════════════════════════════════════════════════════════════
//  QUOTAS (from BQ — same as Jamie's proven query)
// ═══════════════════════════════════════════════════════════════

export async function fetchQuotas(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `quotas:${team}` : 'quotas:all';
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
        AND q.month BETWEEN '${QUARTER.start}' AND '${QUARTER.end}'
      GROUP BY u.name, u.user_role, role_type
      ORDER BY role_type, u.name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  STANDARDS METRICS (win rate, avg deal, time to win, outbound mix)
// ═══════════════════════════════════════════════════════════════

export async function fetchStandardsMetrics(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `standards:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      WITH won AS (
        SELECT
          COUNT(*) AS won_count,
          SUM(sf.Projected_Billed_Revenue__c) AS won_pbr,
          AVG(DATE_DIFF(o.close_date, DATE(sf.CreatedDate), DAY)) AS avg_time_to_win,
          COUNTIF(sf.LeadSource = 'Outbound') AS outbound_won
        FROM \`shopify-dw.sales.sales_opportunities\` o
        JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
        WHERE o.record_type = 'Sales'
          AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
          AND sf.Type = 'Existing Business' AND sf.IsDeleted = FALSE
          AND o.is_won = TRUE
          AND o.close_date >= '${QUARTER.start}' AND o.close_date <= '${QUARTER.end}'
      ),
      closed_lost AS (
        SELECT COUNT(*) AS lost_count
        FROM \`shopify-dw.sales.sales_opportunities\` o
        JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
        WHERE o.record_type = 'Sales'
          AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
          AND sf.Type = 'Existing Business' AND sf.IsDeleted = FALSE
          AND o.is_won = FALSE AND o.forecast_category = 'Omitted'
          AND o.close_date >= '${QUARTER.start}' AND o.close_date <= '${QUARTER.end}'
      )
      SELECT
        w.won_count, w.won_pbr, w.avg_time_to_win, cl.lost_count,
        SAFE_DIVIDE(w.won_count, w.won_count + cl.lost_count) AS win_rate,
        SAFE_DIVIDE(w.outbound_won, w.won_count) AS outbound_mix
      FROM won w, closed_lost cl`;
    const rows = await query(sql);
    return rows.length ? rows[0] : { won_count: 0, won_pbr: 0, avg_time_to_win: null, lost_count: 0, win_rate: null, outbound_mix: null };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsStandardsMetrics(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_standards:${team}` : 'all_reps_standards';
  const fetchFn = async () => {
    const sql = `
      SELECT
        o.salesforce_owner_name,
        COUNT(*) AS won_count,
        AVG(DATE_DIFF(o.close_date, DATE(sf.CreatedDate), DAY)) AS avg_time_to_win,
        SAFE_DIVIDE(COUNTIF(sf.LeadSource = 'Outbound'), COUNT(*)) AS outbound_mix
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
      WHERE o.record_type = 'Sales'
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND sf.Type = 'Existing Business' AND sf.IsDeleted = FALSE
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}' AND o.close_date <= '${QUARTER.end}'
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchOppsCreated(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `opps_created:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT COUNT(*) AS opps_created
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
      WHERE o.record_type = 'Sales'
        AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
        AND sf.Type = 'Existing Business' AND sf.IsDeleted = FALSE
        AND DATE(sf.CreatedDate) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].opps_created) || 0 : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsOppsCreated(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_reps_opps_created:${team}` : 'all_reps_opps_created';
  const fetchFn = async () => {
    const sql = `
      SELECT o.salesforce_owner_name, COUNT(*) AS opps_created
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` sf ON o.opportunity_id = sf.Id
      WHERE o.record_type = 'Sales'
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND sf.Type = 'Existing Business' AND sf.IsDeleted = FALSE
        AND DATE(sf.CreatedDate) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  WEEKLY WON PBR (revenue trend chart)
// ═══════════════════════════════════════════════════════════════

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
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON raw.Id = o.opportunity_id
      WHERE o.record_type = 'Sales' AND raw.Type = 'Existing Business' AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}' AND o.close_date <= '${QUARTER.end}'
      GROUP BY week ORDER BY week`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

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
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON raw.Id = o.opportunity_id
      WHERE o.record_type = 'Sales' AND raw.Type = 'Existing Business' AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND o.is_won = TRUE
        AND o.close_date >= '${QUARTER.start}' AND o.close_date <= '${QUARTER.end}'
      GROUP BY o.salesforce_owner_name, week ORDER BY week`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  NEXT QUARTER (outlook tab)
// ═══════════════════════════════════════════════════════════════

export async function fetchNextQuarterPipeline(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const isAll = sfOwnerName === '__all__';
  const key = `next_quarter_pipeline:${sfOwnerName}`;
  const fetchFn = async () => {
    const ownerFilter = isAll
      ? `AND o.salesforce_owner_name IN ${teamRepSubquery()}`
      : `AND o.salesforce_owner_name = '${esc(sfOwnerName)}'`;
    const sql = `
      SELECT
        o.salesforce_owner_name, o.opportunity_id, o.name AS opp_name,
        o.current_stage_name, o.forecast_category, o.close_date, o.next_step,
        o.is_closed, o.is_won, o.updated_at,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales' AND raw.Type = 'Existing Business' AND raw.IsDeleted = FALSE
        ${ownerFilter}
        AND o.is_closed = FALSE AND o.is_won = FALSE
        AND o.close_date >= '${NEXT_QUARTER.start}' AND o.close_date <= '${NEXT_QUARTER.end}'
      ORDER BY pbr DESC`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchNextQuarterQuotas(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `next_quotas:${team}` : 'next_quotas:all';
  const fetchFn = async () => {
    const rolePattern = team
      ? `AMER-SALES-%-SMB-ALL-X-ALL-D2CRETAIL${team.replace('D2CRETAIL', '')}`
      : 'AMER-SALES-%-SMB-ALL-X-ALL-D2CRETAIL%';
    const sql = `
      SELECT u.name, u.user_role,
        CASE WHEN u.user_role LIKE '%LEAD%' THEN 'coach' ELSE 'rep' END AS role_type,
        SUM(q.amount) AS quarterly_quota
      FROM \`shopify-dw.sales.sales_users_daily_snapshot\` u
      JOIN \`shopify-dw.people.worker_current\` w
        ON LOWER(REPLACE(u.name, "'", "\u2019")) = LOWER(w.worker_full_name)
      JOIN \`shopify-dw.people.incentive_compensation_monthly_quotas\` q
        ON w.worker_id = q.worker_id
      WHERE u.date = (SELECT MAX(date) FROM \`shopify-dw.sales.sales_users_daily_snapshot\`)
        AND u.is_active = TRUE AND u.user_role LIKE '${rolePattern}'
        AND q.metric = 'billed_revenue'
        AND q.month BETWEEN '${NEXT_QUARTER.start}' AND '${NEXT_QUARTER.end}'
      GROUP BY u.name, u.user_role, role_type ORDER BY role_type, u.name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchNextQuarterWonPBR(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `next_won:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales' AND raw.Type = 'Existing Business' AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
        AND o.is_won = TRUE
        AND o.close_date >= '${NEXT_QUARTER.start}' AND o.close_date <= '${NEXT_QUARTER.end}'`;
    const rows = await query(sql);
    return rows.length ? Number(rows[0].won_pbr) : 0;
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

export async function fetchAllRepsNextQuarterWonPBR(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_next_won:${team}` : 'all_next_won';
  const fetchFn = async () => {
    const sql = `
      SELECT o.salesforce_owner_name,
        COALESCE(SUM(raw.Projected_Billed_Revenue__c), 0) AS won_pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales' AND raw.Type = 'Existing Business' AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name IN ${teamRepSubquery(team)}
        AND o.is_won = TRUE
        AND o.close_date >= '${NEXT_QUARTER.start}' AND o.close_date <= '${NEXT_QUARTER.end}'
      GROUP BY o.salesforce_owner_name`;
    return await query(sql);
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  FORECAST (Salesforce modelled_salesforce_forecast)
// ═══════════════════════════════════════════════════════════════

export async function fetchForecastCommit(sfOwnerName, forceRefresh = false) {
  await ensureAuth();
  const key = `forecast_commit:${sfOwnerName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT COALESCE(SUM(f.value), 0) AS total_commit
      FROM \`sdp-for-analysts-platform.rev_ops_prod.modelled_salesforce_forecast\` f
      WHERE f.forecast_owner_rep_role LIKE '%AMER-SALES-REP-SMB-ALL-X-ALL-D2CRETAIL%'
        AND f.measure = 'Closed Won'
        AND f.value_type = 'Projected Billed Revenue'
        AND f.forecast_item_category = 'CommitForecast'
        AND f.is_coach_forecast = TRUE
        AND f.period_end_date = '${QUARTER.end}'
        AND f.owner_name = '${esc(sfOwnerName)}'
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
    const leadPattern = team
      ? `AMER-SALES-LEAD-SMB-ALL-X-ALL-D2CRETAIL${team.replace('D2CRETAIL', '')}`
      : 'AMER-SALES-LEAD-SMB-ALL-X-ALL-D2CRETAIL%';
    const sql = `
      SELECT f.owner_name, f.forecast_owner_rep_role, SUM(f.value) AS total_commit
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

export async function fetchAllRepsForecastDetails(team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `rep_forecast_details:${team}` : 'rep_forecast_details';
  const fetchFn = async () => {
    const teamSuffix = team ? team.replace('D2CRETAIL', '') : '%';
    const rolePattern = `AMER-SALES-REP-SMB-ALL-X-ALL-D2CRETAIL${teamSuffix}`;
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

// ═══════════════════════════════════════════════════════════════
//  COACHING: Rep activity comparison (all reps, unified)
// ═══════════════════════════════════════════════════════════════

export async function fetchRepActivityComparison(days = 7, forceRefresh = false) {
  await ensureAuth();
  const key = `rep_comparison_unified:${days}`;
  const fetchFn = async () => {
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_calls AS (
        SELECT
          LOWER(a.attendee_email) AS rep_email,
          sc.platform,
          sc.call_disposition AS disposition,
          sc.has_transcript,
          sc.call_duration_minutes AS duration_min
        FROM \`shopify-dw.sales.sales_calls\` sc,
          UNNEST(sc.attendee_details) AS a
        WHERE a.is_shopify_employee = TRUE
          AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      crm_calls AS (
        SELECT
          cu.email AS rep_email,
          COALESCE(cc.platform, cc.source) AS platform,
          cc.disposition,
          cc.has_transcript,
          ROUND(cc.duration_seconds / 60.0, 1) AS duration_min
        FROM \`shopify-dw.base.base__crm_calls\` cc
        JOIN crm_users cu ON cc.user_id = cu.id
        WHERE NOT cc.longboat_is_deleted
          AND DATE(cc.occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      all_calls AS (SELECT * FROM dw_calls UNION ALL SELECT * FROM crm_calls)
      SELECT
        rep_email,
        COUNT(*) AS total,
        COUNTIF(platform IN ('google_meet','salesloft_conversation','salesloft')) AS meetings,
        COUNTIF(LOWER(disposition) = 'connected') AS connected,
        COUNTIF(has_transcript) AS transcribed,
        ROUND(AVG(IF(duration_min > 0, duration_min, NULL)), 1) AS avg_duration
      FROM all_calls
      GROUP BY rep_email`;
    const rows = await query(sql);
    const teamEmails = new Set(Object.keys(REP_ROSTER));
    return rows.filter(r => teamEmails.has(r.rep_email));
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  MERCHANT CONTEXT (for AI tools)
// ═══════════════════════════════════════════════════════════════

export async function fetchMerchantContext(sfOwnerName, oppName, forceRefresh = false) {
  await ensureAuth();
  const key = `merchant:${sfOwnerName}:${oppName}`;
  const fetchFn = async () => {
    const sql = `
      SELECT o.opportunity_id, o.name AS opp_name, o.current_stage_name,
        o.forecast_category, o.close_date, o.next_step,
        raw.Projected_Billed_Revenue__c AS pbr
      FROM \`shopify-dw.sales.sales_opportunities\` o
      JOIN \`shopify-dw.raw_salesforce_banff.opportunity\` raw ON o.opportunity_id = raw.Id
      WHERE o.record_type = 'Sales' AND raw.Type = 'Existing Business' AND raw.IsDeleted = FALSE
        AND o.salesforce_owner_name = '${esc(sfOwnerName)}'
        AND o.name = '${esc(oppName)}'
      LIMIT 1`;
    const deal = await query(sql);
    return { deal: deal[0] || null };
  };
  return forceRefresh ? fetchFn() : getCachedOrFetch(key, fetchFn);
}

// ═══════════════════════════════════════════════════════════════
//  FORECAST SNAPSHOTS (quick.db persistence)
// ═══════════════════════════════════════════════════════════════

const forecastSnapshots = quick.db.collection('coaching_hub_forecast_snapshots');

export async function saveForecastSnapshot(data, weekLabel) {
  const key = data.viewMode === 'team' ? `${weekLabel}:team` : `${weekLabel}:${data.sfName || 'unknown'}`;
  try {
    const existing = await forecastSnapshots.where({ key }).limit(1).find();
    if (existing.length) return;
  } catch (_) {}
  try {
    await forecastSnapshots.create({
      key, week: weekLabel,
      entity: data.viewMode === 'team' ? 'team' : (data.sfName || 'unknown'),
      data: JSON.stringify(data.snapshotPayload),
      created_at: new Date().toISOString(),
    });
  } catch (_) {}
}

export async function getForecastSnapshots(entity, weeksBack = 8) {
  try {
    const items = await forecastSnapshots
      .where({ entity }).orderBy('created_at', 'desc').limit(weeksBack).find();
    return items.map(i => ({ week: i.week, ...JSON.parse(i.data) }));
  } catch (_) { return []; }
}

export async function cleanupOldSnapshots(maxWeeks = 13) {
  try {
    const all = await forecastSnapshots.orderBy('created_at', 'asc').find();
    const cutoff = new Date();
    cutoff.setDate(cutoff.getDate() - maxWeeks * 7);
    for (const item of all) {
      if (new Date(item.created_at) < cutoff) await forecastSnapshots.delete(item._id);
    }
  } catch (_) {}
}

// ═══════════════════════════════════════════════════════════════
//  TEAM-WIDE RECENT CALLS (for coaching tab - shows data immediately)
//  Loads all team calls from L14D with transcripts/summaries
// ═══════════════════════════════════════════════════════════════

export async function fetchAllRepsRecentCalls(days = 14, team = null, forceRefresh = false) {
  await ensureAuth();
  const key = team ? `all_recent_calls_unified:${days}:${team}` : `all_recent_calls_unified:${days}`;
  const fetchFn = async () => {
    const sql = `
      WITH ${crmUserEmailCTE()},
      dw_calls AS (
        SELECT
          LOWER(a.attendee_email) AS rep_email,
          CAST(sc.event_id AS STRING) AS call_id,
          sc.event_start AS occurred_at,
          sc.call_title AS title,
          sc.platform,
          sc.call_duration_minutes AS duration_min,
          sc.call_disposition AS disposition,
          sc.has_transcript,
          sc.transcript_summary.text AS summary_text,
          'dw' AS source_table
        FROM \`shopify-dw.sales.sales_calls\` sc,
          UNNEST(sc.attendee_details) AS a
        WHERE a.is_shopify_employee = TRUE
          AND DATE(sc.event_start) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      crm_calls AS (
        SELECT
          cu.email AS rep_email,
          CAST(cc.crm_call_id AS STRING) AS call_id,
          cc.occurred_at,
          cc.title,
          COALESCE(cc.platform, cc.source) AS platform,
          ROUND(cc.duration_seconds / 60.0, 1) AS duration_min,
          cc.disposition,
          cc.has_transcript,
          CAST(NULL AS STRING) AS summary_text,
          'crm' AS source_table
        FROM \`shopify-dw.base.base__crm_calls\` cc
        JOIN crm_users cu ON cc.user_id = cu.id
        WHERE NOT cc.longboat_is_deleted
          AND DATE(cc.occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL ${days} DAY)
      ),
      all_calls AS (
        SELECT * FROM dw_calls UNION ALL SELECT * FROM crm_calls
      )
      SELECT *
      FROM all_calls
      WHERE duration_min > 0
      ORDER BY occurred_at DESC
      LIMIT 100`;
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
