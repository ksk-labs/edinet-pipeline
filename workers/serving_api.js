// edinet-serving-api: EDINET 提供層の読み取り専用 API (P2-T6、仕様 #15 §9)。
//
// D1 (バインディング DB) に同期された Gold と会社マスタの現行値を返す。
// すべて GET のみ、応答は JSON で、すべての応答に snapshot_date
// (版固定の単位である日次スナップショット日) を含める。認証なし
// (公開・読み取り専用・個人情報なし、ASVS L1)。
// SQL はバインド変数で組み立て、列名はホワイトリストからのみ使う。

const INDICATORS = new Set([
  "net_sales", "operating_income", "net_income", "roe", "net_sales_cagr5",
  "operating_margin", "ordinary_income", "eps", "bps", "equity_ratio",
  "total_assets", "net_assets", "operating_cf", "capex_acquisition",
  "free_cf", "dividend_per_share", "dividend_increase_years",
  "shares_outstanding", "shares_average",
]);

const ANNUAL_SUMMARY_COLUMNS = [
  "internal_id", "edinet_code", "period_start", "period_end",
  "period_months", "is_irregular", "accounting_standard", "net_sales",
  "operating_income", "ordinary_income", "net_income", "eps", "bps",
  "roe", "equity_ratio", "total_assets", "net_assets", "operating_cf",
  "free_cf", "dividend_per_share",
];

const GOLD_ROUTES = {
  annual: "gold_annual",
  quarterly: "gold_quarterly",
  dividends: "gold_dividends",
};

function jsonResponse(body, status) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });
}

async function loadMeta(db) {
  const { results } = await db
    .prepare('SELECT "key", "value" FROM "serving_meta"')
    .all();
  const meta = {};
  for (const row of results) meta[row.key] = row.value;
  return meta;
}

function parsePaging(url) {
  const rawLimit = url.searchParams.get("limit");
  const rawOffset = url.searchParams.get("offset");
  let limit = 50;
  let offset = 0;
  if (rawLimit !== null) {
    limit = Number(rawLimit);
    if (!Number.isInteger(limit) || limit < 0) return null;
    if (limit > 500) limit = 500;
  }
  if (rawOffset !== null) {
    offset = Number(rawOffset);
    if (!Number.isInteger(offset) || offset < 0) return null;
  }
  return { limit, offset };
}

async function countAndPage(db, fromWhere, binds, paging, orderBy) {
  const countRow = await db
    .prepare(`SELECT COUNT(*) AS n FROM ${fromWhere}`)
    .bind(...binds)
    .first();
  const { results } = await db
    .prepare(
      `SELECT * FROM ${fromWhere} ORDER BY ${orderBy} LIMIT ? OFFSET ?`)
    .bind(...binds, paging.limit, paging.offset)
    .all();
  return { total: countRow.n, items: results };
}

async function companyExists(db, internalId) {
  const row = await db
    .prepare('SELECT "internal_id" FROM "companies" WHERE "internal_id" = ?')
    .bind(internalId)
    .first();
  return row !== null;
}

async function handleMeta(db, meta) {
  let counts = {};
  if (meta.table_counts) {
    try {
      counts = JSON.parse(meta.table_counts);
    } catch {
      counts = {};
    }
  }
  return {
    snapshot_date: meta.snapshot_date ?? null,
    gold_generated_at: meta.gold_generated_at ?? null,
    synced_at: meta.synced_at ?? null,
    tables: counts,
  };
}

async function handleCompanies(db, url) {
  const paging = parsePaging(url);
  if (paging === null) throw { status: 400, message: "invalid limit or offset" };
  const conditions = [];
  const binds = [];
  const q = url.searchParams.get("q");
  if (q) {
    conditions.push('"name" LIKE ?');
    binds.push(`%${q}%`);
  }
  const code = url.searchParams.get("code");
  if (code) {
    conditions.push('("edinet_code" = ? OR "primary_securities_code" = ?)');
    binds.push(code, code);
  }
  const where = conditions.length ? ` WHERE ${conditions.join(" AND ")}` : "";
  return countAndPage(
    db, `"companies"${where}`, binds, paging, '"internal_id" ASC');
}

async function handleCompany(db, internalId) {
  const company = await db
    .prepare('SELECT * FROM "companies" WHERE "internal_id" = ?')
    .bind(internalId)
    .first();
  if (company === null) throw { status: 404, message: "company not found" };
  const { results: securities } = await db
    .prepare(
      'SELECT * FROM "securities" WHERE "internal_id" = ? ' +
      'ORDER BY "is_primary" DESC, "securities_code" ASC')
    .bind(internalId)
    .all();
  const latest = await db
    .prepare(
      `SELECT ${ANNUAL_SUMMARY_COLUMNS.map((c) => `"${c}"`).join(", ")} ` +
      'FROM "gold_annual_latest" WHERE "internal_id" = ?')
    .bind(internalId)
    .first();
  return { company, securities, latest_annual: latest };
}

async function handleCompanyRows(db, internalId, kind, url) {
  const paging = parsePaging(url);
  if (paging === null) throw { status: 400, message: "invalid limit or offset" };
  if (!(await companyExists(db, internalId))) {
    throw { status: 404, message: "company not found" };
  }
  if (kind === "events") {
    return countAndPage(
      db, '"events" WHERE "internal_id" = ?', [internalId], paging,
      '"disclosed_at" DESC, "event_id" ASC');
  }
  return countAndPage(
    db, `"${GOLD_ROUTES[kind]}" WHERE "internal_id" = ?`, [internalId],
    paging, '"period_end" DESC, "period_start" DESC');
}

async function handleScreen(db, url) {
  const paging = parsePaging(url);
  if (paging === null) throw { status: 400, message: "invalid limit or offset" };
  const indicator = url.searchParams.get("indicator");
  if (!indicator || !INDICATORS.has(indicator)) {
    throw { status: 400, message: "unknown or missing indicator" };
  }
  const conditions = [`"${indicator}" IS NOT NULL`];
  const binds = [];
  for (const [param, op] of [["min", ">="], ["max", "<="]]) {
    const raw = url.searchParams.get(param);
    if (raw !== null) {
      const value = Number(raw);
      if (raw.trim() === "" || !Number.isFinite(value)) {
        throw { status: 400, message: `invalid ${param}` };
      }
      conditions.push(`"${indicator}" ${op} ?`);
      binds.push(value);
    }
  }
  const page = await countAndPage(
    db, `"gold_annual_latest" WHERE ${conditions.join(" AND ")}`, binds,
    paging, `"${indicator}" DESC, "internal_id" ASC`);
  return { indicator, ...page };
}

export default {
  async fetch(request, env) {
    let snapshot = null;
    let meta = {};
    try {
      meta = await loadMeta(env.DB);
      snapshot = meta.snapshot_date ?? null;
    } catch {
      meta = {};
    }
    try {
      if (request.method !== "GET") {
        return jsonResponse(
          { error: "method not allowed", snapshot_date: snapshot }, 405);
      }
      const url = new URL(request.url);
      const path = url.pathname.replace(/\/+$/, "") || "/";
      let body;
      let match;
      if (path === "/v1/meta") {
        body = await handleMeta(env.DB, meta);
      } else if (path === "/v1/companies") {
        body = await handleCompanies(env.DB, url);
      } else if ((match = path.match(/^\/v1\/companies\/(\d+)$/))) {
        body = await handleCompany(env.DB, Number(match[1]));
      } else if ((match = path.match(
          /^\/v1\/companies\/(\d+)\/(annual|quarterly|dividends|events)$/))) {
        body = await handleCompanyRows(
          env.DB, Number(match[1]), match[2], url);
      } else if (path === "/v1/screen") {
        body = await handleScreen(env.DB, url);
      } else {
        return jsonResponse(
          { error: "not found", snapshot_date: snapshot }, 404);
      }
      return jsonResponse({ snapshot_date: snapshot, ...body }, 200);
    } catch (e) {
      if (e && typeof e === "object" && "status" in e) {
        return jsonResponse(
          { error: e.message, snapshot_date: snapshot }, e.status);
      }
      return jsonResponse(
        { error: "internal error", snapshot_date: snapshot }, 500);
    }
  },
};
