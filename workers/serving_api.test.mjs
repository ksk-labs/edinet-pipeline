// workers/serving_api.js のテスト (P2-T6)。
// 実行: リポジトリのルートで `node workers/serving_api.test.mjs`
// (CI の tests.yml は Python のみのため、本テストはローカル実行用。
//  D1 は SQL とバインド値を記録するフェイクで置き換える)
import assert from "node:assert/strict";
import { readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

const src = readFileSync(new URL("./serving_api.js", import.meta.url), "utf8");
const tmp = join(tmpdir(), "serving_api_under_test.mjs");
writeFileSync(tmp, src);
const worker = (await import(tmp)).default;

function makeDb(handler) {
  const calls = [];
  return {
    calls,
    prepare(sql) {
      const stmt = {
        sql,
        args: [],
        bind(...args) {
          stmt.args = args;
          return stmt;
        },
        async all() {
          calls.push({ sql, args: stmt.args });
          return { results: handler(sql, stmt.args, "all") ?? [] };
        },
        async first() {
          calls.push({ sql, args: stmt.args });
          return handler(sql, stmt.args, "first") ?? null;
        },
      };
      return stmt;
    },
  };
}

const META_ROWS = [
  { key: "snapshot_date", value: "2026-11-20" },
  { key: "gold_generated_at", value: "2026-11-20T00:00:00+00:00" },
  { key: "table_counts", value: '{"companies": 3}' },
];

function baseHandler(extra) {
  return (sql, args, mode) => {
    if (sql.includes("serving_meta")) return META_ROWS;
    return extra(sql, args, mode);
  };
}

async function callApi(db, path, method = "GET") {
  const response = await worker.fetch(
    new Request(`https://example.test${path}`, { method }), { DB: db });
  return { status: response.status, body: await response.json() };
}

test("meta は snapshot と行数を返す", async () => {
  const db = makeDb(baseHandler(() => []));
  const { status, body } = await callApi(db, "/v1/meta");
  assert.equal(status, 200);
  assert.equal(body.snapshot_date, "2026-11-20");
  assert.deepEqual(body.tables, { companies: 3 });
});

test("companies の q は名称の部分一致として組み立てる", async () => {
  const db = makeDb(baseHandler((sql) => {
    if (sql.includes("COUNT(*)")) return { n: 0 };
    return [];
  }));
  const { status, body } = await callApi(db, "/v1/companies?q=アルファ");
  assert.equal(status, 200);
  assert.equal(body.total, 0);
  const listCall = db.calls.find((c) => c.sql.includes('FROM "companies"')
    && c.sql.includes("LIMIT"));
  assert.ok(listCall.sql.includes('"name" LIKE ?'));
  assert.equal(listCall.args[0], "%アルファ%");
});

test("companies の q は % と _ とバックスラッシュをエスケープする", async () => {
  const db = makeDb(baseHandler((sql) => {
    if (sql.includes("COUNT(*)")) return { n: 0 };
    return [];
  }));
  const { status } = await callApi(
    db, "/v1/companies?q=" + encodeURIComponent("100%_x\\y"));
  assert.equal(status, 200);
  const listCall = db.calls.find((c) => c.sql.includes('FROM "companies"')
    && c.sql.includes("LIMIT"));
  assert.ok(listCall.sql.includes("ESCAPE"));
  assert.equal(listCall.args[0], "%100\\%\\_x\\\\y%");
});

test("companies の code は EDINET コードと証券コードの両方に当てる", async () => {
  const db = makeDb(baseHandler((sql) => {
    if (sql.includes("COUNT(*)")) return { n: 0 };
    return [];
  }));
  await callApi(db, "/v1/companies?code=E00001");
  const listCall = db.calls.find((c) => c.sql.includes('FROM "companies"')
    && c.sql.includes("LIMIT"));
  assert.ok(listCall.sql.includes('"edinet_code" = ?'));
  assert.ok(listCall.sql.includes('"primary_securities_code" = ?'));
  assert.deepEqual(listCall.args.slice(0, 2), ["E00001", "E00001"]);
});

test("存在しない会社は 404", async () => {
  const db = makeDb(baseHandler(() => null));
  const { status, body } = await callApi(db, "/v1/companies/999");
  assert.equal(status, 404);
  assert.equal(body.snapshot_date, "2026-11-20");
});

test("screen は未知の指標を 400 で拒む", async () => {
  const db = makeDb(baseHandler(() => []));
  const { status } = await callApi(db, "/v1/screen?indicator=password");
  assert.equal(status, 400);
});

test("screen は指標の範囲条件を組み立てる", async () => {
  const db = makeDb(baseHandler((sql) => {
    if (sql.includes("COUNT(*)")) return { n: 0 };
    return [];
  }));
  const { status, body } = await callApi(
    db, "/v1/screen?indicator=roe&min=10&max=20");
  assert.equal(status, 200);
  assert.equal(body.indicator, "roe");
  const listCall = db.calls.find((c) => c.sql.includes("gold_annual_latest")
    && c.sql.includes("LIMIT"));
  assert.ok(listCall.sql.includes('"roe" >= ?'));
  assert.ok(listCall.sql.includes('"roe" <= ?'));
  assert.deepEqual(listCall.args.slice(0, 2), [10, 20]);
});

test("不正な limit は 400", async () => {
  const db = makeDb(baseHandler(() => []));
  const { status } = await callApi(db, "/v1/companies?limit=-1");
  assert.equal(status, 400);
});

test("GET 以外は 405", async () => {
  const db = makeDb(baseHandler(() => []));
  const { status } = await callApi(db, "/v1/meta", "POST");
  assert.equal(status, 405);
});

test("events は開示日時の降順で組み立てる", async () => {
  const db = makeDb(baseHandler((sql, args, mode) => {
    if (sql.includes('FROM "companies"') && mode === "first") {
      return { internal_id: args[0] };
    }
    if (sql.includes("COUNT(*)")) return { n: 0 };
    return [];
  }));
  const { status } = await callApi(db, "/v1/companies/1/events");
  assert.equal(status, 200);
  const listCall = db.calls.find((c) => c.sql.includes('"events"')
    && c.sql.includes("LIMIT"));
  assert.ok(listCall.sql.includes('"disclosed_at" DESC'));
});
