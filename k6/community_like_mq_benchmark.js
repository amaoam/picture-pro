import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';

// RabbitMQ V1 与 Redis V3 共用公开 API 路径；API 返回代表 Redis 命令已受理，不代表 MySQL 已立即完成持久化。
const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8123/api').replace(/\/$/, '');
const TARGET_TYPE = (__ENV.TARGET_TYPE || 'POST').toUpperCase();
const TARGET_MODE = (__ENV.TARGET_MODE || 'single').toLowerCase(); // single | multi | mixed
const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '60s';
const PRE_VUS = Number(__ENV.PRE_VUS || 200);
const MAX_VUS = Number(__ENV.MAX_VUS || 400);
const HOT_RATIO = Math.max(0, Math.min(1, Number(__ENV.HOT_RATIO || 0.8)));
const SESSION_COOKIE_NAME = __ENV.SESSION_COOKIE_NAME || 'satoken';
const businessFailures = new Rate('business_failures');

// 这些 JSON 只在本机保存；不要把用户密码或真实 targets.local.json 提交到 Git。
const credentials = new SharedArray('mq-load-test-credentials', function () {
  return JSON.parse(open(__ENV.USERS_FILE || './k6/users.local.json'));
});
const targetConfig = JSON.parse(open(__ENV.TARGETS_FILE || './k6/targets.local.json'));

export const options = {
  setupTimeout: __ENV.SETUP_TIMEOUT || '5m',
  scenarios: {
    like_toggle_load: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: PRE_VUS,
      maxVUs: MAX_VUS,
      gracefulStop: '10s',
    },
  },
  thresholds: {
    'http_req_failed{name:like_toggle}': ['rate<0.01'],
    business_failures: ['rate<0.01'],
    'http_req_duration{name:like_toggle}': ['p(95)<500', 'p(99)<1000'],
    dropped_iterations: ['count==0'],
  },
};

function parseBody(response) {
  try { return JSON.parse(response.body || '{}'); }
  catch (_) { return {}; }
}

function successfulBusinessResponse(response) {
  return response.status >= 200 && response.status < 300 && parseBody(response).code === 0;
}

function extractCookie(response, cookieName) {
  const values = response.cookies && response.cookies[cookieName];
  if (values && values.length && values[0].value) return values[0].value;
  const setCookie = response.headers['Set-Cookie'] || response.headers['set-cookie'] || '';
  const regex = new RegExp('(?:^|[,;]\\s*)' + cookieName + '=([^;]+)');
  const match = regex.exec(String(setCookie));
  return match ? match[1] : null;
}

function validateTargetId(id, label) {
  if (!/^\d+$/.test(String(id || ''))) {
    throw new Error(`${label} 不是有效的数字 ID：${id}。请替换 k6/targets.local.json 中的占位符。`);
  }
}

function uniqueTargetIds() {
  const ids = Array.isArray(targetConfig.targetIds) ? targetConfig.targetIds.map(String) : [];
  const hot = String(__ENV.TARGET_ID || targetConfig.hotTargetId || '');
  validateTargetId(hot, 'hotTargetId / TARGET_ID');
  const all = Array.from(new Set([hot, ...ids]));
  if (TARGET_MODE !== 'single') all.forEach((id, i) => validateTargetId(id, `targetIds[${i}]`));
  return { hot, all, others: all.filter((id) => id !== hot) };
}

export function setup() {
  if (TARGET_TYPE !== 'POST' && TARGET_TYPE !== 'PICTURE') throw new Error('TARGET_TYPE 只能是 POST 或 PICTURE。');
  if (!['single', 'multi', 'mixed'].includes(TARGET_MODE)) throw new Error('TARGET_MODE 只能是 single、multi、mixed。');
  if (!Number.isInteger(RATE) || RATE <= 0) throw new Error('RATE 必须是正整数。');
  if (!Number.isInteger(PRE_VUS) || !Number.isInteger(MAX_VUS) || PRE_VUS <= 0 || MAX_VUS < PRE_VUS) {
    throw new Error('请确保 PRE_VUS > 0，且 MAX_VUS >= PRE_VUS。');
  }
  if (credentials.length < MAX_VUS) {
    throw new Error(`测试账号数 ${credentials.length} 小于 MAX_VUS=${MAX_VUS}。请至少准备 ${MAX_VUS} 个独立账号。`);
  }
  const { all, others } = uniqueTargetIds();
  if (TARGET_MODE === 'multi' && all.length < 2) throw new Error('multi 模式至少需要 2 个不同的真实目标 ID。');
  if (TARGET_MODE === 'mixed' && others.length < 1) throw new Error('mixed 模式需要 hotTargetId 之外至少 1 个目标 ID。');

  const sessions = [];
  for (const account of credentials) {
    const response = http.post(`${BASE_URL}/user/login`,
      JSON.stringify({ userAccount: account.userAccount, userPassword: account.userPassword }),
      { headers: { 'Content-Type': 'application/json' }, tags: { name: 'login_setup' } });
    const payload = parseBody(response);
    if (response.status < 200 || response.status >= 300 || payload.code !== 0) {
      throw new Error(`测试账号登录失败：${account.userAccount}，HTTP=${response.status}，业务码=${payload.code}`);
    }
    const cookieValue = extractCookie(response, SESSION_COOKIE_NAME);
    if (!cookieValue) throw new Error(`登录响应未找到 ${SESSION_COOKIE_NAME} Cookie，请检查会话 Cookie 名称。`);
    sessions.push({ cookie: `${SESSION_COOKIE_NAME}=${cookieValue}` });
  }
  return { sessions, targetIds: all };
}

function chooseTarget(data) {
  const hot = String(__ENV.TARGET_ID || targetConfig.hotTargetId || '');
  if (TARGET_MODE === 'single') return hot;
  if (TARGET_MODE === 'multi') return data.targetIds[(__VU - 1) % data.targetIds.length];
  const others = data.targetIds.filter((id) => id !== hot);
  if (Math.random() < HOT_RATIO) return hot;
  return others[Math.floor(Math.random() * others.length)];
}

// 每个 VU 独占一个测试账号，并按每个 target 独立切换 LIKE / UNLIKE。
const likedByTarget = Object.create(null);

export default function (data) {
  // The PowerShell benchmark harness uses this marker to separate account-login setup
  // telemetry from the actual constant-arrival-rate load phase. It does not change load.
  if (__VU === 1 && __ITER === 0) {
    console.log('BENCHMARK_LOAD_STARTED');
  }

  const session = data.sessions[__VU - 1];
  if (!session) throw new Error(`没有给 VU ${__VU} 分配测试账号，请增加 users.local.json 账号数。`);
  const targetId = chooseTarget(data);
  const targetKey = `${TARGET_TYPE}:${targetId}`;
  const currentlyLiked = likedByTarget[targetKey] === true;
  const targetPath = TARGET_TYPE === 'POST'
    ? `/community/post/${targetId}/like`
    : `/community/picture/${targetId}/like`;
  const params = {
    headers: { Cookie: session.cookie },
    tags: { name: 'like_toggle', target_type: TARGET_TYPE, experiment: TARGET_MODE,
      operation: currentlyLiked ? 'unlike' : 'like' },
    timeout: __ENV.REQUEST_TIMEOUT || '10s',
  };
  const response = currentlyLiked
    ? http.del(`${BASE_URL}${targetPath}`, null, params)
    : http.post(`${BASE_URL}${targetPath}`, null, params);
  const ok = successfulBusinessResponse(response);
  businessFailures.add(!ok);
  check(response, {
    'HTTP 2xx': (r) => r.status >= 200 && r.status < 300,
    'business code is 0': (r) => parseBody(r).code === 0,
  });
  if (ok) likedByTarget[targetKey] = !currentlyLiked;
}
