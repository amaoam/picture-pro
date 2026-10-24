import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8123/api').replace(/\/$/, '');
const TARGET_TYPE = (__ENV.TARGET_TYPE || 'POST').toUpperCase();
const TARGET_MODE = (__ENV.TARGET_MODE || 'single').toLowerCase(); // single | multi | mixed
const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '60s';
const PRE_VUS = Number(__ENV.PRE_VUS || 50);
const MAX_VUS = Number(__ENV.MAX_VUS || 100);
const HOT_RATIO = Math.max(0, Math.min(1, Number(__ENV.HOT_RATIO || 0.8)));
const SESSION_COOKIE_NAME = __ENV.SESSION_COOKIE_NAME || 'satoken';
const businessFailures = new Rate('business_failures');

// 只在测试机本地放置真实压测账号；不要把 users.local.json 提交到仓库。
const credentials = new SharedArray('load-test-credentials', function () {
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
    throw new Error(`${label} 不是有效的纯数字 ID：${id}。请将 targets.local.json 中的占位符替换成真实、可访问的目标 ID。`);
  }
}

function uniqueTargetIds() {
  const ids = Array.isArray(targetConfig.targetIds) ? targetConfig.targetIds.map(String) : [];
  const hot = String(__ENV.TARGET_ID || targetConfig.hotTargetId || '');
  validateTargetId(hot, 'hotTargetId / TARGET_ID');
  const all = Array.from(new Set([hot, ...ids]));
  if (TARGET_MODE !== 'single') {
    all.forEach((id, i) => validateTargetId(id, `targetIds[${i}]`));
  }
  return { hot, all, others: all.filter((id) => id !== hot) };
}

export function setup() {
  if (TARGET_TYPE !== 'POST' && TARGET_TYPE !== 'PICTURE') {
    throw new Error('TARGET_TYPE 只能是 POST 或 PICTURE。');
  }
  if (!['single', 'multi', 'mixed'].includes(TARGET_MODE)) {
    throw new Error('TARGET_MODE 只能是 single、multi 或 mixed。');
  }
  if (!Number.isInteger(RATE) || RATE <= 0) throw new Error('RATE 必须是正整数。');
  if (!Number.isInteger(PRE_VUS) || !Number.isInteger(MAX_VUS) || PRE_VUS <= 0 || MAX_VUS < PRE_VUS) {
    throw new Error('请确保 PRE_VUS > 0，且 MAX_VUS >= PRE_VUS。');
  }
  if (credentials.length < MAX_VUS) {
    throw new Error(`测试账号数 ${credentials.length} 小于 MAX_VUS=${MAX_VUS}。请准备至少 MAX_VUS 个独立账号。`);
  }
  const { all, others } = uniqueTargetIds();
  if (TARGET_MODE === 'multi' && all.length < 2) throw new Error('multi 模式至少需要 2 个不同的真实目标 ID。');
  if (TARGET_MODE === 'mixed' && others.length < 1) throw new Error('mixed 模式需要 hotTargetId 之外至少 1 个目标 ID。');

  const sessions = [];
  for (const account of credentials) {
    const response = http.post(
      `${BASE_URL}/user/login`,
      JSON.stringify({ userAccount: account.userAccount, userPassword: account.userPassword }),
      { headers: { 'Content-Type': 'application/json' }, tags: { name: 'login_setup' } }
    );
    const payload = parseBody(response);
    if (response.status < 200 || response.status >= 300 || payload.code !== 0) {
      throw new Error(`测试账号登录失败：${account.userAccount}，HTTP=${response.status}，业务码=${payload.code}`);
    }
    const cookieValue = extractCookie(response, SESSION_COOKIE_NAME);
    if (!cookieValue) {
      throw new Error(`登录响应未找到 ${SESSION_COOKIE_NAME} Cookie。请确认当前项目使用的会话 Cookie 名称。`);
    }
    sessions.push({ cookie: `${SESSION_COOKIE_NAME}=${cookieValue}` });
  }
  return { sessions, targetIds: all };
}

function chooseTarget(data) {
  const hot = String(__ENV.TARGET_ID || targetConfig.hotTargetId || '');
  const targets = data.targetIds;
  if (TARGET_MODE === 'single') return hot;
  if (TARGET_MODE === 'multi') {
    // 一个 VU 固定到一个目标；便于观察多个目标分散后的吞吐变化。
    return targets[(__VU - 1) % targets.length];
  }
  // mixed：按请求抽样，约 HOT_RATIO 的请求打到热点，其余均匀抽到非热点目标。
  // 每个 VU 按 targetKey 分别保存状态，不会把 A 目标的 unlike 状态误用于 B 目标。
  const others = targets.filter((id) => id !== hot);
  if (Math.random() < HOT_RATIO) return hot;
  return others[Math.floor(Math.random() * others.length)];
}

// 记录每个 VU 在每个 target 上自己的成功状态。每个账号只由单个 VU 使用。
const likedByTarget = Object.create(null);

export default function (data) {
  const session = data.sessions[__VU - 1];
  if (!session) throw new Error(`没有为 VU ${__VU} 分配到测试账号。请增加 users.local.json 中的账号数。`);

  const targetId = chooseTarget(data);
  const targetKey = `${TARGET_TYPE}:${targetId}`;
  const currentlyLiked = likedByTarget[targetKey] === true;
  const targetPath = TARGET_TYPE === 'POST'
    ? `/community/post/${targetId}/like`
    : `/community/picture/${targetId}/like`;
  const params = {
    headers: { Cookie: session.cookie },
    tags: {
      name: 'like_toggle',
      target_type: TARGET_TYPE,
      experiment: TARGET_MODE,
      operation: currentlyLiked ? 'unlike' : 'like',
    },
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
