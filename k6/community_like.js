import http from 'k6/http';
import { check } from 'k6';
import { Rate } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const BASE_URL = (__ENV.BASE_URL || 'http://localhost:8123/api').replace(/\/$/, '');
const TARGET_TYPE = (__ENV.TARGET_TYPE || 'POST').toUpperCase();
const TARGET_ID = __ENV.TARGET_ID;
const RATE = Number(__ENV.RATE || 10);
const DURATION = __ENV.DURATION || '30s';
const PRE_VUS = Number(__ENV.PRE_VUS || 10);
const MAX_VUS = Number(__ENV.MAX_VUS || 20);
const SESSION_COOKIE_NAME = __ENV.SESSION_COOKIE_NAME || 'satoken';
const businessFailures = new Rate('business_failures');
s
// 由 scripts/New-LoadTestUsers.ps1 生成。文件只保存在本地，不要提交到 Git。
const credentials = new SharedArray('load-test-credentials', function () {
  return JSON.parse(open(__ENV.USERS_FILE || './users.local.json')); 
});

export const options = {
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
  const match = String(setCookie).match(regex);
  return match ? match[1] : null;
}

export function setup() {
  if (!TARGET_ID || !/^\d+$/.test(String(TARGET_ID))) {
    throw new Error('请设置有效的 TARGET_ID。');
  }
  if (TARGET_TYPE !== 'POST' && TARGET_TYPE !== 'PICTURE') {
    throw new Error('TARGET_TYPE 只能是 POST 或 PICTURE。');
  }
  if (credentials.length < MAX_VUS) {
    throw new Error(`测试账号数 ${credentials.length} 小于 MAX_VUS=${MAX_VUS}。请先增加测试账号，确保一个 VU 使用一个独立账号。`);
  }

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
      throw new Error(`登录响应未找到 ${SESSION_COOKIE_NAME} Cookie。请查看浏览器登录 Cookie，并设置 SESSION_COOKIE_NAME。`);
    }
    sessions.push({ cookie: `${SESSION_COOKIE_NAME}=${cookieValue}` });
  }
  return { sessions };
}

// 每个 VU 都使用自己的测试用户，在点赞和取消点赞之间交替。
// 这样能持续产生真实状态变化，而不是反复请求同一个用户的重复点赞。
let likedByThisVu = false;

export default function (data) {
  const session = data.sessions[__VU - 1];
  const targetPath = TARGET_TYPE === 'POST'
    ? `/community/post/${TARGET_ID}/like`
    : `/community/picture/${TARGET_ID}/like`;
  const url = `${BASE_URL}${targetPath}`;
  const params = {
    headers: { Cookie: session.cookie },
    tags: { name: 'like_toggle', target_type: TARGET_TYPE, operation: likedByThisVu ? 'unlike' : 'like' },
    timeout: __ENV.REQUEST_TIMEOUT || '10s',
  };

  const response = likedByThisVu
    ? http.del(url, null, params)
    : http.post(url, null, params);
  const ok = successfulBusinessResponse(response);
  businessFailures.add(!ok);
  check(response, {
    'HTTP 2xx': (r) => r.status >= 200 && r.status < 300,
    'business code is 0': (r) => parseBody(r).code === 0,
  });
  // 只有业务成功才切换本 VU 的预期状态，避免失败请求让后续操作方向错位。
  if (ok) likedByThisVu = !likedByThisVu;
}
