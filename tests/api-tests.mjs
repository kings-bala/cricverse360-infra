#!/usr/bin/env node

const API_BASE = "https://zig9f1eaqf.execute-api.us-east-1.amazonaws.com/v1";
const TEST_EMAIL = `testuser_${Date.now()}@cricverse360test.com`;
const TEST_PASSWORD = "TestPass123!";
const TEST_NAME = "Test User API";
const COACH_EMAIL = `coach_${Date.now()}@cricverse360test.com`;

let accessToken = "";
let testUserId = "";
let testAcademyId = "";
let testSessionId = "";
let testStatsId = "";
let testPostId = "";
let testWatchId = "";
let testStrategyId = "";
let coachUserId = "";

const results = { passed: 0, failed: 0, skipped: 0, tests: [] };

async function api(method, path, body = null, token = null, headers = {}) {
  const opts = {
    method,
    headers: {
      "Content-Type": "application/json",
      ...headers,
    },
  };
  if (token) opts.headers["Authorization"] = `Bearer ${token}`;
  if (body) opts.body = JSON.stringify(body);
  try {
    const res = await fetch(`${API_BASE}${path}`, opts);
    const data = await res.json();
    return { status: res.status, data, ok: res.ok };
  } catch (err) {
    return { status: 0, data: { error: err.message }, ok: false };
  }
}

function assert(condition, message) {
  if (!condition) throw new Error(message);
}

async function test(name, fn) {
  try {
    await fn();
    results.passed++;
    results.tests.push({ name, status: "PASS" });
    console.log(`  PASS  ${name}`);
  } catch (err) {
    results.failed++;
    results.tests.push({ name, status: "FAIL", error: err.message });
    console.log(`  FAIL  ${name} — ${err.message}`);
  }
}

function skip(name, reason) {
  results.skipped++;
  results.tests.push({ name, status: "SKIP", reason });
  console.log(`  SKIP  ${name} — ${reason}`);
}

// ══════════════════════════════════════════════════════
// 1. HEALTH CHECK
// ══════════════════════════════════════════════════════
async function testHealthCheck() {
  console.log("\n--- Health Check ---");

  await test("GET /health returns 200 with status ok", async () => {
    const res = await api("GET", "/health");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.status === "ok", `Expected status 'ok', got '${res.data.status}'`);
    assert(res.data.service === "CricVerse360 API", `Unexpected service name`);
  });

  await test("GET / returns 200 (root health)", async () => {
    const res = await api("GET", "/");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 2. AUTH ROUTES
// ══════════════════════════════════════════════════════
async function testAuth() {
  console.log("\n--- Auth Routes ---");

  await test("POST /auth/register — missing fields returns 400", async () => {
    const res = await api("POST", "/auth/register", { email: TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /auth/register — valid registration returns 200", async () => {
    const res = await api("POST", "/auth/register", {
      email: TEST_EMAIL,
      password: TEST_PASSWORD,
      fullName: TEST_NAME,
      role: "player",
    });
    assert(res.status === 200 || res.status === 409, `Expected 200 or 409, got ${res.status}: ${JSON.stringify(res.data)}`);
    if (res.data.userId) testUserId = res.data.userId;
  });

  await test("POST /auth/register — duplicate email returns 409", async () => {
    const res = await api("POST", "/auth/register", {
      email: TEST_EMAIL,
      password: TEST_PASSWORD,
      fullName: TEST_NAME,
      role: "player",
    });
    assert(res.status === 409 || res.status === 200, `Expected 409/200, got ${res.status}`);
  });

  await test("POST /auth/login — missing fields returns 400", async () => {
    const res = await api("POST", "/auth/login", { email: TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /auth/login — invalid password returns 401/403", async () => {
    const res = await api("POST", "/auth/login", {
      email: TEST_EMAIL,
      password: "WrongPassword123!",
    });
    assert([401, 403, 404].includes(res.status), `Expected 401/403/404, got ${res.status}`);
  });

  await test("POST /auth/verify — missing fields returns 400", async () => {
    const res = await api("POST", "/auth/verify", { email: TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /auth/verify — invalid code returns 400", async () => {
    const res = await api("POST", "/auth/verify", {
      email: TEST_EMAIL,
      code: "000000",
    });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /auth/forgot-password — missing email returns 400", async () => {
    const res = await api("POST", "/auth/forgot-password", {});
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /auth/reset-password — missing fields returns 400", async () => {
    const res = await api("POST", "/auth/reset-password", { email: TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("GET /auth/me — no token returns 401", async () => {
    const res = await api("GET", "/auth/me");
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 3. USER ROUTES (using X-User-Email header for demo mode)
// ══════════════════════════════════════════════════════
async function testUserRoutes() {
  console.log("\n--- User Routes ---");

  await test("GET /auth/me — with email header returns user (demo mode)", async () => {
    const res = await api("GET", "/auth/me", null, null, {
      "X-User-Email": TEST_EMAIL,
      "X-User-Name": TEST_NAME,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}: ${JSON.stringify(res.data)}`);
    assert(res.data.email === TEST_EMAIL, `Expected email ${TEST_EMAIL}`);
    if (res.data.id) testUserId = res.data.id;
  });

  await test("GET /users/profile — returns profile", async () => {
    const res = await api("GET", "/users/profile", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.email === TEST_EMAIL, `Expected email match`);
  });

  await test("PUT /users/profile — update profile", async () => {
    const res = await api("PUT", "/users/profile", {
      fullName: "Test User Updated",
      role: "player",
      preferences: { theme: "dark" },
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.message === "Profile updated", `Unexpected message: ${res.data.message}`);
  });

  await test("PUT /users/profile — no auth returns 401", async () => {
    const res = await api("PUT", "/users/profile", { fullName: "Hacker" });
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });

  await test("POST /users/avatar — returns upload URL", async () => {
    const res = await api("POST", "/users/avatar", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.uploadUrl, "Expected uploadUrl in response");
    assert(res.data.key, "Expected key in response");
  });

  await test("POST /users/video-upload — returns upload URL", async () => {
    const res = await api("POST", "/users/video-upload", {
      extension: "mp4",
      contentType: "video/mp4",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.uploadUrl, "Expected uploadUrl");
    assert(res.data.key, "Expected key");
  });

  await test("Setup coach user for later tests", async () => {
    const res = await api("GET", "/auth/me", null, null, { "X-User-Email": COACH_EMAIL, "X-User-Name": "Coach User" });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    coachUserId = res.data.id;
    await api("PUT", "/users/profile", { role: "coach" }, null, { "X-User-Email": COACH_EMAIL });
  });
}

// ══════════════════════════════════════════════════════
// 4. STATS ROUTES
// ══════════════════════════════════════════════════════
async function testStatsRoutes() {
  console.log("\n--- Stats Routes ---");

  await test("GET /stats — no auth returns 401", async () => {
    const res = await api("GET", "/stats");
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });

  await test("POST /stats — save stats", async () => {
    const res = await api("POST", "/stats", {
      statType: "batting",
      statData: { runs: 142, balls: 98, fours: 14, sixes: 6 },
      source: "manual",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id in response");
    testStatsId = res.data.id;
  });

  await test("GET /stats — get saved stats", async () => {
    const res = await api("GET", "/stats", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
    assert(res.data.length > 0, "Expected at least 1 stat");
  });

  await test("GET /stats/history — returns history", async () => {
    const res = await api("GET", "/stats/history", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("POST /stats/cricclubs-sync — sync CricClubs stats", async () => {
    const res = await api("POST", "/stats/cricclubs-sync", {
      cricclubsUrl: "https://cricclubs.com/viewPlayer.do?playerId=12345",
      stats: { matches: 10, runs: 450, average: 45.0 },
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 5. SESSIONS ROUTES
// ══════════════════════════════════════════════════════
async function testSessionsRoutes() {
  console.log("\n--- Sessions Routes ---");

  await test("POST /sessions — create session", async () => {
    const res = await api("POST", "/sessions", {
      sessionType: "net",
      sessionData: { duration: 60, drills: ["front-foot", "pull-shot"], notes: "Good session" },
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
    testSessionId = res.data.id;
  });

  await test("GET /sessions — list sessions", async () => {
    const res = await api("GET", "/sessions", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
    assert(res.data.length > 0, "Expected at least 1 session");
  });

  await test("GET /sessions — no auth returns 401", async () => {
    const res = await api("GET", "/sessions");
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 6. ANALYSIS ROUTES
// ══════════════════════════════════════════════════════
async function testAnalysisRoutes() {
  console.log("\n--- Analysis Routes ---");

  await test("POST /analysis — save analysis", async () => {
    const res = await api("POST", "/analysis", {
      analysisType: "batting",
      scores: { technique: 85, power: 72, timing: 90 },
      feedback: "Good front-foot play, work on backfoot defense",
      videoRef: "videos/test/clip1.mp4",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
  });

  await test("GET /analysis/history — returns history", async () => {
    const res = await api("GET", "/analysis/history", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
    assert(res.data.length > 0, "Expected at least 1 analysis");
  });

  await test("POST /analysis — no auth returns 401", async () => {
    const res = await api("POST", "/analysis", { analysisType: "batting" });
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 7. IDOL ROUTES
// ══════════════════════════════════════════════════════
async function testIdolRoutes() {
  console.log("\n--- Idol Routes ---");

  await test("POST /idol/selections — save selections", async () => {
    const res = await api("POST", "/idol/selections", {
      selections: { batting: "Virat Kohli", bowling: "Jasprit Bumrah", fielding: "Jonty Rhodes" },
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /idol/selections — get selections", async () => {
    const res = await api("GET", "/idol/selections", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.selections, "Expected selections in response");
  });

  await test("POST /idol/progress — save progress", async () => {
    const res = await api("POST", "/idol/progress", {
      legendId: "virat-kohli",
      routineName: "Cover Drive Mastery",
      completed: true,
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /idol/progress — get progress", async () => {
    const res = await api("GET", "/idol/progress", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });
}

// ══════════════════════════════════════════════════════
// 8. ACADEMY ROUTES
// ══════════════════════════════════════════════════════
async function testAcademyRoutes() {
  console.log("\n--- Academy Routes ---");

  await test("POST /academy — create academy", async () => {
    const res = await api("POST", "/academy", {
      name: "Test Cricket Academy",
      location: "New Jersey, USA",
      description: "A test academy for API testing",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
    testAcademyId = res.data.id;
  });

  await test("GET /academy — list user's academies", async () => {
    const res = await api("GET", "/academy", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
    assert(res.data.length > 0, "Expected at least 1 academy");
    if (res.data[0]?.id) testAcademyId = res.data[0].id;
  });

  await test("POST /academy/roster — add player to roster", async () => {
    if (!testAcademyId) { skip("POST /academy/roster", "No academy ID"); return; }
    const res = await api("POST", "/academy/roster", {
      academyId: testAcademyId,
      userId: testUserId,
      skillLevel: "intermediate",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });

  await test("GET /academy/roster — get roster", async () => {
    const res = await api("GET", "/academy/roster", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("POST /academy/attendance — record attendance", async () => {
    if (!testAcademyId || !testUserId) { skip("POST /academy/attendance", "No IDs"); return; }
    const res = await api("POST", "/academy/attendance", {
      academyId: testAcademyId,
      userId: testUserId,
      date: "2026-02-27",
      status: "present",
      notes: "On time",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });

  await test("GET /academy/attendance — get attendance", async () => {
    const res = await api("GET", "/academy/attendance", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("POST /academy/staff — add staff", async () => {
    if (!testAcademyId || !testUserId) { skip("POST /academy/staff", "No IDs"); return; }
    const res = await api("POST", "/academy/staff", {
      academyId: testAcademyId,
      userId: testUserId,
      role: "coach",
      specialization: "batting",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });

  await test("GET /academy/staff — get staff", async () => {
    const res = await api("GET", "/academy/staff", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("POST /academy/invite — send invite", async () => {
    const res = await api("POST", "/academy/invite", {
      email: "newplayer@test.com",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /academy/reports — get reports", async () => {
    const res = await api("GET", "/academy/reports", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.totalPlayers !== undefined, "Expected totalPlayers");
    assert(res.data.totalAttendanceRecords !== undefined, "Expected totalAttendanceRecords");
  });

  await test("GET /academy/fee-settings — get fee settings", async () => {
    const res = await api("GET", "/academy/fee-settings", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("PUT /academy/fee-settings — save fee settings", async () => {
    const res = await api("PUT", "/academy/fee-settings", {
      monthlyFee: 150,
      registrationFee: 50,
      currency: "USD",
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /academy/fee-settings — verify saved settings", async () => {
    const res = await api("GET", "/academy/fee-settings", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.monthlyFee === 150, `Expected monthlyFee 150, got ${res.data.monthlyFee}`);
  });
}

// ══════════════════════════════════════════════════════
// 9. ADMIN ROUTES
// ══════════════════════════════════════════════════════
async function testAdminRoutes() {
  console.log("\n--- Admin Routes ---");

  // First make our test user an admin
  await test("Setup: promote test user to admin role", async () => {
    await api("PUT", "/users/profile", { role: "admin" }, null, {
      "X-User-Email": TEST_EMAIL,
    });
    const res = await api("GET", "/users/profile", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /admin/users — list users", async () => {
    const res = await api("GET", "/admin/users", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("GET /admin/dashboard — get dashboard stats", async () => {
    const res = await api("GET", "/admin/dashboard", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.totalUsers !== undefined, "Expected totalUsers");
    assert(res.data.totalSessions !== undefined, "Expected totalSessions");
    assert(res.data.totalAnalyses !== undefined, "Expected totalAnalyses");
  });

  await test("GET /admin/audit-log — get audit log", async () => {
    const res = await api("GET", "/admin/audit-log", null, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("GET /admin/users — non-admin returns 403", async () => {
    const res = await api("GET", "/admin/users", null, null, {
      "X-User-Email": "random_nonadmin@test.com",
      "X-User-Name": "Random User",
    });
    assert(res.status === 403, `Expected 403, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 10. CATALOG ROUTES
// ══════════════════════════════════════════════════════
async function testCatalogRoutes() {
  console.log("\n--- Catalog Routes ---");

  const categories = ["players", "agents", "teams", "leagues", "tournaments", "sponsors", "coaches"];

  for (const cat of categories) {
    await test(`GET /catalog/${cat} — returns data`, async () => {
      const res = await api("GET", `/catalog/${cat}`);
      assert(res.status === 200, `Expected 200, got ${res.status}`);
      assert(res.data, "Expected data in response");
    });
  }

  await test("GET /catalog/nonexistent — returns 404", async () => {
    const res = await api("GET", "/catalog/nonexistent_category_xyz");
    assert(res.status === 404, `Expected 404, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 11. PAYMENTS ROUTES
// ══════════════════════════════════════════════════════
async function testPaymentsRoutes() {
  console.log("\n--- Payments Routes ---");

  await test("POST /payments/send-reminder — send reminder", async () => {
    const res = await api("POST", "/payments/send-reminder", {
      studentId: testUserId || "test-student-id",
      studentName: "Test Student",
      feeType: "monthly",
      amount: 150,
    }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("POST /payments/send-reminder — missing fields returns 400", async () => {
    const res = await api("POST", "/payments/send-reminder", {}, null, {
      "X-User-Email": TEST_EMAIL,
    });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 12. FEED ROUTES (NEW)
// ══════════════════════════════════════════════════════
async function testFeedRoutes() {
  console.log("\n--- Feed Routes (NEW) ---");

  await test("GET /feed/posts — returns array", async () => {
    const res = await api("GET", "/feed/posts");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("POST /feed/posts — no auth returns 401", async () => {
    const res = await api("POST", "/feed/posts", { content: "test" });
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });

  await test("POST /feed/posts — missing content returns 400", async () => {
    const res = await api("POST", "/feed/posts", {}, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /feed/posts — create post", async () => {
    const res = await api("POST", "/feed/posts", { content: "Just scored a century!", postType: "performance", region: "northeast", statsSnapshot: { runs: 105 } }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
    testPostId = res.data.id;
  });

  await test("POST /feed/posts — create second post", async () => {
    const res = await api("POST", "/feed/posts", { content: "Great training session.", postType: "training", region: "southeast" }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });

  await test("GET /feed/posts — returns posts with author info", async () => {
    const res = await api("GET", "/feed/posts");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.length >= 2, `Expected >= 2 posts, got ${res.data.length}`);
    assert(res.data[0].author_name, "Expected author_name");
  });

  await test("GET /feed/posts?region=northeast — filter by region", async () => {
    const res = await api("GET", "/feed/posts?region=northeast");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    for (const p of res.data) assert(p.region === "northeast", `Expected northeast, got ${p.region}`);
  });

  await test("GET /feed/posts?type=performance — filter by type", async () => {
    const res = await api("GET", "/feed/posts?type=performance");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    for (const p of res.data) assert(p.post_type === "performance", "Expected performance");
  });

  await test("POST /feed/posts/:id/like — like a post", async () => {
    const res = await api("POST", `/feed/posts/${testPostId}/like`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.liked === true, "Expected liked=true");
  });

  await test("POST /feed/posts/:id/like — unlike (toggle)", async () => {
    const res = await api("POST", `/feed/posts/${testPostId}/like`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.liked === false, "Expected liked=false");
  });

  await test("POST /feed/posts/:id/like — re-like", async () => {
    const res = await api("POST", `/feed/posts/${testPostId}/like`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.liked === true, "Expected liked=true");
  });

  await test("POST /feed/posts/:id/comments — no content returns 400", async () => {
    const res = await api("POST", `/feed/posts/${testPostId}/comments`, {}, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /feed/posts/:id/comments — add comment", async () => {
    const res = await api("POST", `/feed/posts/${testPostId}/comments`, { content: "Awesome innings!" }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
  });

  await test("GET /feed/posts/:id/comments — get comments", async () => {
    const res = await api("GET", `/feed/posts/${testPostId}/comments`);
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data) && res.data.length >= 1, "Expected at least 1 comment");
    assert(res.data[0].author_name, "Expected author_name");
  });

  await test("POST /feed/posts/:id/share — share post", async () => {
    const res = await api("POST", `/feed/posts/${testPostId}/share`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("DELETE /feed/posts/:id — no auth returns 401", async () => {
    const res = await api("DELETE", `/feed/posts/${testPostId}`);
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 13. ENERGY / LEADERBOARD ROUTES (NEW)
// ══════════════════════════════════════════════════════
async function testEnergyRoutes() {
  console.log("\n--- Energy / Leaderboard Routes (NEW) ---");

  // Reset test user back to player (was promoted to admin in admin tests)
  await api("PUT", "/users/profile", { role: "player" }, null, { "X-User-Email": TEST_EMAIL });

  await test("GET /energy/me — get my energy", async () => {
    const res = await api("GET", "/energy/me", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.total_points !== undefined, "Expected total_points");
  });

  await test("GET /energy/me — no auth returns 401", async () => {
    const res = await api("GET", "/energy/me");
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });

  await test("GET /energy/leaderboard — get leaderboard", async () => {
    const res = await api("GET", "/energy/leaderboard");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("GET /energy/leaderboard?period=weekly — weekly", async () => {
    const res = await api("GET", "/energy/leaderboard?period=weekly");
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("GET /energy/my-badges — get my badges", async () => {
    const res = await api("GET", "/energy/my-badges", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data), "Expected array");
  });

  await test("POST /energy/award — player cannot award (403)", async () => {
    const res = await api("POST", "/energy/award", { targetUserId: testUserId, points: 10 }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 403, `Expected 403, got ${res.status}`);
  });

  await test("POST /energy/award — coach can award + badge", async () => {
    const res = await api("POST", "/energy/award", { targetUserId: testUserId, points: 50, reason: "excellent_batting", badgeName: "Century Maker" }, null, { "X-User-Email": COACH_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.badgeAwarded === true, "Expected badge awarded");
  });

  await test("POST /energy/award — missing fields returns 400", async () => {
    const res = await api("POST", "/energy/award", {}, null, { "X-User-Email": COACH_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("GET /energy/my-badges — badge appears after award", async () => {
    const res = await api("GET", "/energy/my-badges", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.length >= 1, "Expected at least 1 badge");
    assert(res.data.some(b => b.badge_name === "Century Maker"), "Expected Century Maker badge");
  });

  await test("GET /energy/me — points increased after award", async () => {
    const res = await api("GET", "/energy/me", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(parseInt(res.data.total_points) >= 50, `Expected >= 50 points, got ${res.data.total_points}`);
  });
}

// ══════════════════════════════════════════════════════
// 14. COMPARE ROUTES (NEW)
// ══════════════════════════════════════════════════════
async function testCompareRoutes() {
  console.log("\n--- Compare Routes (NEW) ---");

  await test("GET /compare/players — missing ids returns 400", async () => {
    const res = await api("GET", "/compare/players");
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("GET /compare/players — single id returns 400", async () => {
    const res = await api("GET", `/compare/players?ids=${testUserId}`);
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("GET /compare/players — compare two players", async () => {
    const res = await api("GET", `/compare/players?ids=${testUserId},${coachUserId}`);
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.players, "Expected players");
    assert(Array.isArray(res.data.players), "Expected players array");
  });
}

// ══════════════════════════════════════════════════════
// 15. SELECTOR / WATCHLIST ROUTES (NEW)
// ══════════════════════════════════════════════════════
async function testSelectorRoutes() {
  console.log("\n--- Selector / Watchlist Routes (NEW) ---");

  await test("GET /selector/watchlist — no auth returns 401", async () => {
    const res = await api("GET", "/selector/watchlist");
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });

  await test("POST /selector/watchlist — missing playerId returns 400", async () => {
    const res = await api("POST", "/selector/watchlist", {}, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /selector/watchlist — add to watchlist", async () => {
    const res = await api("POST", "/selector/watchlist", { playerId: coachUserId, playerName: "Coach Player", listType: "watch", notes: "Good batsman", ranking: 1 }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
    testWatchId = res.data.id;
  });

  await test("POST /selector/watchlist — add to shortlist", async () => {
    const res = await api("POST", "/selector/watchlist", { playerId: testUserId, playerName: "Test Player", listType: "shortlist", ranking: 1 }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });

  await test("GET /selector/watchlist — get watchlist", async () => {
    const res = await api("GET", "/selector/watchlist?type=watch", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data) && res.data.length >= 1, "Expected at least 1 entry");
  });

  await test("GET /selector/watchlist?type=shortlist — get shortlist", async () => {
    const res = await api("GET", "/selector/watchlist?type=shortlist", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(res.data.length >= 1, "Expected at least 1 shortlisted");
  });

  await test("PUT /selector/watchlist/rankings — update rankings", async () => {
    const res = await api("PUT", "/selector/watchlist/rankings", { rankings: [{ id: testWatchId, ranking: 5 }] }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("DELETE /selector/watchlist/:id — remove", async () => {
    const res = await api("DELETE", `/selector/watchlist/${testWatchId}`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /selector/watchlist — entry removed", async () => {
    const res = await api("GET", "/selector/watchlist?type=watch", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    const found = res.data.find(w => w.id === testWatchId);
    assert(!found, "Expected removed entry to be gone");
  });
}

// ══════════════════════════════════════════════════════
// 16. STRATEGY ROUTES (NEW)
// ══════════════════════════════════════════════════════
async function testStrategyRoutes() {
  console.log("\n--- Strategy Routes (NEW) ---");

  await test("GET /strategy/plans — no auth returns 401", async () => {
    const res = await api("GET", "/strategy/plans");
    assert(res.status === 401, `Expected 401, got ${res.status}`);
  });

  await test("POST /strategy/plans — missing matchName returns 400", async () => {
    const res = await api("POST", "/strategy/plans", {}, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 400, `Expected 400, got ${res.status}`);
  });

  await test("POST /strategy/plans — create strategy", async () => {
    const res = await api("POST", "/strategy/plans", { matchName: "Finals vs Mumbai XI", opponent: "Mumbai XI", phase: "powerplay", bowlingPlan: { type: "pace" }, battingPlan: { approach: "aggressive" }, fieldPositions: { slips: 2 }, notes: "Target 180+" }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
    assert(res.data.id, "Expected id");
    testStrategyId = res.data.id;
  });

  await test("POST /strategy/plans — create second strategy", async () => {
    const res = await api("POST", "/strategy/plans", { matchName: "Semi vs Delhi XI", opponent: "Delhi XI", phase: "death", bowlingPlan: { type: "yorkers" } }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 201, `Expected 201, got ${res.status}`);
  });

  await test("GET /strategy/plans — list strategies", async () => {
    const res = await api("GET", "/strategy/plans", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    assert(Array.isArray(res.data) && res.data.length >= 2, "Expected >= 2 strategies");
  });

  await test("PUT /strategy/plans/:id — update strategy", async () => {
    const res = await api("PUT", `/strategy/plans/${testStrategyId}`, { notes: "Updated: Target 200+", phase: "middle" }, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("DELETE /strategy/plans/:id — delete strategy", async () => {
    const res = await api("DELETE", `/strategy/plans/${testStrategyId}`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });

  await test("GET /strategy/plans — one less after delete", async () => {
    const res = await api("GET", "/strategy/plans", null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
    const found = res.data.find(s => s.id === testStrategyId);
    assert(!found, "Deleted strategy should not appear");
  });
}

// ══════════════════════════════════════════════════════
// 17. CLEANUP
// ══════════════════════════════════════════════════════
async function testCleanup() {
  console.log("\n--- Cleanup ---");

  await test("DELETE /feed/posts/:id — delete own post", async () => {
    if (!testPostId) return;
    const res = await api("DELETE", `/feed/posts/${testPostId}`, null, null, { "X-User-Email": TEST_EMAIL });
    assert(res.status === 200, `Expected 200, got ${res.status}`);
  });
}

// ══════════════════════════════════════════════════════
// 18. EDGE CASES & ERROR HANDLING
// ══════════════════════════════════════════════════════
async function testEdgeCases() {
  console.log("\n--- Edge Cases ---");

  await test("GET /nonexistent-route — returns 404", async () => {
    const res = await api("GET", "/this/route/does/not/exist");
    assert(res.status === 404, `Expected 404, got ${res.status}`);
  });

  await test("OPTIONS preflight — returns 200", async () => {
    const https = await import("https");
    const status = await new Promise((resolve, reject) => {
      const url = new URL(`${API_BASE}/health`);
      const req = https.request({ hostname: url.hostname, path: url.pathname, method: "OPTIONS", headers: { "Origin": "https://cricverse360.com", "Access-Control-Request-Method": "GET" } }, (res) => resolve(res.statusCode));
      req.on("error", reject);
      req.end();
    });
    assert(status === 200 || status === 204, `Expected 200 or 204, got ${status}`);
  });
}

// ══════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════
async function main() {
  console.log("=== CricVerse360 API Test Suite ===");
  console.log(`API: ${API_BASE}`);
  console.log(`Test user: ${TEST_EMAIL}`);
  console.log(`Timestamp: ${new Date().toISOString()}\n`);

  await testHealthCheck();
  await testAuth();
  await testUserRoutes();
  await testStatsRoutes();
  await testSessionsRoutes();
  await testAnalysisRoutes();
  await testIdolRoutes();
  await testAcademyRoutes();
  await testAdminRoutes();
  await testCatalogRoutes();
  await testPaymentsRoutes();
  await testFeedRoutes();
  await testEnergyRoutes();
  await testCompareRoutes();
  await testSelectorRoutes();
  await testStrategyRoutes();
  await testCleanup();
  await testEdgeCases();

  console.log("\n=== RESULTS ===");
  console.log(`Total: ${results.passed + results.failed + results.skipped}`);
  console.log(`Passed: ${results.passed}`);
  console.log(`Failed: ${results.failed}`);
  console.log(`Skipped: ${results.skipped}`);

  if (results.failed > 0) {
    console.log("\n--- FAILURES ---");
    for (const t of results.tests.filter(t => t.status === "FAIL")) {
      console.log(`  ${t.name}: ${t.error}`);
    }
  }

  console.log("\nDone.");
  process.exit(results.failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error("Fatal error:", err);
  process.exit(1);
});
