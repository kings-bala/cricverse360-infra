import { RDSDataClient, ExecuteStatementCommand } from "@aws-sdk/client-rds-data";
import { RDSClient, StopDBClusterCommand, StartDBClusterCommand, DescribeDBClustersCommand } from "@aws-sdk/client-rds";
import {
  CognitoIdentityProviderClient,
  SignUpCommand,
  InitiateAuthCommand,
  ConfirmSignUpCommand,
  ForgotPasswordCommand,
  ConfirmForgotPasswordCommand,
  GetUserCommand,
  AdminGetUserCommand,
  AdminUpdateUserAttributesCommand,
  AdminDisableUserCommand,
  AdminEnableUserCommand,
  ListUsersCommand,
} from "@aws-sdk/client-cognito-identity-provider";
import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import crypto from "crypto";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const region = process.env.REGION || "us-east-1";
const rds = new RDSDataClient({ region });
const rdsControl = new RDSClient({ region });
const cognito = new CognitoIdentityProviderClient({ region });
const s3 = new S3Client({ region });

const DB_CLUSTER_ARN = process.env.DB_CLUSTER_ARN;
const DB_SECRET_ARN = process.env.DB_SECRET_ARN;
const DB_NAME = process.env.DB_NAME;
const USER_POOL_ID = process.env.USER_POOL_ID;
const USER_POOL_CLIENT_ID = process.env.USER_POOL_CLIENT_ID;
const BUCKET_NAME = process.env.BUCKET_NAME;
const DB_CLUSTER_ID = process.env.DB_CLUSTER_ID;

function respond(statusCode, body) {
  return {
    statusCode,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-User-Email,X-User-Name",
      "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
    },
    body: JSON.stringify(body),
  };
}

async function runSql(sql, parameters = []) {
  const cmd = new ExecuteStatementCommand({
    resourceArn: DB_CLUSTER_ARN,
    secretArn: DB_SECRET_ARN,
    database: DB_NAME,
    sql,
    parameters,
    includeResultMetadata: true,
  });
  return rds.send(cmd);
}

function parseRows(result) {
  if (!result.records || !result.columnMetadata) return [];
  const cols = result.columnMetadata.map((c) => c.name);
  return result.records.map((row) => {
    const obj = {};
    row.forEach((field, i) => {
      const val = field.stringValue ?? field.longValue ?? field.doubleValue ?? field.booleanValue ?? null;
      obj[cols[i]] = val;
    });
    return obj;
  });
}

async function getUserFromToken(event) {
  const authHeader = event.headers?.Authorization || event.headers?.authorization || "";
  const token = authHeader.replace("Bearer ", "");

  // Try Cognito token first
  if (token) {
    try {
      const cmd = new GetUserCommand({ AccessToken: token });
      const user = await cognito.send(cmd);
      const attrs = {};
      for (const attr of user.UserAttributes || []) {
        attrs[attr.Name] = attr.Value;
      }
      return { username: user.Username, email: attrs.email || user.Username, ...attrs };
    } catch {
      // token invalid, fall through to email header
    }
  }

  // Demo mode: identify user by X-User-Email header
  const emailHeader = event.headers?.["X-User-Email"] || event.headers?.["x-user-email"] || "";
  if (emailHeader) {
    // Auto-create user in DB if not exists
    const existing = await runSql("SELECT id, email, full_name FROM users WHERE email = :email", [
      { name: "email", value: { stringValue: emailHeader } },
    ]);
    const rows = parseRows(existing);
    if (rows.length > 0) {
      return { username: rows[0].email, email: rows[0].email, sub: rows[0].id, name: rows[0].full_name };
    }
    // Create new user from header info
    const nameHeader = event.headers?.["X-User-Name"] || event.headers?.["x-user-name"] || emailHeader.split("@")[0];
    const userId = crypto.randomUUID();
    await runSql(
      "INSERT INTO users (id, email, full_name, role) VALUES (:id, :email, :name, 'player') ON CONFLICT (email) DO NOTHING",
      [
        { name: "id", value: { stringValue: userId } },
        { name: "email", value: { stringValue: emailHeader } },
        { name: "name", value: { stringValue: nameHeader } },
      ]
    );
    return { username: emailHeader, email: emailHeader, sub: userId, name: nameHeader };
  }

  return null;
}

async function initDb() {
  const tables = [
    `CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      full_name TEXT NOT NULL,
      role TEXT DEFAULT 'player',
      academy TEXT DEFAULT '',
      avatar_url TEXT DEFAULT '',
      preferences JSONB DEFAULT '{}',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS player_stats (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      stat_type TEXT NOT NULL,
      stat_data JSONB NOT NULL,
      source TEXT DEFAULT 'manual',
      recorded_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS sessions (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      session_type TEXT NOT NULL,
      session_data JSONB NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS analysis (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      analysis_type TEXT NOT NULL,
      scores JSONB DEFAULT '{}',
      feedback TEXT DEFAULT '',
      video_ref TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS idol_selections (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      selections JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS idol_progress (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      legend_id TEXT NOT NULL,
      routine_name TEXT NOT NULL,
      completed BOOLEAN DEFAULT FALSE,
      completed_at TIMESTAMP
    )`,
    `CREATE TABLE IF NOT EXISTS academies (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      owner_id TEXT NOT NULL REFERENCES users(id),
      location TEXT DEFAULT '',
      description TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS academy_roster (
      id TEXT PRIMARY KEY,
      academy_id TEXT NOT NULL REFERENCES academies(id),
      user_id TEXT NOT NULL REFERENCES users(id),
      skill_level TEXT DEFAULT 'beginner',
      joined_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS attendance (
      id TEXT PRIMARY KEY,
      academy_id TEXT NOT NULL REFERENCES academies(id),
      user_id TEXT NOT NULL REFERENCES users(id),
      date TEXT NOT NULL,
      status TEXT DEFAULT 'present',
      notes TEXT DEFAULT ''
    )`,
    `CREATE TABLE IF NOT EXISTS academy_staff (
      id TEXT PRIMARY KEY,
      academy_id TEXT NOT NULL REFERENCES academies(id),
      user_id TEXT NOT NULL REFERENCES users(id),
      role TEXT NOT NULL,
      specialization TEXT DEFAULT '',
      joined_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS audit_log (
      id TEXT PRIMARY KEY,
      admin_id TEXT NOT NULL,
      action TEXT NOT NULL,
      target_id TEXT DEFAULT '',
      details JSONB DEFAULT '{}',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS catalog (
      category TEXT PRIMARY KEY,
      data JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS academy_fee_settings (
      id TEXT PRIMARY KEY,
      academy_id TEXT NOT NULL UNIQUE,
      settings JSONB NOT NULL,
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS feed_posts (
      id TEXT PRIMARY KEY,
      author_id TEXT NOT NULL REFERENCES users(id),
      content TEXT NOT NULL,
      post_type TEXT DEFAULT 'general',
      region TEXT DEFAULT 'all',
      stats_snapshot JSONB DEFAULT '{}',
      media_url TEXT DEFAULT '',
      like_count INTEGER DEFAULT 0,
      comment_count INTEGER DEFAULT 0,
      share_count INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS feed_comments (
      id TEXT PRIMARY KEY,
      post_id TEXT NOT NULL REFERENCES feed_posts(id) ON DELETE CASCADE,
      author_id TEXT NOT NULL REFERENCES users(id),
      content TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS feed_likes (
      id TEXT PRIMARY KEY,
      post_id TEXT NOT NULL REFERENCES feed_posts(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(post_id, user_id)
    )`,
    `CREATE TABLE IF NOT EXISTS energy_scores (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      total_points INTEGER DEFAULT 0,
      weekly_points INTEGER DEFAULT 0,
      level TEXT DEFAULT 'rookie',
      streak_days INTEGER DEFAULT 0,
      last_activity TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(user_id)
    )`,
    `CREATE TABLE IF NOT EXISTS badges (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      badge_type TEXT NOT NULL,
      badge_name TEXT NOT NULL,
      awarded_by TEXT DEFAULT '',
      awarded_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS watchlists (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      player_id TEXT NOT NULL,
      player_name TEXT DEFAULT '',
      list_type TEXT DEFAULT 'watch',
      notes TEXT DEFAULT '',
      ranking INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS match_strategies (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      match_name TEXT NOT NULL,
      opponent TEXT DEFAULT '',
      phase TEXT DEFAULT 'powerplay',
      bowling_plan JSONB DEFAULT '{}',
      batting_plan JSONB DEFAULT '{}',
      field_positions JSONB DEFAULT '{}',
      notes TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS drills (
      id TEXT PRIMARY KEY,
      author_id TEXT NOT NULL REFERENCES users(id),
      title TEXT NOT NULL,
      description TEXT DEFAULT '',
      video_url TEXT DEFAULT '',
      video_key TEXT DEFAULT '',
      thumbnail_url TEXT DEFAULT '',
      category TEXT DEFAULT 'batting',
      skill_level TEXT DEFAULT 'beginner',
      duration_minutes INTEGER DEFAULT 0,
      tags JSONB DEFAULT '[]',
      like_count INTEGER DEFAULT 0,
      comment_count INTEGER DEFAULT 0,
      share_count INTEGER DEFAULT 0,
      visibility TEXT DEFAULT 'public',
      academy_id TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS drill_likes (
      id TEXT PRIMARY KEY,
      drill_id TEXT NOT NULL REFERENCES drills(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES users(id),
      created_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(drill_id, user_id)
    )`,
    `CREATE TABLE IF NOT EXISTS drill_comments (
      id TEXT PRIMARY KEY,
      drill_id TEXT NOT NULL REFERENCES drills(id) ON DELETE CASCADE,
      author_id TEXT NOT NULL REFERENCES users(id),
      content TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
  ];
  for (const sql of tables) {
    await runSql(sql);
  }
  const migrations = [
    "DROP TABLE IF EXISTS badges CASCADE",
    `CREATE TABLE IF NOT EXISTS badges (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      badge_type TEXT NOT NULL DEFAULT '',
      badge_name TEXT NOT NULL DEFAULT '',
      awarded_by TEXT DEFAULT '',
      awarded_at TIMESTAMP DEFAULT NOW()
    )`,
  ];
  for (const m of migrations) {
    try { await runSql(m); } catch(e) { /* migration may fail if already applied */ }
  }
}

let catalogSeeded = false;
async function seedCatalog() {
  if (catalogSeeded) return;
  const check = await runSql("SELECT COUNT(*) as count FROM catalog");
  const rows = parseRows(check);
  if (parseInt(rows[0]?.count || "0", 10) > 0) { catalogSeeded = true; return; }
  try {
    const __dirname = dirname(fileURLToPath(import.meta.url));
    const raw = readFileSync(join(__dirname, "seed", "catalog.json"), "utf8");
    const seed = JSON.parse(raw);
    const categories = [
      ["players", seed.players],
      ["agents", seed.agents],
      ["teams", seed.t20Teams],
      ["leagues", seed.t20Leagues],
      ["tournaments", seed.tournaments],
      ["sponsors", seed.sponsors],
      ["available_sponsorships", seed.availableSponsorships],
      ["coaches", seed.coaches],
      ["match_history", seed.playerMatchHistory],
      ["combine_data", seed.playerCombineData],
      ["performance_feed", seed.performanceFeedItems],
    ];
    for (const [cat, data] of categories) {
      await runSql(
        "INSERT INTO catalog (category, data) VALUES (:cat, :data::jsonb) ON CONFLICT (category) DO NOTHING",
        [
          { name: "cat", value: { stringValue: cat } },
          { name: "data", value: { stringValue: JSON.stringify(data) } },
        ]
      );
    }
    catalogSeeded = true;
  } catch (e) {
    console.error("seedCatalog error:", e);
  }
}

let dbInitialized = false;

// ─── Route Handlers ───

async function handleAuthRegister(body) {
  const { email, password, fullName, role } = body;
  if (!email || !password || !fullName) {
    return respond(400, { error: "email, password, and fullName are required" });
  }
  try {
    await cognito.send(new SignUpCommand({
      ClientId: USER_POOL_CLIENT_ID,
      Username: email,
      Password: password,
      UserAttributes: [
        { Name: "email", Value: email },
        { Name: "name", Value: fullName },
        { Name: "custom:role", Value: role || "player" },
      ],
    }));
    const userId = crypto.randomUUID();
    await runSql(
      "INSERT INTO users (id, email, full_name, role) VALUES (:id, :email, :name, :role) ON CONFLICT (email) DO NOTHING",
      [
        { name: "id", value: { stringValue: userId } },
        { name: "email", value: { stringValue: email } },
        { name: "name", value: { stringValue: fullName } },
        { name: "role", value: { stringValue: role || "player" } },
      ]
    );
    return respond(200, { message: "Registration successful. Please verify your email.", userId });
  } catch (err) {
    if (err.name === "UsernameExistsException") {
      return respond(409, { error: "An account with this email already exists" });
    }
    return respond(500, { error: err.message });
  }
}

async function handleAuthLogin(body) {
  const { email, password } = body;
  if (!email || !password) {
    return respond(400, { error: "email and password are required" });
  }
  try {
    const result = await cognito.send(new InitiateAuthCommand({
      AuthFlow: "USER_PASSWORD_AUTH",
      ClientId: USER_POOL_CLIENT_ID,
      AuthParameters: { USERNAME: email, PASSWORD: password },
    }));
    const tokens = result.AuthenticationResult;
    return respond(200, {
      accessToken: tokens.AccessToken,
      refreshToken: tokens.RefreshToken,
      idToken: tokens.IdToken,
      expiresIn: tokens.ExpiresIn,
    });
  } catch (err) {
    if (err.name === "NotAuthorizedException") {
      return respond(401, { error: "Invalid email or password" });
    }
    if (err.name === "UserNotConfirmedException") {
      return respond(403, { error: "Please verify your email first" });
    }
    if (err.name === "UserNotFoundException") {
      return respond(404, { error: "No account found with this email" });
    }
    return respond(500, { error: err.message });
  }
}

async function handleAuthVerify(body) {
  const { email, code } = body;
  if (!email || !code) return respond(400, { error: "email and code are required" });
  try {
    await cognito.send(new ConfirmSignUpCommand({
      ClientId: USER_POOL_CLIENT_ID,
      Username: email,
      ConfirmationCode: code,
    }));
    return respond(200, { message: "Email verified successfully" });
  } catch (err) {
    return respond(400, { error: err.message });
  }
}

async function handleForgotPassword(body) {
  const { email } = body;
  if (!email) return respond(400, { error: "email is required" });
  try {
    await cognito.send(new ForgotPasswordCommand({
      ClientId: USER_POOL_CLIENT_ID,
      Username: email,
    }));
    return respond(200, { message: "Password reset code sent to your email" });
  } catch (err) {
    return respond(500, { error: err.message });
  }
}

async function handleResetPassword(body) {
  const { email, code, newPassword } = body;
  if (!email || !code || !newPassword) return respond(400, { error: "email, code, and newPassword are required" });
  try {
    await cognito.send(new ConfirmForgotPasswordCommand({
      ClientId: USER_POOL_CLIENT_ID,
      Username: email,
      ConfirmationCode: code,
      Password: newPassword,
    }));
    return respond(200, { message: "Password reset successful" });
  } catch (err) {
    return respond(400, { error: err.message });
  }
}

async function handleAuthMe(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql("SELECT * FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "User profile not found" });
  return respond(200, rows[0]);
}

async function handleGetProfile(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql("SELECT * FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "Profile not found" });
  return respond(200, rows[0]);
}

async function handleUpdateProfile(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { fullName, role, academy, preferences } = body;
  await runSql(
    `UPDATE users SET
      full_name = COALESCE(:name, full_name),
      role = COALESCE(:role, role),
      academy = COALESCE(:academy, academy),
      preferences = COALESCE(:prefs::jsonb, preferences),
      updated_at = NOW()
    WHERE email = :email`,
    [
      { name: "name", value: fullName ? { stringValue: fullName } : { isNull: true } },
      { name: "role", value: role ? { stringValue: role } : { isNull: true } },
      { name: "academy", value: academy ? { stringValue: academy } : { isNull: true } },
      { name: "prefs", value: preferences ? { stringValue: JSON.stringify(preferences) } : { isNull: true } },
      { name: "email", value: { stringValue: user.email } },
    ]
  );
  return respond(200, { message: "Profile updated" });
}

async function handleAvatarUpload(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const key = `avatars/${user.sub || user.username}.jpg`;
  const url = await getSignedUrl(s3, new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    ContentType: "image/jpeg",
  }), { expiresIn: 300 });
  return respond(200, { uploadUrl: url, key });
}

async function handleVideoUpload(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const ext = body.extension || "mp4";
  const contentType = body.contentType || "video/mp4";
  const key = `videos/${user.sub || user.username}/${Date.now()}.${ext}`;
  const url = await getSignedUrl(s3, new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    ContentType: contentType,
  }), { expiresIn: 600 });
  return respond(200, { uploadUrl: url, key });
}

async function handleGetStats(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM player_stats WHERE user_id = (SELECT id FROM users WHERE email = :email) ORDER BY recorded_at DESC",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostStats(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { statType, statData, source } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO player_stats (id, user_id, stat_type, stat_data, source) VALUES (:id, (SELECT id FROM users WHERE email = :email), :type, :data::jsonb, :source)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "type", value: { stringValue: statType || "general" } },
      { name: "data", value: { stringValue: JSON.stringify(statData || {}) } },
      { name: "source", value: { stringValue: source || "manual" } },
    ]
  );
  return respond(201, { id, message: "Stats saved" });
}

async function handleCricclubsSync(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { cricclubsUrl, stats } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO player_stats (id, user_id, stat_type, stat_data, source) VALUES (:id, (SELECT id FROM users WHERE email = :email), 'cricclubs', :data::jsonb, :source)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "data", value: { stringValue: JSON.stringify(stats || {}) } },
      { name: "source", value: { stringValue: cricclubsUrl || "cricclubs" } },
    ]
  );
  return respond(201, { id, message: "CricClubs stats synced" });
}

async function handleStatsHistory(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM player_stats WHERE user_id = (SELECT id FROM users WHERE email = :email) ORDER BY recorded_at DESC LIMIT 50",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handleGetSessions(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM sessions WHERE user_id = (SELECT id FROM users WHERE email = :email) ORDER BY created_at DESC",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostSession(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { sessionType, sessionData } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO sessions (id, user_id, session_type, session_data) VALUES (:id, (SELECT id FROM users WHERE email = :email), :type, :data::jsonb)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "type", value: { stringValue: sessionType || "net" } },
      { name: "data", value: { stringValue: JSON.stringify(sessionData || {}) } },
    ]
  );
  return respond(201, { id, message: "Session saved" });
}

async function handleGetSessionById(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const sessionId = event.pathParameters?.sessionId;
  const result = await runSql(
    "SELECT * FROM sessions WHERE id = :id AND user_id = (SELECT id FROM users WHERE email = :email)",
    [
      { name: "id", value: { stringValue: sessionId } },
      { name: "email", value: { stringValue: user.email } },
    ]
  );
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "Session not found" });
  return respond(200, rows[0]);
}

async function handlePostAnalysis(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { analysisType, scores, feedback, videoRef } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO analysis (id, user_id, analysis_type, scores, feedback, video_ref) VALUES (:id, (SELECT id FROM users WHERE email = :email), :type, :scores::jsonb, :feedback, :ref)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "type", value: { stringValue: analysisType || "general" } },
      { name: "scores", value: { stringValue: JSON.stringify(scores || {}) } },
      { name: "feedback", value: { stringValue: feedback || "" } },
      { name: "ref", value: { stringValue: videoRef || "" } },
    ]
  );
  return respond(201, { id, message: "Analysis saved" });
}

async function handleAnalysisHistory(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM analysis WHERE user_id = (SELECT id FROM users WHERE email = :email) ORDER BY created_at DESC LIMIT 50",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handleGetIdolSelections(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM idol_selections WHERE user_id = (SELECT id FROM users WHERE email = :email) ORDER BY updated_at DESC LIMIT 1",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  const rows = parseRows(result);
  return respond(200, rows[0] || { selections: {} });
}

async function handlePostIdolSelections(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const id = crypto.randomUUID();
  await runSql(
    `INSERT INTO idol_selections (id, user_id, selections, updated_at)
     VALUES (:id, (SELECT id FROM users WHERE email = :email), :sel::jsonb, NOW())`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "sel", value: { stringValue: JSON.stringify(body.selections || {}) } },
    ]
  );
  return respond(200, { message: "Selections saved" });
}

async function handleGetIdolProgress(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM idol_progress WHERE user_id = (SELECT id FROM users WHERE email = :email)",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostIdolProgress(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { legendId, routineName, completed } = body;
  const id = crypto.randomUUID();
  await runSql(
    `INSERT INTO idol_progress (id, user_id, legend_id, routine_name, completed, completed_at)
     VALUES (:id, (SELECT id FROM users WHERE email = :email), :lid, :rname, :done, NOW())
     ON CONFLICT DO NOTHING`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "lid", value: { stringValue: legendId || "" } },
      { name: "rname", value: { stringValue: routineName || "" } },
      { name: "done", value: { booleanValue: completed !== false } },
    ]
  );
  return respond(200, { message: "Progress saved" });
}

async function handleGetAcademy(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM academies WHERE owner_id = (SELECT id FROM users WHERE email = :email)",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostAcademy(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { name, location, description } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO academies (id, name, owner_id, location, description) VALUES (:id, :name, (SELECT id FROM users WHERE email = :email), :loc, :desc)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "name", value: { stringValue: name || "" } },
      { name: "email", value: { stringValue: user.email } },
      { name: "loc", value: { stringValue: location || "" } },
      { name: "desc", value: { stringValue: description || "" } },
    ]
  );
  return respond(201, { id, message: "Academy created" });
}

async function handleGetRoster(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    `SELECT ar.*, u.full_name, u.email FROM academy_roster ar
     JOIN users u ON ar.user_id = u.id
     JOIN academies a ON ar.academy_id = a.id
     WHERE a.owner_id = (SELECT id FROM users WHERE email = :email)`,
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostRoster(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { academyId, userId, skillLevel } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO academy_roster (id, academy_id, user_id, skill_level) VALUES (:id, :aid, :uid, :level)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "aid", value: { stringValue: academyId || "" } },
      { name: "uid", value: { stringValue: userId || "" } },
      { name: "level", value: { stringValue: skillLevel || "beginner" } },
    ]
  );
  return respond(201, { id, message: "Player added to roster" });
}

async function handleGetAttendance(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    `SELECT att.*, u.full_name FROM attendance att
     JOIN users u ON att.user_id = u.id
     JOIN academies a ON att.academy_id = a.id
     WHERE a.owner_id = (SELECT id FROM users WHERE email = :email)
     ORDER BY att.date DESC LIMIT 100`,
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostAttendance(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { academyId, userId, date, status, notes } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO attendance (id, academy_id, user_id, date, status, notes) VALUES (:id, :aid, :uid, :date, :status, :notes)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "aid", value: { stringValue: academyId || "" } },
      { name: "uid", value: { stringValue: userId || "" } },
      { name: "date", value: { stringValue: date || new Date().toISOString().split("T")[0] } },
      { name: "status", value: { stringValue: status || "present" } },
      { name: "notes", value: { stringValue: notes || "" } },
    ]
  );
  return respond(201, { id, message: "Attendance recorded" });
}

async function handleGetStaff(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    `SELECT s.*, u.full_name, u.email FROM academy_staff s
     JOIN users u ON s.user_id = u.id
     JOIN academies a ON s.academy_id = a.id
     WHERE a.owner_id = (SELECT id FROM users WHERE email = :email)`,
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handlePostStaff(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { academyId, userId, role, specialization } = body;
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO academy_staff (id, academy_id, user_id, role, specialization) VALUES (:id, :aid, :uid, :role, :spec)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "aid", value: { stringValue: academyId || "" } },
      { name: "uid", value: { stringValue: userId || "" } },
      { name: "role", value: { stringValue: role || "coach" } },
      { name: "spec", value: { stringValue: specialization || "" } },
    ]
  );
  return respond(201, { id, message: "Staff member added" });
}

async function handleInvite(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  return respond(200, { message: `Invite sent to ${body.email || "player"}` });
}

async function handleAcademyReports(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const rosterCount = await runSql(
    `SELECT COUNT(*) as count FROM academy_roster ar
     JOIN academies a ON ar.academy_id = a.id
     WHERE a.owner_id = (SELECT id FROM users WHERE email = :email)`,
    [{ name: "email", value: { stringValue: user.email } }]
  );
  const attendanceCount = await runSql(
    `SELECT COUNT(*) as count FROM attendance att
     JOIN academies a ON att.academy_id = a.id
     WHERE a.owner_id = (SELECT id FROM users WHERE email = :email)`,
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, {
    totalPlayers: parseRows(rosterCount)[0]?.count || 0,
    totalAttendanceRecords: parseRows(attendanceCount)[0]?.count || 0,
  });
}

async function handleAdminGetUsers(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(dbUser);
  if (!rows.length || rows[0].role !== "admin") return respond(403, { error: "Admin access required" });

  const result = await runSql("SELECT id, email, full_name, role, academy, created_at FROM users ORDER BY created_at DESC");
  return respond(200, parseRows(result));
}

async function handleAdminBlockUser(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(dbUser);
  if (!rows.length || rows[0].role !== "admin") return respond(403, { error: "Admin access required" });

  const targetId = event.pathParameters?.userId;
  const { blocked } = body;
  const targetUser = await runSql("SELECT email FROM users WHERE id = :id", [
    { name: "id", value: { stringValue: targetId } },
  ]);
  const targetRows = parseRows(targetUser);
  if (!targetRows.length) return respond(404, { error: "User not found" });

  const cognitoCmd = blocked ? AdminDisableUserCommand : AdminEnableUserCommand;
  await cognito.send(new cognitoCmd({
    UserPoolId: USER_POOL_ID,
    Username: targetRows[0].email,
  }));

  const auditId = crypto.randomUUID();
  await runSql(
    "INSERT INTO audit_log (id, admin_id, action, target_id, details) VALUES (:id, :admin, :action, :target, :details::jsonb)",
    [
      { name: "id", value: { stringValue: auditId } },
      { name: "admin", value: { stringValue: user.email } },
      { name: "action", value: { stringValue: blocked ? "block_user" : "unblock_user" } },
      { name: "target", value: { stringValue: targetId } },
      { name: "details", value: { stringValue: JSON.stringify({ email: targetRows[0].email }) } },
    ]
  );
  return respond(200, { message: `User ${blocked ? "blocked" : "unblocked"}` });
}

async function handleAdminChangeRole(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(dbUser);
  if (!rows.length || rows[0].role !== "admin") return respond(403, { error: "Admin access required" });

  const targetId = event.pathParameters?.userId;
  const { role } = body;
  await runSql("UPDATE users SET role = :role WHERE id = :id", [
    { name: "role", value: { stringValue: role } },
    { name: "id", value: { stringValue: targetId } },
  ]);

  const auditId = crypto.randomUUID();
  await runSql(
    "INSERT INTO audit_log (id, admin_id, action, target_id, details) VALUES (:id, :admin, :action, :target, :details::jsonb)",
    [
      { name: "id", value: { stringValue: auditId } },
      { name: "admin", value: { stringValue: user.email } },
      { name: "action", value: { stringValue: "change_role" } },
      { name: "target", value: { stringValue: targetId } },
      { name: "details", value: { stringValue: JSON.stringify({ newRole: role }) } },
    ]
  );
  return respond(200, { message: "Role updated" });
}

async function handleAuditLog(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(dbUser);
  if (!rows.length || rows[0].role !== "admin") return respond(403, { error: "Admin access required" });

  const result = await runSql("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT 100");
  return respond(200, parseRows(result));
}

async function handleAdminDashboard(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(dbUser);
  if (!rows.length || rows[0].role !== "admin") return respond(403, { error: "Admin access required" });

  const userCount = await runSql("SELECT COUNT(*) as count FROM users");
  const sessionCount = await runSql("SELECT COUNT(*) as count FROM sessions");
  const analysisCount = await runSql("SELECT COUNT(*) as count FROM analysis");
  return respond(200, {
    totalUsers: parseRows(userCount)[0]?.count || 0,
    totalSessions: parseRows(sessionCount)[0]?.count || 0,
    totalAnalyses: parseRows(analysisCount)[0]?.count || 0,
  });
}

// ─── Catalog Route Handlers ───

async function handleGetCatalog(category) {
  await seedCatalog();
  const result = await runSql("SELECT data FROM catalog WHERE category = :cat", [
    { name: "cat", value: { stringValue: category } },
  ]);
  const rows = parseRows(result);
  if (!rows.length) return respond(404, { error: "Category not found" });
  return respond(200, JSON.parse(rows[0].data));
}

async function handleGetFeeSettings(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const academy = await runSql(
    "SELECT id FROM academies WHERE owner_id = (SELECT id FROM users WHERE email = :email) LIMIT 1",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  const academyRows = parseRows(academy);
  if (!academyRows.length) return respond(404, { error: "No academy found" });
  const academyId = academyRows[0].id;
  const result = await runSql(
    "SELECT settings FROM academy_fee_settings WHERE academy_id = :aid ORDER BY updated_at DESC LIMIT 1",
    [{ name: "aid", value: { stringValue: academyId } }]
  );
  const rows = parseRows(result);
  if (!rows.length) return respond(200, {});
  return respond(200, JSON.parse(rows[0].settings));
}

async function handlePutFeeSettings(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const academy = await runSql(
    "SELECT id FROM academies WHERE owner_id = (SELECT id FROM users WHERE email = :email) LIMIT 1",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  const academyRows = parseRows(academy);
  if (!academyRows.length) return respond(404, { error: "No academy found" });
  const academyId = academyRows[0].id;
  const id = crypto.randomUUID();
  await runSql(
    `INSERT INTO academy_fee_settings (id, academy_id, settings, updated_at)
     VALUES (:id, :aid, :settings::jsonb, NOW())
     ON CONFLICT (academy_id) DO UPDATE SET settings = EXCLUDED.settings, updated_at = NOW()`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "aid", value: { stringValue: academyId } },
      { name: "settings", value: { stringValue: JSON.stringify(body) } },
    ]
  );
  return respond(200, { message: "Fee settings saved" });
}

// ─── Feed Route Handlers ───

async function handleGetFeedPosts(event) {
  const qs = event.queryStringParameters || {};
  const region = qs.region || "all";
  const postType = qs.type || "all";
  const limit = Math.min(parseInt(qs.limit || "20", 10), 100);
  const offset = parseInt(qs.offset || "0", 10);
  let sql = "SELECT fp.*, u.full_name as author_name, u.avatar_url as author_avatar, u.role as author_role FROM feed_posts fp JOIN users u ON fp.author_id = u.id";
  const conditions = [];
  const params = [];
  if (region !== "all") {
    conditions.push("fp.region = :region");
    params.push({ name: "region", value: { stringValue: region } });
  }
  if (postType !== "all") {
    conditions.push("fp.post_type = :ptype");
    params.push({ name: "ptype", value: { stringValue: postType } });
  }
  if (conditions.length > 0) sql += " WHERE " + conditions.join(" AND ");
  sql += " ORDER BY fp.created_at DESC";
  sql += ` LIMIT ${limit} OFFSET ${offset}`;
  const result = await runSql(sql, params);
  return respond(200, parseRows(result));
}

async function handleCreateFeedPost(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { content, postType, region, statsSnapshot, mediaUrl } = body;
  if (!content) return respond(400, { error: "content is required" });
  const id = crypto.randomUUID();
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  await runSql(
    `INSERT INTO feed_posts (id, author_id, content, post_type, region, stats_snapshot, media_url)
     VALUES (:id, :uid, :content, :ptype, :region, :stats::jsonb, :media)`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "uid", value: { stringValue: userId } },
      { name: "content", value: { stringValue: content } },
      { name: "ptype", value: { stringValue: postType || "general" } },
      { name: "region", value: { stringValue: region || "all" } },
      { name: "stats", value: { stringValue: JSON.stringify(statsSnapshot || {}) } },
      { name: "media", value: { stringValue: mediaUrl || "" } },
    ]
  );
  // Award energy points for posting
  await awardEnergy(userId, 5, "post_created");
  return respond(201, { id, message: "Post created" });
}

async function handleLikeFeedPost(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const postId = event.pathParameters?.postId || event.path.split("/")[3];
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const existing = await runSql(
    "SELECT id FROM feed_likes WHERE post_id = :pid AND user_id = :uid",
    [
      { name: "pid", value: { stringValue: postId } },
      { name: "uid", value: { stringValue: userId } },
    ]
  );
  if (parseRows(existing).length > 0) {
    await runSql("DELETE FROM feed_likes WHERE post_id = :pid AND user_id = :uid", [
      { name: "pid", value: { stringValue: postId } },
      { name: "uid", value: { stringValue: userId } },
    ]);
    await runSql("UPDATE feed_posts SET like_count = GREATEST(like_count - 1, 0), updated_at = NOW() WHERE id = :pid", [
      { name: "pid", value: { stringValue: postId } },
    ]);
    return respond(200, { liked: false, message: "Like removed" });
  }
  const likeId = crypto.randomUUID();
  await runSql(
    "INSERT INTO feed_likes (id, post_id, user_id) VALUES (:id, :pid, :uid)",
    [
      { name: "id", value: { stringValue: likeId } },
      { name: "pid", value: { stringValue: postId } },
      { name: "uid", value: { stringValue: userId } },
    ]
  );
  await runSql("UPDATE feed_posts SET like_count = like_count + 1, updated_at = NOW() WHERE id = :pid", [
    { name: "pid", value: { stringValue: postId } },
  ]);
  await awardEnergy(userId, 1, "post_liked");
  return respond(200, { liked: true, message: "Post liked" });
}

async function handleCommentOnPost(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const postId = event.pathParameters?.postId || event.path.split("/")[3];
  const { content } = body;
  if (!content) return respond(400, { error: "content is required" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO feed_comments (id, post_id, author_id, content) VALUES (:id, :pid, :uid, :content)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "pid", value: { stringValue: postId } },
      { name: "uid", value: { stringValue: userId } },
      { name: "content", value: { stringValue: content } },
    ]
  );
  await runSql("UPDATE feed_posts SET comment_count = comment_count + 1, updated_at = NOW() WHERE id = :pid", [
    { name: "pid", value: { stringValue: postId } },
  ]);
  await awardEnergy(userId, 2, "comment_added");
  return respond(201, { id, message: "Comment added" });
}

async function handleGetPostComments(event) {
  const postId = event.pathParameters?.postId || event.path.split("/")[3];
  const result = await runSql(
    "SELECT fc.*, u.full_name as author_name, u.avatar_url as author_avatar FROM feed_comments fc JOIN users u ON fc.author_id = u.id WHERE fc.post_id = :pid ORDER BY fc.created_at ASC",
    [{ name: "pid", value: { stringValue: postId } }]
  );
  return respond(200, parseRows(result));
}

async function handleDeleteFeedPost(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const postId = event.pathParameters?.postId || event.path.split("/")[3];
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  const post = await runSql("SELECT author_id FROM feed_posts WHERE id = :pid",[
    { name: "pid", value: { stringValue: postId } },
  ]);
  const postRow = parseRows(post)[0];
  if (!postRow) return respond(404, { error: "Post not found" });
  if (postRow.author_id !== userId) return respond(403, { error: "Not your post" });
  await runSql("DELETE FROM feed_posts WHERE id = :pid", [
    { name: "pid", value: { stringValue: postId } },
  ]);
  return respond(200, { message: "Post deleted" });
}

async function handleShareFeedPost(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const postId = event.pathParameters?.postId || event.path.split("/")[3];
  await runSql("UPDATE feed_posts SET share_count = share_count + 1, updated_at = NOW() WHERE id = :pid", [
    { name: "pid", value: { stringValue: postId } },
  ]);
  return respond(200, { message: "Post shared" });
}

// ─── Energy / Leaderboard Route Handlers ───

async function awardEnergy(userId, points, reason) {
  const existing = await runSql("SELECT id, total_points, weekly_points FROM energy_scores WHERE user_id = :uid", [
    { name: "uid", value: { stringValue: userId } },
  ]);
  const rows = parseRows(existing);
  if (rows.length > 0) {
    await runSql(
      "UPDATE energy_scores SET total_points = total_points + :pts, weekly_points = weekly_points + :pts, last_activity = NOW(), updated_at = NOW() WHERE user_id = :uid",
      [
        { name: "pts", value: { longValue: points } },
        { name: "uid", value: { stringValue: userId } },
      ]
    );
  } else {
    const id = crypto.randomUUID();
    await runSql(
      "INSERT INTO energy_scores (id, user_id, total_points, weekly_points, last_activity) VALUES (:id, :uid, :pts, :pts, NOW())",
      [
        { name: "id", value: { stringValue: id } },
        { name: "uid", value: { stringValue: userId } },
        { name: "pts", value: { longValue: points } },
      ]
    );
  }
}

async function handleGetLeaderboard(event) {
  const qs = event.queryStringParameters || {};
  const period = qs.period || "all";
  const limit = Math.min(parseInt(qs.limit || "20", 10), 100);
  const orderCol = period === "weekly" ? "es.weekly_points" : "es.total_points";
  const result = await runSql(
    `SELECT es.*, u.full_name, u.avatar_url, u.role,
     (SELECT COUNT(*) FROM badges b WHERE b.user_id = es.user_id) as badge_count
     FROM energy_scores es JOIN users u ON es.user_id = u.id
     ORDER BY ${orderCol} DESC LIMIT ${limit}`
  );
  return respond(200, parseRows(result));
}

async function handleGetMyEnergy(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const result = await runSql("SELECT * FROM energy_scores WHERE user_id = :uid", [
    { name: "uid", value: { stringValue: userId } },
  ]);
  const rows = parseRows(result);
  if (!rows.length) return respond(200, { total_points: 0, weekly_points: 0, level: "rookie", streak_days: 0 });
  return respond(200, rows[0]);
}

async function handleGetMyBadges(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const result = await runSql("SELECT * FROM badges WHERE user_id = :uid ORDER BY awarded_at DESC", [
    { name: "uid", value: { stringValue: userId } },
  ]);
  return respond(200, parseRows(result));
}

async function handleAwardEnergy(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT id, role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const dbRow = parseRows(dbUser)[0];
  if (!dbRow || !['coach', 'academy_admin', 'admin', 'owner'].includes(dbRow.role)) {
    return respond(403, { error: "Only coaches/admins can award energy" });
  }
  const { targetUserId, points, reason, badgeName } = body;
  if (!targetUserId || !points) return respond(400, { error: "targetUserId and points are required" });
  await awardEnergy(targetUserId, points, reason || "coach_award");
  if (badgeName) {
    const badgeId = crypto.randomUUID();
    await runSql(
      "INSERT INTO badges (id, user_id, badge_type, badge_name, awarded_by) VALUES (:id, :uid, :btype, :bname, :by)",
      [
        { name: "id", value: { stringValue: badgeId } },
        { name: "uid", value: { stringValue: targetUserId } },
        { name: "btype", value: { stringValue: reason || "coach_award" } },
        { name: "bname", value: { stringValue: badgeName } },
        { name: "by", value: { stringValue: dbRow.id } },
      ]
    );
  }
  return respond(200, { message: `Awarded ${points} CE to user`, badgeAwarded: !!badgeName });
}

// ─── Compare Route Handlers ───

async function handleComparePlayers(event) {
  const qs = event.queryStringParameters || {};
  const ids = (qs.ids || "").split(",").filter(Boolean);
  if (ids.length < 2) return respond(400, { error: "Provide at least 2 player IDs (comma-separated ids param)" });
  const players = [];
  for (const pid of ids.slice(0, 4)) {
    const userResult = await runSql(
      "SELECT id, full_name, email, role, avatar_url FROM users WHERE id = :id",
      [{ name: "id", value: { stringValue: pid } }]
    );
    const userRows = parseRows(userResult);
    if (!userRows.length) continue;
    const statsResult = await runSql(
      "SELECT stat_type, stat_data, source, recorded_at FROM player_stats WHERE user_id = :uid ORDER BY recorded_at DESC LIMIT 10",
      [{ name: "uid", value: { stringValue: pid } }]
    );
    const energyResult = await runSql(
      "SELECT total_points, weekly_points, level, streak_days FROM energy_scores WHERE user_id = :uid",
      [{ name: "uid", value: { stringValue: pid } }]
    );
    const badgeResult = await runSql(
      "SELECT badge_type, badge_name, awarded_at FROM badges WHERE user_id = :uid",
      [{ name: "uid", value: { stringValue: pid } }]
    );
    players.push({
      ...userRows[0],
      stats: parseRows(statsResult),
      energy: parseRows(energyResult)[0] || { total_points: 0, weekly_points: 0 },
      badges: parseRows(badgeResult),
    });
  }
  return respond(200, { players });
}

// ─── Selector / Watchlist Route Handlers ───

async function handleGetWatchlist(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const qs = event.queryStringParameters || {};
  const listType = qs.type || "watch";
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const result = await runSql(
    "SELECT * FROM watchlists WHERE user_id = :uid AND list_type = :lt ORDER BY ranking ASC, created_at DESC",
    [
      { name: "uid", value: { stringValue: userId } },
      { name: "lt", value: { stringValue: listType } },
    ]
  );
  return respond(200, parseRows(result));
}

async function handleAddToWatchlist(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { playerId, playerName, listType, notes, ranking } = body;
  if (!playerId) return respond(400, { error: "playerId is required" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const id = crypto.randomUUID();
  await runSql(
    `INSERT INTO watchlists (id, user_id, player_id, player_name, list_type, notes, ranking)
     VALUES (:id, :uid, :pid, :pname, :lt, :notes, :rank)`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "uid", value: { stringValue: userId } },
      { name: "pid", value: { stringValue: playerId } },
      { name: "pname", value: { stringValue: playerName || "" } },
      { name: "lt", value: { stringValue: listType || "watch" } },
      { name: "notes", value: { stringValue: notes || "" } },
      { name: "rank", value: { longValue: ranking || 0 } },
    ]
  );
  return respond(201, { id, message: "Added to watchlist" });
}

async function handleRemoveFromWatchlist(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const watchId = event.pathParameters?.watchId || event.path.split("/").pop();
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  const result = await runSql("DELETE FROM watchlists WHERE id = :wid AND user_id = :uid", [
    { name: "wid", value: { stringValue: watchId } },
    { name: "uid", value: { stringValue: userId } },
  ]);
  return respond(200, { message: "Removed from watchlist" });
}

async function handleUpdateWatchlistRanking(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { rankings } = body;
  if (!Array.isArray(rankings)) return respond(400, { error: "rankings array is required" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  for (const item of rankings) {
    await runSql(
      "UPDATE watchlists SET ranking = :rank, updated_at = NOW() WHERE id = :wid AND user_id = :uid",
      [
        { name: "rank", value: { longValue: item.ranking || 0 } },
        { name: "wid", value: { stringValue: item.id } },
        { name: "uid", value: { stringValue: userId } },
      ]
    );
  }
  return respond(200, { message: "Rankings updated" });
}

// ─── Strategy Route Handlers ───

async function handleGetStrategies(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const result = await runSql(
    "SELECT * FROM match_strategies WHERE user_id = :uid ORDER BY updated_at DESC",
    [{ name: "uid", value: { stringValue: userId } }]
  );
  return respond(200, parseRows(result));
}

async function handleCreateStrategy(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { matchName, opponent, phase, bowlingPlan, battingPlan, fieldPositions, notes } = body;
  if (!matchName) return respond(400, { error: "matchName is required" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const id = crypto.randomUUID();
  await runSql(
    `INSERT INTO match_strategies (id, user_id, match_name, opponent, phase, bowling_plan, batting_plan, field_positions, notes)
     VALUES (:id, :uid, :mname, :opp, :phase, :bowl::jsonb, :bat::jsonb, :field::jsonb, :notes)`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "uid", value: { stringValue: userId } },
      { name: "mname", value: { stringValue: matchName } },
      { name: "opp", value: { stringValue: opponent || "" } },
      { name: "phase", value: { stringValue: phase || "powerplay" } },
      { name: "bowl", value: { stringValue: JSON.stringify(bowlingPlan || {}) } },
      { name: "bat", value: { stringValue: JSON.stringify(battingPlan || {}) } },
      { name: "field", value: { stringValue: JSON.stringify(fieldPositions || {}) } },
      { name: "notes", value: { stringValue: notes || "" } },
    ]
  );
  return respond(201, { id, message: "Strategy created" });
}

async function handleUpdateStrategy(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const stratId = event.pathParameters?.strategyId || event.path.split("/").pop();
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  const { matchName, opponent, phase, bowlingPlan, battingPlan, fieldPositions, notes } = body;
  await runSql(
    `UPDATE match_strategies SET
      match_name = COALESCE(:mname, match_name),
      opponent = COALESCE(:opp, opponent),
      phase = COALESCE(:phase, phase),
      bowling_plan = COALESCE(:bowl::jsonb, bowling_plan),
      batting_plan = COALESCE(:bat::jsonb, batting_plan),
      field_positions = COALESCE(:field::jsonb, field_positions),
      notes = COALESCE(:notes, notes),
      updated_at = NOW()
    WHERE id = :sid AND user_id = :uid`,
    [
      { name: "mname", value: matchName ? { stringValue: matchName } : { isNull: true } },
      { name: "opp", value: opponent ? { stringValue: opponent } : { isNull: true } },
      { name: "phase", value: phase ? { stringValue: phase } : { isNull: true } },
      { name: "bowl", value: bowlingPlan ? { stringValue: JSON.stringify(bowlingPlan) } : { isNull: true } },
      { name: "bat", value: battingPlan ? { stringValue: JSON.stringify(battingPlan) } : { isNull: true } },
      { name: "field", value: fieldPositions ? { stringValue: JSON.stringify(fieldPositions) } : { isNull: true } },
      { name: "notes", value: notes ? { stringValue: notes } : { isNull: true } },
      { name: "sid", value: { stringValue: stratId } },
      { name: "uid", value: { stringValue: userId } },
    ]
  );
  return respond(200, { message: "Strategy updated" });
}

async function handleDeleteStrategy(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const stratId = event.pathParameters?.strategyId || event.path.split("/").pop();
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  await runSql("DELETE FROM match_strategies WHERE id = :sid AND user_id = :uid", [
    { name: "sid", value: { stringValue: stratId } },
    { name: "uid", value: { stringValue: userId } },
  ]);
  return respond(200, { message: "Strategy deleted" });
}

async function handleSendReminder(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { studentId, studentName, feeType, amount } = body;
  if (!studentId || !feeType) return respond(400, { error: "studentId and feeType are required" });
  const auditId = crypto.randomUUID();
  await runSql(
    "INSERT INTO audit_log (id, admin_id, action, target_id, details) VALUES (:id, :admin, 'send_reminder', :target, :details::jsonb)",
    [
      { name: "id", value: { stringValue: auditId } },
      { name: "admin", value: { stringValue: user.email } },
      { name: "target", value: { stringValue: studentId } },
      { name: "details", value: { stringValue: JSON.stringify({ studentName, feeType, amount }) } },
    ]
  );
  return respond(200, { message: `Reminder sent to ${studentName || studentId}` });
}

// ─── Drill Handlers ───
async function handleGetDrills(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const qs = event.queryStringParameters || {};
  const category = qs.category || "";
  const skillLevel = qs.skill_level || "";
  const authorOnly = qs.mine === "true";
  const academyId = qs.academy_id || "";
  let sql = `SELECT d.*, u.full_name as author_name, u.avatar_url as author_avatar
    FROM drills d JOIN users u ON d.author_id = u.id
    WHERE d.visibility != 'deleted'`;
  const params = [];
  if (category) {
    sql += " AND d.category = :cat";
    params.push({ name: "cat", value: { stringValue: category } });
  }
  if (skillLevel) {
    sql += " AND d.skill_level = :lvl";
    params.push({ name: "lvl", value: { stringValue: skillLevel } });
  }
  if (authorOnly) {
    sql += " AND u.email = :email";
    params.push({ name: "email", value: { stringValue: user.email } });
  }
  if (academyId) {
    sql += " AND d.academy_id = :aid";
    params.push({ name: "aid", value: { stringValue: academyId } });
  }
  sql += " ORDER BY d.created_at DESC LIMIT 100";
  const result = await runSql(sql, params);
  return respond(200, parseRows(result));
}

async function handleCreateDrill(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { title, description, videoUrl, videoKey, thumbnailUrl, category, skillLevel, durationMinutes, tags, visibility, academyId } = body;
  if (!title) return respond(400, { error: "title is required" });
  const id = crypto.randomUUID();
  const userResult = await runSql("SELECT id FROM users WHERE email = :email", [{ name: "email", value: { stringValue: user.email } }]);
  const userRows = parseRows(userResult);
  if (userRows.length === 0) return respond(404, { error: "User not found" });
  const authorId = userRows[0].id;
  await runSql(
    `INSERT INTO drills (id, author_id, title, description, video_url, video_key, thumbnail_url, category, skill_level, duration_minutes, tags, visibility, academy_id)
     VALUES (:id, :author, :title, :desc, :vurl, :vkey, :thumb, :cat, :lvl, :dur, :tags::jsonb, :vis, :aid)`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "author", value: { stringValue: authorId } },
      { name: "title", value: { stringValue: title } },
      { name: "desc", value: { stringValue: description || "" } },
      { name: "vurl", value: { stringValue: videoUrl || "" } },
      { name: "vkey", value: { stringValue: videoKey || "" } },
      { name: "thumb", value: { stringValue: thumbnailUrl || "" } },
      { name: "cat", value: { stringValue: category || "batting" } },
      { name: "lvl", value: { stringValue: skillLevel || "beginner" } },
      { name: "dur", value: { longValue: durationMinutes || 0 } },
      { name: "tags", value: { stringValue: JSON.stringify(tags || []) } },
      { name: "vis", value: { stringValue: visibility || "public" } },
      { name: "aid", value: { stringValue: academyId || "" } },
    ]
  );
  return respond(201, { id, message: "Drill created" });
}

async function handleGetDrillById(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const drillId = event.path.split("/").pop();
  const result = await runSql(
    `SELECT d.*, u.full_name as author_name, u.avatar_url as author_avatar
     FROM drills d JOIN users u ON d.author_id = u.id WHERE d.id = :id`,
    [{ name: "id", value: { stringValue: drillId } }]
  );
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "Drill not found" });
  return respond(200, rows[0]);
}

async function handleUpdateDrill(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const drillId = event.path.split("/").filter(Boolean);
  const id = drillId[drillId.length - 1];
  const { title, description, videoUrl, category, skillLevel, durationMinutes, tags, visibility } = body;
  await runSql(
    `UPDATE drills SET title = COALESCE(:title, title), description = COALESCE(:desc, description),
     video_url = COALESCE(:vurl, video_url), category = COALESCE(:cat, category),
     skill_level = COALESCE(:lvl, skill_level), duration_minutes = COALESCE(:dur, duration_minutes),
     tags = COALESCE(:tags::jsonb, tags), visibility = COALESCE(:vis, visibility), updated_at = NOW()
     WHERE id = :id AND author_id = (SELECT id FROM users WHERE email = :email)`,
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
      { name: "title", value: title ? { stringValue: title } : { isNull: true } },
      { name: "desc", value: description ? { stringValue: description } : { isNull: true } },
      { name: "vurl", value: videoUrl ? { stringValue: videoUrl } : { isNull: true } },
      { name: "cat", value: category ? { stringValue: category } : { isNull: true } },
      { name: "lvl", value: skillLevel ? { stringValue: skillLevel } : { isNull: true } },
      { name: "dur", value: durationMinutes ? { longValue: durationMinutes } : { isNull: true } },
      { name: "tags", value: tags ? { stringValue: JSON.stringify(tags) } : { isNull: true } },
      { name: "vis", value: visibility ? { stringValue: visibility } : { isNull: true } },
    ]
  );
  return respond(200, { message: "Drill updated" });
}

async function handleDeleteDrill(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const parts = event.path.split("/").filter(Boolean);
  const id = parts[parts.length - 1];
  await runSql(
    "DELETE FROM drills WHERE id = :id AND author_id = (SELECT id FROM users WHERE email = :email)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "email", value: { stringValue: user.email } },
    ]
  );
  return respond(200, { message: "Drill deleted" });
}

async function handleLikeDrill(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const parts = event.path.split("/").filter(Boolean);
  const drillId = parts[parts.length - 2];
  const userResult = await runSql("SELECT id FROM users WHERE email = :email", [{ name: "email", value: { stringValue: user.email } }]);
  const userRows = parseRows(userResult);
  if (userRows.length === 0) return respond(404, { error: "User not found" });
  const userId = userRows[0].id;
  const existing = await runSql(
    "SELECT id FROM drill_likes WHERE drill_id = :did AND user_id = :uid",
    [
      { name: "did", value: { stringValue: drillId } },
      { name: "uid", value: { stringValue: userId } },
    ]
  );
  const existingRows = parseRows(existing);
  if (existingRows.length > 0) {
    await runSql("DELETE FROM drill_likes WHERE drill_id = :did AND user_id = :uid", [
      { name: "did", value: { stringValue: drillId } },
      { name: "uid", value: { stringValue: userId } },
    ]);
    await runSql("UPDATE drills SET like_count = GREATEST(like_count - 1, 0) WHERE id = :id", [{ name: "id", value: { stringValue: drillId } }]);
    return respond(200, { message: "Unliked", liked: false });
  }
  const likeId = crypto.randomUUID();
  await runSql(
    "INSERT INTO drill_likes (id, drill_id, user_id) VALUES (:id, :did, :uid)",
    [
      { name: "id", value: { stringValue: likeId } },
      { name: "did", value: { stringValue: drillId } },
      { name: "uid", value: { stringValue: userId } },
    ]
  );
  await runSql("UPDATE drills SET like_count = like_count + 1 WHERE id = :id", [{ name: "id", value: { stringValue: drillId } }]);
  return respond(200, { message: "Liked", liked: true });
}

async function handleGetDrillComments(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const parts = event.path.split("/").filter(Boolean);
  const drillId = parts[parts.length - 2];
  const result = await runSql(
    `SELECT dc.*, u.full_name as author_name, u.avatar_url as author_avatar
     FROM drill_comments dc JOIN users u ON dc.author_id = u.id
     WHERE dc.drill_id = :did ORDER BY dc.created_at ASC`,
    [{ name: "did", value: { stringValue: drillId } }]
  );
  return respond(200, parseRows(result));
}

async function handleCommentOnDrill(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const parts = event.path.split("/").filter(Boolean);
  const drillId = parts[parts.length - 2];
  const { content } = body;
  if (!content) return respond(400, { error: "content is required" });
  const userResult = await runSql("SELECT id FROM users WHERE email = :email", [{ name: "email", value: { stringValue: user.email } }]);
  const userRows = parseRows(userResult);
  if (userRows.length === 0) return respond(404, { error: "User not found" });
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO drill_comments (id, drill_id, author_id, content) VALUES (:id, :did, :uid, :content)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "did", value: { stringValue: drillId } },
      { name: "uid", value: { stringValue: userRows[0].id } },
      { name: "content", value: { stringValue: content } },
    ]
  );
  await runSql("UPDATE drills SET comment_count = comment_count + 1 WHERE id = :id", [{ name: "id", value: { stringValue: drillId } }]);
  return respond(201, { id, message: "Comment added" });
}

async function handleShareDrill(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const parts = event.path.split("/").filter(Boolean);
  const drillId = parts[parts.length - 2];
  const drillResult = await runSql("SELECT * FROM drills WHERE id = :id", [{ name: "id", value: { stringValue: drillId } }]);
  const drillRows = parseRows(drillResult);
  if (drillRows.length === 0) return respond(404, { error: "Drill not found" });
  const drill = drillRows[0];
  const userResult = await runSql("SELECT id FROM users WHERE email = :email", [{ name: "email", value: { stringValue: user.email } }]);
  const userRows = parseRows(userResult);
  if (userRows.length === 0) return respond(404, { error: "User not found" });
  const postId = crypto.randomUUID();
  const postContent = `Shared a drill: ${drill.title}\n${drill.description || ""}`;
  await runSql(
    `INSERT INTO feed_posts (id, author_id, content, post_type, media_url)
     VALUES (:id, :uid, :content, 'drill_share', :media)`,
    [
      { name: "id", value: { stringValue: postId } },
      { name: "uid", value: { stringValue: userRows[0].id } },
      { name: "content", value: { stringValue: postContent } },
      { name: "media", value: { stringValue: drill.video_url || "" } },
    ]
  );
  await runSql("UPDATE drills SET share_count = share_count + 1 WHERE id = :id", [{ name: "id", value: { stringValue: drillId } }]);
  return respond(200, { message: "Drill shared to feed", postId });
}

async function handleDrillVideoUpload(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const ext = body.extension || "mp4";
  const contentType = body.contentType || "video/mp4";
  const key = `drills/${user.sub || user.username}/${Date.now()}.${ext}`;
  const url = await getSignedUrl(s3, new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    ContentType: contentType,
  }), { expiresIn: 600 });
  return respond(200, { uploadUrl: url, key });
}

// ─── DB Control Handlers ───
async function handleDbStop() {
  try {
    await rdsControl.send(new StopDBClusterCommand({ DBClusterIdentifier: DB_CLUSTER_ID }));
    return respond(200, { message: "Database cluster is stopping. It may take a few minutes." });
  } catch (err) {
    if (err.name === "InvalidDBClusterStateFault") {
      return respond(409, { error: "Cluster is already stopped or in a transitional state." });
    }
    return respond(500, { error: err.message });
  }
}

async function handleDbStart() {
  try {
    await rdsControl.send(new StartDBClusterCommand({ DBClusterIdentifier: DB_CLUSTER_ID }));
    return respond(200, { message: "Database cluster is starting. It may take a few minutes." });
  } catch (err) {
    if (err.name === "InvalidDBClusterStateFault") {
      return respond(409, { error: "Cluster is already running or in a transitional state." });
    }
    return respond(500, { error: err.message });
  }
}

async function handleDbStatus() {
  try {
    const result = await rdsControl.send(new DescribeDBClustersCommand({ DBClusterIdentifier: DB_CLUSTER_ID }));
    const cluster = result.DBClusters[0];
    return respond(200, {
      status: cluster.Status,
      clusterIdentifier: cluster.DBClusterIdentifier,
      engine: cluster.Engine,
      engineVersion: cluster.EngineVersion,
    });
  } catch (err) {
    return respond(500, { error: err.message });
  }
}

// ─── Main Router ───
export async function handler(event) {
  const method = event.httpMethod;
  const path = event.path?.replace(/\/$/, '') || '/';
  let body = {};
  try {
    body = event.body ? JSON.parse(event.body) : {};
  } catch {
    body = {};
  }

  // Initialize DB on first call (cold start)
  if (!dbInitialized) {
    try {
      await initDb();
      dbInitialized = true;
    } catch (err) {
      console.error("DB init error:", err);
    }
  }

  // Handle OPTIONS preflight
  if (method === "OPTIONS") return respond(200, {});

  try {
    // Auth routes
    if (path === "/auth/register" && method === "POST") return await handleAuthRegister(body);
    if (path === "/auth/login" && method === "POST") return await handleAuthLogin(body);
    if (path === "/auth/verify" && method === "POST") return await handleAuthVerify(body);
    if (path === "/auth/forgot-password" && method === "POST") return await handleForgotPassword(body);
    if (path === "/auth/reset-password" && method === "POST") return await handleResetPassword(body);
    if (path === "/auth/me" && method === "GET") return await handleAuthMe(event);

    // User routes
    if (path === "/users/profile" && method === "GET") return await handleGetProfile(event);
    if (path === "/users/profile" && method === "PUT") return await handleUpdateProfile(event, body);
    if (path === "/users/avatar" && method === "POST") return await handleAvatarUpload(event);
    if (path === "/users/video-upload" && method === "POST") return await handleVideoUpload(event, body);

    // Stats routes
    if (path === "/stats" && method === "GET") return await handleGetStats(event);
    if (path === "/stats" && method === "POST") return await handlePostStats(event, body);
    if (path === "/stats/cricclubs-sync" && method === "POST") return await handleCricclubsSync(event, body);
    if (path === "/stats/history" && method === "GET") return await handleStatsHistory(event);

    // Sessions routes
    if (path === "/sessions" && method === "GET") return await handleGetSessions(event);
    if (path === "/sessions" && method === "POST") return await handlePostSession(event, body);
    if (path === "/sessions/{sessionId}" && method === "GET") return await handleGetSessionById(event);

    // Analysis routes
    if (path === "/analysis" && method === "POST") return await handlePostAnalysis(event, body);
    if (path === "/analysis/history" && method === "GET") return await handleAnalysisHistory(event);

    // Idol routes
    if (path === "/idol/selections" && method === "GET") return await handleGetIdolSelections(event);
    if (path === "/idol/selections" && method === "POST") return await handlePostIdolSelections(event, body);
    if (path === "/idol/progress" && method === "GET") return await handleGetIdolProgress(event);
    if (path === "/idol/progress" && method === "POST") return await handlePostIdolProgress(event, body);

    // Academy routes
    if (path === "/academy" && method === "GET") return await handleGetAcademy(event);
    if (path === "/academy" && method === "POST") return await handlePostAcademy(event, body);
    if (path === "/academy/roster" && method === "GET") return await handleGetRoster(event);
    if (path === "/academy/roster" && method === "POST") return await handlePostRoster(event, body);
    if (path === "/academy/attendance" && method === "GET") return await handleGetAttendance(event);
    if (path === "/academy/attendance" && method === "POST") return await handlePostAttendance(event, body);
    if (path === "/academy/staff" && method === "GET") return await handleGetStaff(event);
    if (path === "/academy/staff" && method === "POST") return await handlePostStaff(event, body);
    if (path === "/academy/invite" && method === "POST") return await handleInvite(event, body);
    if (path === "/academy/reports" && method === "GET") return await handleAcademyReports(event);

    // Admin DB control routes (GET supported for easy browser access)
    if (path === "/admin/db/stop" && (method === "POST" || method === "GET")) return await handleDbStop();
    if (path === "/admin/db/start" && (method === "POST" || method === "GET")) return await handleDbStart();
    if (path === "/admin/db/status" && method === "GET") return await handleDbStatus();

    // Admin routes
    if (path === "/admin/users" && method === "GET") return await handleAdminGetUsers(event);
    if (path === "/admin/users/{userId}/block" && method === "PUT") return await handleAdminBlockUser(event, body);
    if (path === "/admin/users/{userId}/role" && method === "PUT") return await handleAdminChangeRole(event, body);
    if (path === "/admin/audit-log" && method === "GET") return await handleAuditLog(event);
    if (path === "/admin/dashboard" && method === "GET") return await handleAdminDashboard(event);

    // Feed routes
    if (path === "/feed/posts" && method === "GET") return await handleGetFeedPosts(event);
    if (path === "/feed/posts" && method === "POST") return await handleCreateFeedPost(event, body);
    if (path.match(/^\/feed\/posts\/[^/]+\/like$/) && method === "POST") return await handleLikeFeedPost(event);
    if (path.match(/^\/feed\/posts\/[^/]+\/comments$/) && method === "POST") return await handleCommentOnPost(event, body);
    if (path.match(/^\/feed\/posts\/[^/]+\/comments$/) && method === "GET") return await handleGetPostComments(event);
    if (path.match(/^\/feed\/posts\/[^/]+\/share$/) && method === "POST") return await handleShareFeedPost(event);
    if (path.match(/^\/feed\/posts\/[^/]+$/) && method === "DELETE") return await handleDeleteFeedPost(event);

    // Energy / Leaderboard routes
    if (path === "/energy/leaderboard" && method === "GET") return await handleGetLeaderboard(event);
    if (path === "/energy/me" && method === "GET") return await handleGetMyEnergy(event);
    if (path === "/energy/my-badges" && method === "GET") return await handleGetMyBadges(event);
    if (path === "/energy/award" && method === "POST") return await handleAwardEnergy(event, body);

    // Compare routes
    if (path === "/compare/players" && method === "GET") return await handleComparePlayers(event);

    // Selector / Watchlist routes
    if (path === "/selector/watchlist" && method === "GET") return await handleGetWatchlist(event);
    if (path === "/selector/watchlist" && method === "POST") return await handleAddToWatchlist(event, body);
    if (path.match(/^\/selector\/watchlist\/[^/]+$/) && method === "DELETE") return await handleRemoveFromWatchlist(event);
    if (path === "/selector/watchlist/rankings" && method === "PUT") return await handleUpdateWatchlistRanking(event, body);

    // Strategy routes
    if (path === "/strategy/plans" && method === "GET") return await handleGetStrategies(event);
    if (path === "/strategy/plans" && method === "POST") return await handleCreateStrategy(event, body);
    if (path.match(/^\/strategy\/plans\/[^/]+$/) && method === "PUT") return await handleUpdateStrategy(event, body);
    if (path.match(/^\/strategy\/plans\/[^/]+$/) && method === "DELETE") return await handleDeleteStrategy(event);

    // Catalog routes
    if (path.startsWith("/catalog/") && method === "GET") {
      const category = path.replace("/catalog/", "").replace(/-/g, "_");
      return await handleGetCatalog(category);
    }

    // Fee settings routes
    if (path === "/academy/fee-settings" && method === "GET") return await handleGetFeeSettings(event);
    if (path === "/academy/fee-settings" && method === "PUT") return await handlePutFeeSettings(event, body);

    // Payments routes
    if (path === "/payments/send-reminder" && method === "POST") return await handleSendReminder(event, body);

    // Drill routes
    if (path === "/drills" && method === "GET") return await handleGetDrills(event);
    if (path === "/drills" && method === "POST") return await handleCreateDrill(event, body);
    if (path === "/drills/upload-url" && method === "POST") return await handleDrillVideoUpload(event, body);
    if (path.match(/^\/drills\/[^/]+\/like$/) && method === "POST") return await handleLikeDrill(event);
    if (path.match(/^\/drills\/[^/]+\/comments$/) && method === "GET") return await handleGetDrillComments(event);
    if (path.match(/^\/drills\/[^/]+\/comments$/) && method === "POST") return await handleCommentOnDrill(event, body);
    if (path.match(/^\/drills\/[^/]+\/share$/) && method === "POST") return await handleShareDrill(event);
    if (path.match(/^\/drills\/[^/]+$/) && method === "GET") return await handleGetDrillById(event);
    if (path.match(/^\/drills\/[^/]+$/) && method === "PUT") return await handleUpdateDrill(event, body);
    if (path.match(/^\/drills\/[^/]+$/) && method === "DELETE") return await handleDeleteDrill(event);

    // Health check
    if ((path === "/health" || path === "/" || path === "") && method === "GET") {
      return respond(200, { status: "ok", service: "CricVerse360 API", version: "1.0.0" });
    }

    return respond(404, { error: "Route not found", path, method });
  } catch (err) {
    console.error("Handler error:", err);
    return respond(500, { error: "Internal server error", details: err.message });
  }
}
