import { RDSDataClient, ExecuteStatementCommand } from "@aws-sdk/client-rds-data";
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
const cognito = new CognitoIdentityProviderClient({ region });
const s3 = new S3Client({ region });

const DB_CLUSTER_ARN = process.env.DB_CLUSTER_ARN;
const DB_SECRET_ARN = process.env.DB_SECRET_ARN;
const DB_NAME = process.env.DB_NAME;
const USER_POOL_ID = process.env.USER_POOL_ID;
const USER_POOL_CLIENT_ID = process.env.USER_POOL_CLIENT_ID;
const BUCKET_NAME = process.env.BUCKET_NAME;

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
    `CREATE TABLE IF NOT EXISTS player_profiles (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      username TEXT UNIQUE NOT NULL,
      age INTEGER,
      location TEXT DEFAULT '',
      role TEXT DEFAULT 'batsman',
      batting_style TEXT DEFAULT '',
      bowling_style TEXT DEFAULT '',
      academy TEXT DEFAULT '',
      bio TEXT DEFAULT '',
      public_profile_enabled BOOLEAN DEFAULT TRUE,
      best_score INTEGER DEFAULT 0,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS videos (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      player_profile_id TEXT DEFAULT '',
      video_url TEXT DEFAULT '',
      video_key TEXT NOT NULL,
      video_type TEXT DEFAULT 'batting',
      status TEXT DEFAULT 'uploaded',
      thumbnail_url TEXT DEFAULT '',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS subscriptions (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      stripe_customer_id TEXT DEFAULT '',
      stripe_subscription_id TEXT DEFAULT '',
      plan TEXT DEFAULT 'free',
      status TEXT DEFAULT 'active',
      current_period_end TIMESTAMP,
      analysis_credits INTEGER DEFAULT 1,
      created_at TIMESTAMP DEFAULT NOW(),
      updated_at TIMESTAMP DEFAULT NOW(),
      UNIQUE(user_id)
    )`,
    `CREATE TABLE IF NOT EXISTS coaches (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      location TEXT DEFAULT '',
      specialization TEXT DEFAULT '',
      experience TEXT DEFAULT '',
      price NUMERIC DEFAULT 0,
      profile_image TEXT DEFAULT '',
      bio TEXT DEFAULT '',
      active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS coach_requests (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id),
      coach_id TEXT NOT NULL REFERENCES coaches(id),
      message TEXT DEFAULT '',
      status TEXT DEFAULT 'pending',
      created_at TIMESTAMP DEFAULT NOW()
    )`,
    `CREATE TABLE IF NOT EXISTS analytics_events (
      id TEXT PRIMARY KEY,
      user_id TEXT DEFAULT '',
      event_name TEXT NOT NULL,
      event_data JSONB DEFAULT '{}',
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

// ─── Stripe Integration ───
async function handleCreateCheckout(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });

  const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
  if (!STRIPE_SECRET_KEY) return respond(500, { error: "Stripe not configured" });

  const { plan, successUrl, cancelUrl } = body;
  if (!plan) return respond(400, { error: "plan is required" });

  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });

  // Get or create Stripe customer
  let sub = await runSql("SELECT stripe_customer_id FROM subscriptions WHERE user_id = :uid", [
    { name: "uid", value: { stringValue: userId } },
  ]);
  let customerId = parseRows(sub)[0]?.stripe_customer_id;

  if (!customerId) {
    const custRes = await fetch("https://api.stripe.com/v1/customers", {
      method: "POST",
      headers: { "Authorization": `Bearer ${STRIPE_SECRET_KEY}`, "Content-Type": "application/x-www-form-urlencoded" },
      body: `email=${encodeURIComponent(user.email)}&metadata[user_id]=${encodeURIComponent(userId)}`,
    });
    const cust = await custRes.json();
    customerId = cust.id;
    await runSql("UPDATE subscriptions SET stripe_customer_id = :cid WHERE user_id = :uid", [
      { name: "cid", value: { stringValue: customerId } },
      { name: "uid", value: { stringValue: userId } },
    ]);
  }

  // Price lookup
  const prices = {
    pro: process.env.STRIPE_PRO_PRICE_ID || "",
    pro_plus: process.env.STRIPE_PRO_PLUS_PRICE_ID || "",
    one_time: process.env.STRIPE_ONE_TIME_PRICE_ID || "",
  };

  const priceId = prices[plan];
  if (!priceId) return respond(400, { error: "Invalid plan" });

  const isOneTime = plan === "one_time";
  const params = new URLSearchParams({
    "customer": customerId,
    "success_url": successUrl || "https://cricverse360.com/pricing?success=true",
    "cancel_url": cancelUrl || "https://cricverse360.com/pricing?cancelled=true",
    "mode": isOneTime ? "payment" : "subscription",
    "line_items[0][price]": priceId,
    "line_items[0][quantity]": "1",
    "metadata[user_id]": userId,
    "metadata[plan]": plan,
  });

  const sessionRes = await fetch("https://api.stripe.com/v1/checkout/sessions", {
    method: "POST",
    headers: { "Authorization": `Bearer ${STRIPE_SECRET_KEY}`, "Content-Type": "application/x-www-form-urlencoded" },
    body: params.toString(),
  });
  const session = await sessionRes.json();
  return respond(200, { url: session.url, sessionId: session.id });
}

async function handleStripeWebhook(event) {
  const STRIPE_SECRET_KEY = process.env.STRIPE_SECRET_KEY;
  const STRIPE_WEBHOOK_SECRET = process.env.STRIPE_WEBHOOK_SECRET;
  if (!STRIPE_SECRET_KEY) return respond(500, { error: "Stripe not configured" });

  const rawBody = event.body || "";
  const sig = event.headers?.["stripe-signature"] || event.headers?.["Stripe-Signature"] || "";

  // Verify webhook signature if secret is configured
  let evt;
  if (STRIPE_WEBHOOK_SECRET && sig) {
    try {
      const parts = sig.split(",").reduce((acc, p) => {
        const [k, v] = p.split("=");
        acc[k.trim()] = v;
        return acc;
      }, {});
      const timestamp = parts.t;
      const signedPayload = `${timestamp}.${rawBody}`;
      const expected = parts.v1;
      const hmac = crypto.createHmac("sha256", STRIPE_WEBHOOK_SECRET).update(signedPayload).digest("hex");
      if (hmac !== expected) return respond(400, { error: "Invalid signature" });
      evt = JSON.parse(rawBody);
    } catch (e) {
      return respond(400, { error: "Webhook verification failed" });
    }
  } else {
    try { evt = JSON.parse(rawBody); } catch { return respond(400, { error: "Invalid JSON" }); }
  }

  const type = evt.type;
  const data = evt.data?.object;

  if (type === "checkout.session.completed") {
    const userId = data.metadata?.user_id;
    const plan = data.metadata?.plan;
    if (!userId) return respond(200, { received: true });

    if (plan === "one_time") {
      await runSql("UPDATE subscriptions SET analysis_credits = analysis_credits + 1, updated_at = NOW() WHERE user_id = :uid", [
        { name: "uid", value: { stringValue: userId } },
      ]);
    } else {
      const credits = plan === "pro_plus" ? 15 : 5;
      await runSql(
        "UPDATE subscriptions SET stripe_subscription_id = :sid, plan = :plan, status = 'active', analysis_credits = :credits, current_period_end = :end, updated_at = NOW() WHERE user_id = :uid",
        [
          { name: "sid", value: { stringValue: data.subscription || "" } },
          { name: "plan", value: { stringValue: plan } },
          { name: "credits", value: { longValue: credits } },
          { name: "end", value: { stringValue: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000).toISOString() } },
          { name: "uid", value: { stringValue: userId } },
        ]
      );
    }
  } else if (type === "customer.subscription.updated" || type === "customer.subscription.deleted") {
    const subId = data.id;
    const status = data.status;
    if (status === "canceled" || status === "unpaid" || type === "customer.subscription.deleted") {
      await runSql("UPDATE subscriptions SET status = 'canceled', plan = 'free', analysis_credits = 0, updated_at = NOW() WHERE stripe_subscription_id = :sid", [
        { name: "sid", value: { stringValue: subId } },
      ]);
    } else if (status === "active") {
      const periodEnd = data.current_period_end ? new Date(data.current_period_end * 1000).toISOString() : null;
      if (periodEnd) {
        await runSql("UPDATE subscriptions SET status = 'active', current_period_end = :end, updated_at = NOW() WHERE stripe_subscription_id = :sid", [
          { name: "end", value: { stringValue: periodEnd } },
          { name: "sid", value: { stringValue: subId } },
        ]);
      }
    }
  } else if (type === "invoice.paid") {
    const subId = data.subscription;
    if (subId) {
      const subRow = await runSql("SELECT plan FROM subscriptions WHERE stripe_subscription_id = :sid", [
        { name: "sid", value: { stringValue: subId } },
      ]);
      const rows = parseRows(subRow);
      if (rows.length > 0) {
        const credits = rows[0].plan === "pro_plus" ? 15 : 5;
        await runSql("UPDATE subscriptions SET analysis_credits = :credits, status = 'active', updated_at = NOW() WHERE stripe_subscription_id = :sid", [
          { name: "credits", value: { longValue: credits } },
          { name: "sid", value: { stringValue: subId } },
        ]);
      }
    }
  }

  return respond(200, { received: true });
}

// ─── AI Video Analysis Handler ───
async function handleAIAnalysis(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { videoId, analysisType, videoKey } = body;
  if (!videoId || !analysisType) return respond(400, { error: "videoId and analysisType are required" });

  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });

  // Check credits
  const sub = await runSql("SELECT plan, analysis_credits FROM subscriptions WHERE user_id = :uid", [
    { name: "uid", value: { stringValue: userId } },
  ]);
  const subRows = parseRows(sub);
  const credits = subRows.length > 0 ? parseInt(subRows[0].analysis_credits || "0", 10) : 0;
  if (credits <= 0) {
    return respond(403, { error: "No analysis credits remaining", upgradeRequired: true });
  }

  // Update video status
  await runSql("UPDATE videos SET status = 'analyzing' WHERE id = :id", [
    { name: "id", value: { stringValue: videoId } },
  ]);

  const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
  let analysisResult;

  if (OPENAI_API_KEY) {
    try {
      // Get video from S3 and generate pre-signed URL for reference
      const key = videoKey || "";
      let videoUrl = "";
      if (key) {
        videoUrl = await getSignedUrl(s3, new GetObjectCommand({
          Bucket: BUCKET_NAME,
          Key: key,
        }), { expiresIn: 600 });
      }

      const systemPrompt = `You are an expert cricket coach and biomechanics analyst. Analyze the cricket ${analysisType} video described below. Return ONLY valid JSON matching this exact structure:
{
  "player_type": "batsman | bowler | all_rounder",
  "analysis_type": "${analysisType}",
  "overall_score": <number 0-100>,
  "summary": "<2-3 sentence overview>",
  "strengths": ["<strength1>", "<strength2>", "<strength3>"],
  "weaknesses": ["<weakness1>", "<weakness2>"],
  "technical_feedback": {
    ${analysisType === "batting" ? '"stance": "<feedback>", "footwork": "<feedback>", "balance": "<feedback>", "timing": "<feedback>", "follow_through": "<feedback>"' : '"run_up": "<feedback>", "front_arm": "<feedback>", "bowling_arm": "<feedback>", "release": "<feedback>", "follow_through": "<feedback>"'}
  },
  "recommended_drills": [
    {"name": "<drill name>", "purpose": "<why>", "instructions": "<how to do it>"}
  ],
  "next_steps": ["<step1>", "<step2>", "<step3>"]
}`;

      const userPrompt = `Analyze this ${analysisType} video. The player has uploaded a cricket ${analysisType} session for analysis. ${videoUrl ? `Video reference: ${videoUrl}` : "Provide general analysis based on typical amateur cricket technique."} Please provide detailed, actionable feedback that will help the player improve.`;

      const openaiRes = await fetch("https://api.openai.com/v1/chat/completions", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${OPENAI_API_KEY}`,
        },
        body: JSON.stringify({
          model: "gpt-4o",
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt },
          ],
          temperature: 0.7,
          max_tokens: 2000,
          response_format: { type: "json_object" },
        }),
      });
      const openaiData = await openaiRes.json();
      const content = openaiData.choices?.[0]?.message?.content;
      if (content) {
        analysisResult = JSON.parse(content);
      }
    } catch (e) {
      console.error("OpenAI analysis error:", e);
    }
  }

  // Fallback if OpenAI is unavailable or fails
  if (!analysisResult) {
    analysisResult = generateFallbackAnalysis(analysisType);
  }

  // Save analysis
  const analysisId = crypto.randomUUID();
  await runSql(
    "INSERT INTO analysis (id, user_id, analysis_type, scores, feedback, video_ref) VALUES (:id, :uid, :type, :scores::jsonb, :feedback, :ref)",
    [
      { name: "id", value: { stringValue: analysisId } },
      { name: "uid", value: { stringValue: userId } },
      { name: "type", value: { stringValue: analysisType } },
      { name: "scores", value: { stringValue: JSON.stringify({ overall: analysisResult.overall_score, ...analysisResult.technical_feedback }) } },
      { name: "feedback", value: { stringValue: analysisResult.summary } },
      { name: "ref", value: { stringValue: videoId } },
    ]
  );

  // Deduct credit
  await runSql(
    "UPDATE subscriptions SET analysis_credits = analysis_credits - 1, updated_at = NOW() WHERE user_id = :uid",
    [{ name: "uid", value: { stringValue: userId } }]
  );

  // Update video status
  await runSql("UPDATE videos SET status = 'analyzed' WHERE id = :id", [
    { name: "id", value: { stringValue: videoId } },
  ]);

  // Update best score on player profile
  const currentBest = await runSql(
    "SELECT best_score FROM player_profiles WHERE user_id = :uid",
    [{ name: "uid", value: { stringValue: userId } }]
  );
  const bestRows = parseRows(currentBest);
  if (bestRows.length > 0 && analysisResult.overall_score > parseInt(bestRows[0].best_score || "0", 10)) {
    await runSql("UPDATE player_profiles SET best_score = :score WHERE user_id = :uid", [
      { name: "score", value: { longValue: analysisResult.overall_score } },
      { name: "uid", value: { stringValue: userId } },
    ]);
  }

  return respond(200, { analysisId, ...analysisResult, confidence: OPENAI_API_KEY ? "high" : "moderate" });
}

function generateFallbackAnalysis(analysisType) {
  const isBatting = analysisType === "batting";
  return {
    player_type: isBatting ? "batsman" : "bowler",
    analysis_type: analysisType,
    overall_score: 65 + Math.floor(Math.random() * 20),
    summary: isBatting
      ? "Good batting foundation with room for improvement in footwork and timing. Focus on front-foot play and weight transfer for more consistent shot-making."
      : "Decent bowling action with a smooth run-up. Work on front arm position and release point consistency to improve accuracy and pace.",
    strengths: isBatting
      ? ["Solid defensive technique", "Good head position", "Balanced setup at the crease"]
      : ["Smooth run-up rhythm", "Good follow-through", "Consistent action"],
    weaknesses: isBatting
      ? ["Front foot movement needs improvement", "Weight transfer slightly delayed"]
      : ["Front arm drops early", "Release point inconsistent"],
    technical_feedback: isBatting
      ? { stance: "Solid base, consider slightly wider stance for better coverage.", footwork: "Back-foot movement is good. Front-foot commitment needs work.", balance: "Generally good balance. Watch for falling toward off side on cuts.", timing: "Good timing on back-foot shots. Front-foot timing needs practice.", follow_through: "Clean follow-through. Extend arms more on drives." }
      : { run_up: "Smooth and rhythmic. Good acceleration to the crease.", front_arm: "Drops slightly early. Keep it up longer for better alignment.", bowling_arm: "Good height at release. Maintain vertical position.", release: "Slight inconsistency in release point. Focus on repeating position.", follow_through: "Good follow-through. Continue rotating fully." },
    recommended_drills: [
      { name: isBatting ? "Shadow Batting Drill" : "Target Bowling Drill", purpose: isBatting ? "Improve muscle memory for footwork" : "Improve accuracy and consistency", instructions: isBatting ? "Practice your stance, trigger movement, and shot execution without a ball. Focus on front-foot stride length. 50 reps daily." : "Place a target on a good length. Bowl 30 balls aiming at the target. Track hit percentage." },
      { name: isBatting ? "Throwdown Practice" : "Front Arm Drill", purpose: isBatting ? "Improve timing and shot selection" : "Keep front arm up longer", instructions: isBatting ? "Face throwdowns from 15 yards. Alternate between defensive and attacking shots. 30 balls per session." : "Bowl with focus on keeping front arm pointing at target until release. Film yourself to check. 20 balls per session." },
    ],
    next_steps: [
      `Focus on ${isBatting ? "front-foot movement" : "release point consistency"} for the next 2 weeks`,
      "Upload another video after practicing to track improvement",
      `Consider working with a ${isBatting ? "batting" : "bowling"} coach for personalized guidance`,
    ],
  };
}

// ─── Player Profile Handlers ───
async function handleGetPlayerProfile(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT pp.* FROM player_profiles pp JOIN users u ON pp.user_id = u.id WHERE u.email = :email",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "Player profile not found" });
  return respond(200, rows[0]);
}

async function handleCreatePlayerProfile(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { username, age, location, role, battingStyle, bowlingStyle, academy, bio } = body;
  if (!username) return respond(400, { error: "username is required" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const id = crypto.randomUUID();
  try {
    await runSql(
      `INSERT INTO player_profiles (id, user_id, username, age, location, role, batting_style, bowling_style, academy, bio)
       VALUES (:id, :uid, :username, :age, :location, :role, :bat, :bowl, :academy, :bio)`,
      [
        { name: "id", value: { stringValue: id } },
        { name: "uid", value: { stringValue: userId } },
        { name: "username", value: { stringValue: username } },
        { name: "age", value: age ? { longValue: age } : { isNull: true } },
        { name: "location", value: { stringValue: location || "" } },
        { name: "role", value: { stringValue: role || "batsman" } },
        { name: "bat", value: { stringValue: battingStyle || "" } },
        { name: "bowl", value: { stringValue: bowlingStyle || "" } },
        { name: "academy", value: { stringValue: academy || "" } },
        { name: "bio", value: { stringValue: bio || "" } },
      ]
    );
    // Create a free subscription for the user
    const subId = crypto.randomUUID();
    await runSql(
      `INSERT INTO subscriptions (id, user_id, plan, status, analysis_credits)
       VALUES (:id, :uid, 'free', 'active', 1)
       ON CONFLICT (user_id) DO NOTHING`,
      [
        { name: "id", value: { stringValue: subId } },
        { name: "uid", value: { stringValue: userId } },
      ]
    );
    return respond(201, { id, message: "Player profile created" });
  } catch (err) {
    if (err.message?.includes("unique") || err.message?.includes("duplicate")) {
      return respond(409, { error: "Username already taken" });
    }
    return respond(500, { error: err.message });
  }
}

async function handleUpdatePlayerProfile(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { age, location, role, battingStyle, bowlingStyle, academy, bio, publicProfileEnabled } = body;
  await runSql(
    `UPDATE player_profiles SET
      age = COALESCE(:age, age),
      location = COALESCE(:location, location),
      role = COALESCE(:role, role),
      batting_style = COALESCE(:bat, batting_style),
      bowling_style = COALESCE(:bowl, bowling_style),
      academy = COALESCE(:academy, academy),
      bio = COALESCE(:bio, bio),
      public_profile_enabled = COALESCE(:pub, public_profile_enabled),
      updated_at = NOW()
    WHERE user_id = (SELECT id FROM users WHERE email = :email)`,
    [
      { name: "age", value: age ? { longValue: age } : { isNull: true } },
      { name: "location", value: location ? { stringValue: location } : { isNull: true } },
      { name: "role", value: role ? { stringValue: role } : { isNull: true } },
      { name: "bat", value: battingStyle ? { stringValue: battingStyle } : { isNull: true } },
      { name: "bowl", value: bowlingStyle ? { stringValue: bowlingStyle } : { isNull: true } },
      { name: "academy", value: academy ? { stringValue: academy } : { isNull: true } },
      { name: "bio", value: bio ? { stringValue: bio } : { isNull: true } },
      { name: "pub", value: publicProfileEnabled !== undefined ? { booleanValue: publicProfileEnabled } : { isNull: true } },
      { name: "email", value: { stringValue: user.email } },
    ]
  );
  return respond(200, { message: "Profile updated" });
}

async function handleGetPublicProfile(event) {
  const username = event.pathParameters?.username || event.path.split("/").pop();
  const result = await runSql(
    `SELECT pp.*, u.full_name, u.avatar_url FROM player_profiles pp
     JOIN users u ON pp.user_id = u.id
     WHERE pp.username = :username AND pp.public_profile_enabled = TRUE`,
    [{ name: "username", value: { stringValue: username } }]
  );
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "Profile not found" });
  // Get analysis history
  const analyses = await runSql(
    `SELECT a.analysis_type, a.scores, a.created_at FROM analysis a
     WHERE a.user_id = :uid ORDER BY a.created_at DESC LIMIT 10`,
    [{ name: "uid", value: { stringValue: rows[0].user_id } }]
  );
  return respond(200, { profile: rows[0], analyses: parseRows(analyses) });
}

// ─── Video & Analysis Handlers ───
async function handleCreateVideo(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { videoType } = body;
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });

  // Check analysis credits
  const sub = await runSql(
    "SELECT plan, analysis_credits FROM subscriptions WHERE user_id = :uid",
    [{ name: "uid", value: { stringValue: userId } }]
  );
  const subRows = parseRows(sub);
  const credits = subRows.length > 0 ? parseInt(subRows[0].analysis_credits || "0", 10) : 1;
  if (credits <= 0) {
    return respond(403, { error: "No analysis credits remaining. Please upgrade your plan.", upgradeRequired: true });
  }

  const ext = body.extension || "mp4";
  const contentType = body.contentType || "video/mp4";
  const videoId = crypto.randomUUID();
  const key = `videos/${userId}/${videoId}.${ext}`;
  const uploadUrl = await getSignedUrl(s3, new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: key,
    ContentType: contentType,
  }), { expiresIn: 600 });

  await runSql(
    `INSERT INTO videos (id, user_id, video_key, video_type, status)
     VALUES (:id, :uid, :key, :vtype, 'uploaded')`,
    [
      { name: "id", value: { stringValue: videoId } },
      { name: "uid", value: { stringValue: userId } },
      { name: "key", value: { stringValue: key } },
      { name: "vtype", value: { stringValue: videoType || "batting" } },
    ]
  );
  return respond(200, { videoId, uploadUrl, key });
}

async function handleGetVideo(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const videoId = event.pathParameters?.videoId || event.path.split("/").pop();
  const result = await runSql(
    "SELECT * FROM videos WHERE id = :id AND user_id = (SELECT id FROM users WHERE email = :email)",
    [
      { name: "id", value: { stringValue: videoId } },
      { name: "email", value: { stringValue: user.email } },
    ]
  );
  const rows = parseRows(result);
  if (rows.length === 0) return respond(404, { error: "Video not found" });
  return respond(200, rows[0]);
}

async function handleGetUserVideos(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const result = await runSql(
    "SELECT * FROM videos WHERE user_id = (SELECT id FROM users WHERE email = :email) ORDER BY created_at DESC",
    [{ name: "email", value: { stringValue: user.email } }]
  );
  return respond(200, parseRows(result));
}

async function handleGetSubscription(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const result = await runSql("SELECT * FROM subscriptions WHERE user_id = :uid", [
    { name: "uid", value: { stringValue: userId } },
  ]);
  const rows = parseRows(result);
  if (rows.length === 0) {
    return respond(200, { plan: "free", status: "active", analysis_credits: 1 });
  }
  return respond(200, rows[0]);
}

// ─── Coach Request Handlers ───
async function handleCreateCoachRequest(event, body) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const { coachId, message } = body;
  if (!coachId) return respond(400, { error: "coachId is required" });
  const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const userId = parseRows(userRow)[0]?.id;
  if (!userId) return respond(404, { error: "User not found" });
  const id = crypto.randomUUID();
  await runSql(
    "INSERT INTO coach_requests (id, user_id, coach_id, message) VALUES (:id, :uid, :cid, :msg)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "uid", value: { stringValue: userId } },
      { name: "cid", value: { stringValue: coachId } },
      { name: "msg", value: { stringValue: message || "" } },
    ]
  );
  return respond(201, { id, message: "Coach request submitted" });
}

async function handleGetCoachRequests(event) {
  const user = await getUserFromToken(event);
  if (!user) return respond(401, { error: "Unauthorized" });
  const dbUser = await runSql("SELECT id, role FROM users WHERE email = :email", [
    { name: "email", value: { stringValue: user.email } },
  ]);
  const rows = parseRows(dbUser);
  if (!rows.length || rows[0].role !== "admin") return respond(403, { error: "Admin access required" });
  const result = await runSql(
    `SELECT cr.*, u.full_name as user_name, u.email as user_email, c.name as coach_name
     FROM coach_requests cr
     JOIN users u ON cr.user_id = u.id
     JOIN coaches c ON cr.coach_id = c.id
     ORDER BY cr.created_at DESC`
  );
  return respond(200, parseRows(result));
}

async function handleGetCoaches() {
  const result = await runSql("SELECT * FROM coaches WHERE active = TRUE ORDER BY name ASC");
  return respond(200, parseRows(result));
}

// ─── Analytics Events Handler ───
async function handleTrackEvent(event, body) {
  const { eventName, eventData, userId } = body;
  if (!eventName) return respond(400, { error: "eventName is required" });
  const id = crypto.randomUUID();
  let uid = userId || "";
  if (!uid) {
    try {
      const user = await getUserFromToken(event);
      if (user) {
        const userRow = await runSql("SELECT id FROM users WHERE email = :email", [
          { name: "email", value: { stringValue: user.email } },
        ]);
        uid = parseRows(userRow)[0]?.id || "";
      }
    } catch { /* anonymous event */ }
  }
  await runSql(
    "INSERT INTO analytics_events (id, user_id, event_name, event_data) VALUES (:id, :uid, :name, :data::jsonb)",
    [
      { name: "id", value: { stringValue: id } },
      { name: "uid", value: { stringValue: uid } },
      { name: "name", value: { stringValue: eventName } },
      { name: "data", value: { stringValue: JSON.stringify(eventData || {}) } },
    ]
  );
  return respond(200, { message: "Event tracked" });
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

    // Player profile routes
    if (path === "/player-profiles" && method === "GET") return await handleGetPlayerProfile(event);
    if (path === "/player-profiles" && method === "POST") return await handleCreatePlayerProfile(event, body);
    if (path === "/player-profiles" && method === "PUT") return await handleUpdatePlayerProfile(event, body);
    if (path.match(/^\/player\/[^/]+$/) && method === "GET") return await handleGetPublicProfile(event);

    // Video routes
    if (path === "/videos" && method === "POST") return await handleCreateVideo(event, body);
    if (path === "/videos" && method === "GET") return await handleGetUserVideos(event);
    if (path.match(/^\/videos\/[^/]+$/) && method === "GET") return await handleGetVideo(event);

    // AI Analysis
    if (path === "/ai-analysis" && method === "POST") return await handleAIAnalysis(event, body);

    // Subscription & Stripe routes
    if (path === "/subscriptions/status" && method === "GET") return await handleGetSubscription(event);
    if (path === "/checkout" && method === "POST") return await handleCreateCheckout(event, body);
    if (path === "/stripe/webhook" && method === "POST") return await handleStripeWebhook(event);

    // Coach routes (marketplace)
    if (path === "/coaches" && method === "GET") return await handleGetCoaches();
    if (path === "/coach-requests" && method === "POST") return await handleCreateCoachRequest(event, body);
    if (path === "/coach-requests" && method === "GET") return await handleGetCoachRequests(event);

    // Analytics events
    if (path === "/events" && method === "POST") return await handleTrackEvent(event, body);

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
