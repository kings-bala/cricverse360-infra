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
      "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key",
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
  if (!token) return null;
  try {
    const cmd = new GetUserCommand({ AccessToken: token });
    const user = await cognito.send(cmd);
    const attrs = {};
    for (const attr of user.UserAttributes || []) {
      attrs[attr.Name] = attr.Value;
    }
    return { username: user.Username, ...attrs };
  } catch {
    return null;
  }
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
  ];
  for (const sql of tables) {
    await runSql(sql);
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

// ─── Main Router ───
export async function handler(event) {
  const method = event.httpMethod;
  const path = event.resource || event.path;
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

    // Health check
    if (path === "/" && method === "GET") {
      return respond(200, { status: "ok", service: "CricVerse360 API", version: "1.0.0" });
    }

    return respond(404, { error: "Route not found", path, method });
  } catch (err) {
    console.error("Handler error:", err);
    return respond(500, { error: "Internal server error", details: err.message });
  }
}
