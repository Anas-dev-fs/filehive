const loadEnvironment = require("./LoadEnvironment");
loadEnvironment();
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
require("dotenv").config();

const app = express();
const server = http.createServer(app);
const { pipeline } = require("stream/promises");

const MAX_FILE_BYTES = parseFileSize(process.env.MAX_FILE_SIZE) || Infinity;

server.requestTimeout = 0;
server.keepAliveTimeout = 65_000;
server.headersTimeout = 66_000;

function overrideConsole() {
  ["log", "error", "warn", "info", "debug"].forEach((method) => {
    const original = console[method];

    console[method] = function (...args) {
      const timestamp = new Intl.DateTimeFormat("en-US", {
        timeZone: "Asia/Karachi",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour12: true,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        fractionalSecondDigits: 3,
      }).format(new Date());

      const green = "\x1b[32m";
      const red = "\x1b[31m";
      const blue = "\x1b[34m";
      const yellow = "\x1b[33m";
      const cyan = "\x1b[36m";
      const reset = "\x1b[0m";

      const prefix = `${yellow}${timestamp}${reset} ${cyan}[${method.toUpperCase()}]${reset} :::`;

      const processedArgs = args.map((arg) => safeFormat(arg));

      original.call(console, prefix, ...processedArgs);
    };
  });
}
const MAX_LENGTH = 1000;

function safeStringify(arg) {
  try {
    if (arg instanceof Error) {
      return `${arg.name}: ${arg.message}\n${arg.stack}`;
    }

    if (typeof arg === "string") {
      return arg.length > MAX_LENGTH
        ? arg.substring(0, MAX_LENGTH) + "... [TRUNCATED]"
        : arg;
    }

    if (typeof arg === "object" && arg !== null) {
      const str = JSON.stringify(arg);
      return str.length > MAX_LENGTH
        ? str.substring(0, MAX_LENGTH) + "... [TRUNCATED]"
        : str;
    }

    return arg;
  } catch (e) {
    return "[Unserializable Object]";
  }
}
function safeFormat(arg) {
  try {
    if (arg instanceof Error) {
      return {
        name: arg.name,
        message: arg.message,
        stack: arg.stack,
      };
    }

    if (typeof arg === "string") {
      return arg.length > MAX_LENGTH
        ? arg.substring(0, MAX_LENGTH) + "... [TRUNCATED]"
        : arg;
    }

    if (typeof arg === "object" && arg !== null) {
      const json = JSON.stringify(arg);

      if (json.length > MAX_LENGTH) {
        return {
          ...arg,
          _truncated: true,
        };
      }

      return arg;
    }

    return arg;
  } catch {
    return "[Unserializable Object]";
  }
}

overrideConsole();

const INACTIVITY_LIMIT_MS =
  Number(process.env.ROOM_INACTIVITY_MINUTES || 60) * 60 * 1000;
const INACTIVITY_WARNING_MS = 5 * 60 * 1000; // end hone se 5 min pehle warning
const SWEEP_INTERVAL_MS = 60 * 1000;

// Har activity par ye call karo
function touchRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room || room.ended) return;

  room.lastActivityAt = Date.now();
  room.inactivityWarned = false;
}
function endRoom(roomId, { message, reason }) {
  const room = rooms.get(roomId);
  if (!room || room.ended) return false;

  room.ended = true;
  room.messages = [];
  saveRooms();

  deleteRoomFiles(roomId);

  io.to(roomId).emit("room_ended", { message, reason });

  // Set ko copy karo, warna leave() karte waqt wahi set badalta hai jis par loop chal raha hai
  const socketsInRoom = io.sockets.adapter.rooms.get(roomId);
  if (socketsInRoom) {
    [...socketsInRoom].forEach((sid) => {
      const s = io.sockets.sockets.get(sid);
      if (s) {
        s.data = {};
        s.leave(roomId);
      }
    });
  }

  room.members.clear();
  console.log(`Room ${roomId} ended (${reason}) — files deleted`);
  return true;
}

// Idle rooms ka sweeper
setInterval(() => {
  const now = Date.now();

  for (const room of rooms.values()) {
    if (room.ended) continue;
    if (room.id.startsWith("MY-")) continue; // personal room exempt

    if (!room.lastActivityAt) {
      room.lastActivityAt = now;
      continue;
    }

    const idleMs = now - room.lastActivityAt;

    if (idleMs >= INACTIVITY_LIMIT_MS) {
      endRoom(room.id, {
        reason: "inactivity",
        message:
          "This room was closed because there was no activity for 1 hour. All files have been deleted.",
      });
    } else if (
      idleMs >= INACTIVITY_LIMIT_MS - INACTIVITY_WARNING_MS &&
      !room.inactivityWarned
    ) {
      room.inactivityWarned = true;

      io.to(room.id).emit("room_inactivity_warning", {
        minutesLeft: Math.ceil((INACTIVITY_LIMIT_MS - idleMs) / 60000),
      });
    }
  }
}, SWEEP_INTERVAL_MS);

const allowedOrigins = (process.env.ALLOWED_ORIGINS || "")
  .split(",")
  .map((origin) => origin.trim())
  .filter((origin) => origin !== "");

const LogRequest = async (req, res, start) => {
  const timestamp = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Karachi",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour12: true,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  }).format(new Date());

  const green = "\x1b[32m";
  const red = "\x1b[31m";
  const blue = "\x1b[34m";
  const yellow = "\x1b[33m";
  const cyan = "\x1b[36m";
  const reset = "\x1b[0m";

  const duration = Date.now() - start;

  const clientIp =
    req.headers["x-forwarded-for"]?.split(",")[0]?.trim() ||
    req.socket.remoteAddress ||
    "Unknown IP";

  let customer = req?.user?.CustomerId ?? req?.user?.UserId ?? "PUBLIC";

  let statusColor = green;
  if (res.statusCode >= 500) statusColor = red;
  else if (res.statusCode >= 400) statusColor = yellow;
  else if (res.statusCode >= 300) statusColor = cyan;

  console.log(
    `${yellow}${timestamp}${reset} ` +
      `${blue} :::Initiator ${reset} ` +
      `${red}${clientIp}${reset} ` +
      `[${green}${req.hostname}[${customer}]${reset}] ` +
      `${req.method} ${req.originalUrl} ` +
      `-> ${statusColor}${res.statusCode}${reset} ` +
      `${cyan}${duration}ms${reset}`,
  );
};

// ─── Storage Setup ────────────────────────────────────────
const DATA_DIR = path.join(__dirname, "data");
const ROOMS_FILE = path.join(DATA_DIR, "rooms.json");
const FILES_DIR = path.join(DATA_DIR, "files");
[DATA_DIR, FILES_DIR].forEach((d) => fs.mkdirSync(d, { recursive: true }));

// ─── Persistence ─────────────────────────────────────────
function loadRooms() {
  try {
    if (fs.existsSync(ROOMS_FILE)) {
      const parsed = JSON.parse(fs.readFileSync(ROOMS_FILE, "utf8"));

      const map = new Map();

      for (const [id, room] of Object.entries(parsed)) {
        map.set(id, {
          ...room,

          lastActivityAt: Date.now(),

          // Active socket connections are recreated
          // whenever users connect.
          members: new Map(),

          // Persistent members/invitations
          memberList: room.memberList || [],

          // Historical members
          pastMembers: room.pastMembers || [],

          // Users permanently blocked from rejoining
          // this room after being kicked.
          kickedMemberIds: room.kickedMemberIds || [],

          messages: room.messages || [],
        });
      }

      return map;
    }
  } catch (e) {
    console.error("Failed to load rooms:", e.message);
  }

  return new Map();
}

function saveRooms() {
  try {
    const obj = {};

    for (const [id, room] of rooms.entries()) {
      obj[id] = {
        id: room.id,
        name: room.name,
        creatorId: room.creatorId,
        createdAt: room.createdAt,
        ended: room.ended || false,
        files: room.files,

        messages: room.messages || [],

        // Persistent room members + invitation status
        memberList: room.memberList || [],

        // Historical members for Recent Rooms / re-invite
        pastMembers: room.pastMembers || [],

        // Users kicked from this room
        kickedMemberIds: room.kickedMemberIds || [],

        messages: room.messages || [],
      };
    }

    fs.writeFileSync(ROOMS_FILE, JSON.stringify(obj, null, 2));
  } catch (e) {
    console.error("Failed to save rooms:", e.message);
  }
}

function deleteRoomFiles(roomId) {
  discardRoomUploads(roomId);
  const roomDir = path.join(FILES_DIR, roomId);
  try {
    if (fs.existsSync(roomDir))
      fs.rmSync(roomDir, { recursive: true, force: true });
    console.log(`Deleted files for room ${roomId}`);
  } catch (e) {
    console.error("Failed to delete room files:", e.message);
  }
}

function parseFileSize(sizeStr) {
  if (!sizeStr) return 0;

  const units = {
    KB: 1024,
    MB: 1024 * 1024,
    GB: 1024 * 1024 * 1024,
  };

  const match = sizeStr.toUpperCase().match(/^(\d+)(KB|MB|GB)$/);
  if (!match) return Number(sizeStr) || 0;

  const [, value, unit] = match;
  return Number(value) * units[unit];
}

function getUniqueFileName(room, uploaderId, originalName) {
  const taken = new Set(
    (room.files || [])
      .filter((f) => String(f.uploaderId) === String(uploaderId))
      .map((f) => f.fileName.toLowerCase()),
  );

  if (!taken.has(originalName.toLowerCase())) return originalName;

  const ext = path.extname(originalName);
  const base = path.basename(originalName, ext);

  let n = 1;
  let candidate;
  do {
    candidate = `${base} (${n})${ext}`;
    n++;
  } while (taken.has(candidate.toLowerCase()));

  return candidate;
}

// ─── Multer ───────────────────────────────────────────────
const storage = multer.diskStorage({
  destination(req, file, cb) {
    const roomDir = path.join(FILES_DIR, req.params.roomId);
    fs.mkdirSync(roomDir, { recursive: true });
    cb(null, roomDir);
  },
  filename(req, file, cb) {
    cb(null, `${uuidv4()}${path.extname(file.originalname)}`);
  },
});
const upload = multer({
  storage,
  limits: { fileSize: parseFileSize(process.env.MAX_FILE_SIZE) },
});

function uploadSingle(req, res, next) {
  upload.single("file")(req, res, (err) => {
    if (!err) return next();

    console.log("req destroyed:", req.destroyed);
    console.log("req aborted:", req.aborted);

    // Client cancelled the upload (xhr.abort / closed tab / network drop)
    if (err.message === "Request aborted" || req.aborted || req.destroyed) {
      console.log(`Upload cancelled by client: room ${req.params.roomId}`);
      return; // don't respond, socket is already gone
    }

    if (err instanceof multer.MulterError) {
      return res.status(400).json({ error: err.message });
    }

    return next(err);
  });
}

// function trackUploadProgress(req, res, next) {
//   const totalBytes = parseInt(req.headers["content-length"] || "0", 10);
//   let receivedBytes = 0;

//   const rawName = req.headers["x-file-name"];
//   let fileName = "Unknown";

//   try {
//     if (rawName) fileName = decodeURIComponent(rawName);
//   } catch {
//     fileName = rawName; // agar decode fail ho to raw hi dikha do
//   }

//   console.log(`Upload started: "${fileName}" (room ${req.params.roomId})`);

//   req.on("data", (chunk) => {
//     receivedBytes += chunk.length;

//     const receivedMB = (receivedBytes / (1024 * 1024)).toFixed(2);
//     const totalMB = (totalBytes / (1024 * 1024)).toFixed(2);

//     console.log(
//       `Server received: ${receivedMB} MB / ${totalMB} MB (room ${req.params.roomId}, Filename:${fileName})`,
//     );
//   });

//   req.on("end", () => {
//     const receivedMB = (receivedBytes / (1024 * 1024)).toFixed(2);
//     const totalMB = (totalBytes / (1024 * 1024)).toFixed(2);

//     console.log(
//       `Server finished receiving request: ${receivedMB} MB / ${totalMB} MB, Filename:${fileName}`,
//     );
//   });

//   req.on("aborted", () => {
//     const receivedMB = (receivedBytes / (1024 * 1024)).toFixed(2);
//     const totalMB = (totalBytes / (1024 * 1024)).toFixed(2);

//     console.log(
//       `Upload aborted: ${receivedMB} MB / ${totalMB} MB, Filename:${fileName}`,
//     );
//   });

//   next();
// }

function trackUploadProgress(req, res, next) {
  const roomId = req.params.roomId;
  const totalBytes = parseInt(req.headers["content-length"] || "0", 10);

  touchRoom(req.params.roomId);

  let fileName = "Unknown";
  try {
    const rawName = req.headers["x-file-name"];
    if (rawName) fileName = decodeURIComponent(rawName);
  } catch {
    fileName = req.headers["x-file-name"] || "Unknown";
  }

  const uploadId = req.headers["x-upload-id"];
  const socketId = req.headers["x-socket-id"];

  // Sirf us socket ko bhejo jo isi room mein ho (spoofing se bachne ke liye)
  const targetSocket = socketId ? io.sockets.sockets.get(socketId) : null;
  const canEmit =
    uploadId && targetSocket && targetSocket.data?.roomId === roomId;

  let receivedBytes = 0;
  let lastPercent = -1;
  let lastEmitAt = 0;
  let lastLoggedStep = -1;
  const startedAt = Date.now();
  let lastChunkAt = Date.now();

  const emitProgress = () => {
    const percent = totalBytes
      ? Math.min(99, Math.floor((receivedBytes / totalBytes) * 100))
      : 0;

    // Console: har 10% par ek line (har chunk par log karna bohot noisy hai)
    const step = Math.floor(percent / 10);
    // if (step !== lastLoggedStep) {
    //   lastLoggedStep = step;
    console.log(
      `[${fileName}] server received ${percent}% ` +
        `(${(receivedBytes / 1048576).toFixed(2)} / ${(totalBytes / 1048576).toFixed(2)} MB)`,
    );
    // }

    if (!canEmit) return;

    const now = Date.now();

    // Throttle: percent badla ho aur kam az kam 200ms guzar chuke hon
    if (percent === lastPercent || now - lastEmitAt < 200) return;

    lastPercent = percent;
    lastEmitAt = now;

    targetSocket.emit("upload_progress", {
      uploadId,
      percent,
      received: receivedBytes,
      total: totalBytes,
    });
  };

  req.on("data", (chunk) => {
    receivedBytes += chunk.length;
    emitProgress();
  });

  req.on("end", () => {
    console.log(`[${fileName}] server finished receiving request`);
  });

  req.on("aborted", () => {
    console.log(
      `[${fileName}] ABORTED ${receivedBytes}/${totalBytes} bytes | ` +
        `elapsed ${((Date.now() - startedAt) / 1000).toFixed(1)}s | ` +
        `idle before abort ${Date.now() - lastChunkAt}ms | ` +
        `socketDestroyed=${req.socket.destroyed}`,
    );
  });
  req.on("error", (e) => {
    console.log(
      `[${fileName}] req error: name=${e.name} code=${e.code} msg=${e.message}`,
    );
  });

  req.on("close", () => {
    if (!req.complete)
      console.log(`[${fileName}] connection closed before upload completed`);
  });
  next();
}

app.use((req, res, next) => {
  const start = Date.now();

  res.on("finish", () => {
    LogRequest(req, res, start);
  });

  next();
});

// ─── Express + Socket.IO ──────────────────────────────────

// app.use(
//   cors({
//     origin: (origin, callback) => {
//       console.log("Incoming origin:", origin);

//       if (!origin || allowedOrigins.includes(origin)) {
//         callback(null, true);
//       } else {
//         callback(null, false); // ✅ DO NOT THROW ERROR
//       }
//     },
//     methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
//     credentials: true, // ✅ enable if needed
//     allowedHeaders: ["Content-Type", "Authorization"],
//   })
// );

const corsOptions = {
  origin: (origin, callback) => {
    console.log("Incoming origin:", origin);

    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error(`CORS blocked origin: ${origin}`));
    }
  },
  methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
  credentials: true,
  allowedHeaders: [
    "Content-Type",
    "Authorization",
    "X-File-Name",
    "X-Upload-Id",
    "X-Socket-Id",
    "X-Upload-Offset",
  ],
  maxAge: 86400,
};

app.use(cors(corsOptions));

app.use(express.json());

const io = new Server(server, {
  pingInterval: 25_000,
  pingTimeout: 60_000,
  cors: {
    origin: (origin, callback) => {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        callback(new Error("Not allowed"));
      }
    },
    methods: ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
  },
});

const rooms = loadRooms();
console.log(`Loaded ${rooms.size} persisted room(s)`);

// ─── Helpers ──────────────────────────────────────────────
function getRoomPublicData(room) {
  const onlineUserIds = new Set();

  for (const member of room.members.values()) {
    if (member.id) {
      onlineUserIds.add(String(member.id));
    }
  }

  const memberList = room.memberList || [];

  return {
    id: room.id,
    name: room.name,
    creatorId: room.creatorId,
    createdAt: room.createdAt,
    ended: room.ended || false,

    memberCount: memberList.length,

    members: memberList.map((member) => ({
      id: member.id,
      name: member.name,
      email: member.email || "",
      invitationStatus: member.invitationStatus || "accepted",

      // THIS is the important part
      online: onlineUserIds.has(String(member.id)),
    })),

    pastMembers: room.pastMembers || [],

    files: (room.files || []).filter((file) => !file.deleted),

    deletedFiles: (room.files || []).filter((file) => file.deleted),

    messages: room.messages || [],
  };
}

// Generate a deterministic room ID from Microsoft user ID for "My Room"
function getPersonalRoomId(msUserId) {
  // Create a simple hash of the user ID and take first 8 chars
  const crypto = require("crypto");
  const hash = crypto.createHash("sha256").update(msUserId).digest("hex");
  return "MY-" + hash.slice(0, 6).toUpperCase();
}

// ─── REST: Get recent rooms by IDs ────────────────────────
// Client passes comma-separated room IDs it has cached locally.
// Server returns the subset that still exist with current metadata.
app.get("/api/rooms/recent", (req, res) => {
  const ids = (req.query.ids || "").split(",").filter(Boolean).slice(0, 5);

  const result = ids.map((id) => {
    const room = rooms.get(id);

    if (!room) {
      return {
        id,
        ended: true,
        missing: true,
      };
    }

    return {
      id: room.id,
      name: room.name,
      createdAt: room.createdAt,
      ended: room.ended || false,
      memberCount: (room.memberList || []).length,
      members: getRoomPublicData(room).members,
      pastMembers: room.pastMembers || [],
      fileCount: room.files.filter((file) => !file.deleted).length,
    };
  });

  res.json(result);
});

// ─── REST: Get or create personal room ("My Room") ─────────
// Returns the user's personal room, creating it if it doesn't exist.
app.post("/api/rooms/my-room", (req, res) => {
  const { userId, userName, userEmail } = req.body;
  if (!userId) return res.status(400).json({ error: "User ID required" });

  const personalRoomId = getPersonalRoomId(userId);
  let room = rooms.get(personalRoomId);

  // Create personal room if it doesn't exist
  if (!room) {
    room = {
      id: personalRoomId,
      name: `${userName.split(" ")[0]}'s Room`,
      creatorId: userId,
      createdAt: new Date().toISOString(),
      ended: false,

      // Active socket connections
      members: new Map(),

      // Persistent room members
      memberList: [
        {
          id: userId,
          name: userName,
          email: userEmail || "",
          invitationStatus: "accepted",
        },
      ],

      files: [],

      pastMembers: [
        {
          id: userId,
          name: userName,
          email: userEmail || "",
        },
      ],
    };
    rooms.set(personalRoomId, room);
    saveRooms();
    fs.mkdirSync(path.join(FILES_DIR, personalRoomId), { recursive: true });
    console.log(`Personal room created: ${personalRoomId} for ${userName}`);
  }

  // Reset ended state if it was previously ended
  if (room.ended) {
    room.ended = false;
    saveRooms();
  }

  res.json({ success: true, room: getRoomPublicData(room) });
});

// ============================================================================
// Server file mein REPLACE karo:
//   `const UPLOAD_TMP_DIR = ...` se le kar
//   `// 4) cancel` wale app.delete(...) route ke end tak
// Baaqi file (REST /files route, sockets, etc.) waise hi rahegi.
// Agar MAX_FILE_BYTES pehle se alag define kiya hai to us line ko hata dena
// (do baar const declare hone par SyntaxError aayega).
// ============================================================================

const UPLOAD_TMP_DIR = path.join(DATA_DIR, "uploads-tmp");
fs.mkdirSync(UPLOAD_TMP_DIR, { recursive: true });

const DEFAULT_CHUNK_BYTES = 2 * 1024 * 1024;
const MAX_CHUNK_BYTES = 8 * 1024 * 1024;
const SESSION_TTL_MS = 24 * 60 * 60 * 1000;
const COMPLETED_KEEP_MS = 10 * 60 * 1000;
const ID_RE = /^[\w-]{8,64}$/; // uploadId file path mein use hota hai, validate zaroori hai

const uploadSessions = new Map();
const partPath = (id) => path.join(UPLOAD_TMP_DIR, `${id}.part`);
const metaPath = (id) => path.join(UPLOAD_TMP_DIR, `${id}.json`);

function persistSession(s) {
  // active (chunk requests), finalizing, fileEntry disk par save nahi hote
  const { active, finalizing, fileEntry, ...data } = s;
  fs.promises
    .writeFile(metaPath(s.uploadId), JSON.stringify(data))
    .catch((e) => console.error("persistSession:", e.message));
}

function sessionState(s) {
  return {
    uploadId: s.uploadId,
    fileSize: s.fileSize,
    chunkSize: s.chunkSize,
    totalChunks: s.totalChunks,
    received: [...s.received], // jo chunk indexes server pe aa chuke hain
    complete: !!s.fileEntry,
    file: s.fileEntry || undefined,
  };
}

function discardUpload(uploadId) {
  const s = uploadSessions.get(uploadId);
  if (!s) return;
  s.active?.forEach((r) => r.destroy());
  uploadSessions.delete(uploadId);
  fs.promises.unlink(partPath(uploadId)).catch(() => {});
  fs.promises.unlink(metaPath(uploadId)).catch(() => {});
}

function discardRoomUploads(roomId) {
  for (const s of [...uploadSessions.values()]) {
    if (s.roomId === roomId) discardUpload(s.uploadId);
  }
}

// Server restart / deploy ke baad bhi resume ho sake
(function loadUploadSessions() {
  const metas = new Set();
  for (const f of fs.readdirSync(UPLOAD_TMP_DIR)) {
    if (!f.endsWith(".json")) continue;
    try {
      const s = JSON.parse(
        fs.readFileSync(path.join(UPLOAD_TMP_DIR, f), "utf8"),
      );
      if (!fs.existsSync(partPath(s.uploadId))) {
        fs.unlinkSync(path.join(UPLOAD_TMP_DIR, f));
        continue;
      }

      // Purane (sequential offset wale) sessions ko naye format mein badlo
      if (!Array.isArray(s.received)) {
        s.chunkSize = s.chunkSize || 2 * 1024 * 1024;
        s.totalChunks = Math.ceil(s.fileSize / s.chunkSize);
        const doneCount = Math.min(
          Math.floor((s.offset || 0) / s.chunkSize),
          s.totalChunks,
        );
        s.received = Array.from({ length: doneCount }, (_, i) => i);
      }

      s.active = new Map();
      s.finalizing = false;
      uploadSessions.set(s.uploadId, s);
      metas.add(s.uploadId);
    } catch {
      /* corrupt meta ignore */
    }
  }
  for (const f of fs.readdirSync(UPLOAD_TMP_DIR)) {
    // yateem .part files
    if (f.endsWith(".part") && !metas.has(f.slice(0, -5))) {
      fs.unlink(path.join(UPLOAD_TMP_DIR, f), () => {});
    }
  }
  console.log(`Restored ${uploadSessions.size} resumable upload session(s)`);
})();

setInterval(
  () => {
    const now = Date.now();
    for (const s of [...uploadSessions.values()]) {
      if (!s.fileEntry && now - s.updatedAt > SESSION_TTL_MS)
        discardUpload(s.uploadId);
    }
  },
  60 * 60 * 1000,
).unref();

// Windows-style unique naming: file.pdf -> file (1).pdf -> file (2).pdf
// Sirf usi uploader ki files se compare hota hai, doosre users se nahi.
function getUniqueFileName(room, uploaderId, originalName) {
  const taken = new Set(
    (room.files || [])
      .filter((f) => !f.deleted && String(f.uploaderId) === String(uploaderId))
      .map((f) => f.fileName.toLowerCase()),
  );

  if (!taken.has(originalName.toLowerCase())) return originalName;

  const ext = path.extname(originalName);
  const base = path.basename(originalName, ext);

  let n = 1;
  let candidate;
  do {
    candidate = `${base} (${n})${ext}`;
    n++;
  } while (taken.has(candidate.toLowerCase()));

  return candidate;
}

async function finalizeUpload(s, room) {
  if (room.ended) throw new Error("Room ended");
  const stat = await fs.promises.stat(partPath(s.uploadId));
  if (stat.size !== s.fileSize) {
    throw new Error(`Size mismatch: expected ${s.fileSize}, got ${stat.size}`);
  }
  const storedName = `${uuidv4()}${path.extname(s.fileName)}`;
  const roomDir = path.join(FILES_DIR, room.id);
  await fs.promises.mkdir(roomDir, { recursive: true });
  await fs.promises.rename(
    partPath(s.uploadId),
    path.join(roomDir, storedName),
  );

  const fileEntry = {
    id: path.basename(storedName, path.extname(storedName)),
    fileName: getUniqueFileName(room, s.uploaderId, s.fileName),
    fileType: s.fileType,
    fileSize: s.fileSize,
    storedName,
    uploadedBy: s.uploaderName,
    uploaderId: s.uploaderId,
    uploadedAt: new Date().toISOString(),
  };
  room.files.push(fileEntry);
  saveRooms();

  s.fileEntry = fileEntry; // last response kho jaye to client status se file le sakta hai
  fs.promises.unlink(metaPath(s.uploadId)).catch(() => {});
  setTimeout(
    () => uploadSessions.delete(s.uploadId),
    COMPLETED_KEEP_MS,
  ).unref();

  io.to(room.id).emit("file_shared", {
    file: fileEntry,
    room: getRoomPublicData(room),
  });
  console.log(
    `File saved (resumable): ${fileEntry.fileName} in room ${room.id}`,
  );
  return fileEntry;
}

// Jab saare chunks aa jayen to ek hi baar finalize chale
async function tryFinalize(s, room) {
  if (s.fileEntry || s.finalizing) return;
  if (s.received.length !== s.totalChunks) return;
  s.finalizing = true;
  try {
    await finalizeUpload(s, room);
  } finally {
    s.finalizing = false;
  }
}

// 1) init (idempotent: same uploadId dobara aaye to current state milti hai)
app.post("/api/rooms/:roomId/uploads", async (req, res) => {
  const { roomId } = req.params;
  const room = rooms.get(roomId);
  if (!room || room.ended)
    return res.status(404).json({ error: "Room not found" });

  const {
    uploadId: requestedId,
    fileName,
    fileType,
    fileSize,
    uploaderId,
    uploaderName,
  } = req.body || {};
  const size = Number(fileSize);
  if (!fileName || !Number.isFinite(size) || size <= 0) {
    return res.status(400).json({ error: "fileName and fileSize required" });
  }
  if (size > MAX_FILE_BYTES) {
    return res.status(413).json({
      error: `File too large (max ${Math.floor(MAX_FILE_BYTES / 1048576)} MB)`,
    });
  }
  if (requestedId !== undefined && !ID_RE.test(String(requestedId))) {
    return res.status(400).json({ error: "Invalid uploadId" });
  }

  touchRoom(roomId);

  const existing = requestedId && uploadSessions.get(requestedId);
  if (
    existing &&
    existing.roomId === roomId &&
    existing.fileName === fileName &&
    existing.fileSize === size
  ) {
    // Saare chunks aa chuke thay lekin finalize fail hua tha? Dobara try karo.
    try {
      await tryFinalize(existing, room);
    } catch (e) {
      console.error(
        `[upload ${existing.uploadId}] finalize failed:`,
        e.message,
      );
      return res.status(500).json({ error: "Failed to finalize upload" });
    }
    return res.json(sessionState(existing));
  }

  const uploadId = requestedId && !existing ? requestedId : uuidv4();
  const chunkSize = DEFAULT_CHUNK_BYTES;
  const s = {
    uploadId,
    roomId,
    fileName,
    fileSize: size,
    fileType: fileType || "application/octet-stream",
    uploaderId: uploaderId || "unknown",
    uploaderName: uploaderName || "Unknown",
    chunkSize,
    totalChunks: Math.ceil(size / chunkSize),
    received: [],
    createdAt: Date.now(),
    updatedAt: Date.now(),
    active: new Map(), // chunkIndex -> chal rahi request
    finalizing: false,
  };

  // Poori file ki jagah pehle se bana lo, taake chunks kisi bhi order mein
  // apni sahi position (offset) par likhe ja saken. Alag "merge" step nahi chahiye.
  fs.closeSync(fs.openSync(partPath(uploadId), "w"));
  fs.truncateSync(partPath(uploadId), size);

  uploadSessions.set(uploadId, s);
  persistSession(s);
  res.status(201).json(sessionState(s));
});

// 2) status: client reconnect ke baad yahin se poochta hai kaun se chunks aa chuke hain
app.get("/api/rooms/:roomId/uploads/:uploadId", (req, res) => {
  const s = uploadSessions.get(req.params.uploadId);
  if (!s || s.roomId !== req.params.roomId) {
    return res.status(404).json({ error: "Upload session not found" });
  }
  touchRoom(s.roomId);
  res.json(sessionState(s));
});

// 3) chunk (kai chunks ek saath, kisi bhi order mein aa sakte hain)
app.put("/api/rooms/:roomId/uploads/:uploadId", async (req, res) => {
  const { roomId, uploadId } = req.params;
  const room = rooms.get(roomId);
  const s = uploadSessions.get(uploadId);
  if (!room || room.ended || !s || s.roomId !== roomId) {
    req.resume();
    return res.status(404).json({ error: "Upload session not found" });
  }
  if (s.fileEntry) {
    req.resume();
    return res.json(sessionState(s));
  } // already complete

  const start = Number(req.headers["x-upload-offset"]);
  const len = Number(req.headers["content-length"]);
  if (
    !Number.isInteger(start) ||
    start < 0 ||
    !Number.isInteger(len) ||
    len <= 0
  ) {
    req.resume();
    return res
      .status(400)
      .json({ error: "X-Upload-Offset and Content-Length required" });
  }
  if (len > MAX_CHUNK_BYTES) {
    req.resume();
    return res.status(413).json({ error: "Chunk too large" });
  }
  if (start % s.chunkSize !== 0 || start >= s.fileSize) {
    req.resume();
    return res.status(400).json({ error: "Invalid chunk offset" });
  }

  const index = start / s.chunkSize;
  const expectedLen = Math.min(s.chunkSize, s.fileSize - start);
  if (len !== expectedLen) {
    req.resume();
    return res
      .status(400)
      .json({ error: `Invalid chunk size, expected ${expectedLen}` });
  }

  // Ye chunk pehle aa chuka hai (retry / duplicate): dobara likhne ki zaroorat nahi
  if (s.received.includes(index)) {
    req.resume();
    try {
      await tryFinalize(s, room);
    } catch (e) {
      console.error(`[upload ${uploadId}] finalize failed:`, e.message);
      return res.status(500).json({ error: "Failed to finalize upload" });
    }
    return res.json(sessionState(s));
  }

  // Isi chunk ki purani adhoori request (network drop ke baad) ho to khatam karo.
  // Dono same bytes same position pe likhte hain, is liye overlap se data kharab nahi hota.
  const prev = s.active.get(index);
  if (prev && !prev.destroyed) prev.destroy();
  s.active.set(index, req);

  // Har chunk file mein apni offset par likha jata hai (r+ flag, start = offset)
  const ws = fs.createWriteStream(partPath(uploadId), { flags: "r+", start });
  let written = 0;
  req.on("data", (c) => {
    written += c.length;
  });

  try {
    await pipeline(req, ws);
  } catch (err) {
    // Chunk received mein add nahi hua. Client wahi chunk dobara bhejega.
    console.warn(
      `[upload ${uploadId}] chunk #${index} interrupted at ${written}/${len} (${err.code || err.message})`,
    );
    if (s.active.get(index) === req) s.active.delete(index);
    return;
  }
  if (s.active.get(index) !== req) return; // koi nayi request isay replace kar chuki hai
  s.active.delete(index);

  if (written !== len) {
    return res
      .status(400)
      .json({ error: "Chunk size mismatch", ...sessionState(s) });
  }

  if (!s.received.includes(index)) s.received.push(index);
  s.updatedAt = Date.now();
  touchRoom(roomId);

  console.log(
    `[${s.fileName}] [Room: ${room.name} (${roomId})] chunk ${s.received.length}/${s.totalChunks} ` +
      `(${(Math.min(s.received.length * s.chunkSize, s.fileSize) / 1048576) | 0} / ${(s.fileSize / 1048576).toFixed(2)} MB)`,
  );

  try {
    await tryFinalize(s, room);
  } catch (e) {
    console.error(`[upload ${uploadId}] finalize failed:`, e.message);
    return res.status(500).json({ error: "Failed to finalize upload" });
  }

  if (!s.fileEntry) persistSession(s);
  res.json(sessionState(s));
});

// 4) cancel
app.delete("/api/rooms/:roomId/uploads/:uploadId", (req, res) => {
  const s = uploadSessions.get(req.params.uploadId);
  if (s && s.roomId === req.params.roomId) discardUpload(s.uploadId);
  res.json({ success: true });
});

// ─── REST: Upload file ────────────────────────────────────
app.post(
  "/api/rooms/:roomId/files",
  trackUploadProgress,
  uploadSingle,
  (req, res) => {
    const room = rooms.get(req.params.roomId);
    if (!room || room.ended)
      return res.status(404).json({ error: "Room not found" });
    if (!req.file) return res.status(400).json({ error: "No file provided" });

    touchRoom(req.params.roomId);

    const { uploaderId, uploaderName } = req.body;
    const fileEntry = {
      id: path.basename(req.file.filename, path.extname(req.file.filename)),
      fileName: req.file.originalname,
      fileType: req.file.mimetype,
      fileSize: req.file.size,
      storedName: req.file.filename,
      uploadedBy: uploaderName || "Unknown",
      uploaderId: uploaderId || "unknown",
      uploadedAt: new Date().toISOString(),
    };
    room.files.push(fileEntry);
    saveRooms();
    io.to(req.params.roomId).emit("file_shared", {
      file: fileEntry,
      room: getRoomPublicData(room),
    });
    console.log(
      `File saved: ${req.file.originalname} in room ${req.params.roomId}`,
    );
    res.json({ success: true, file: fileEntry });
  },
);

// ─── REST: Download file ──────────────────────────────────
app.get("/api/rooms/:roomId/files/:fileId/download", (req, res) => {
  const room = rooms.get(req.params.roomId);

  touchRoom(req.params.roomId);

  if (!room) return res.status(404).json({ error: "Room not found" });
  const fileEntry = room.files.find((f) => f.id === req.params.fileId);
  if (!fileEntry) return res.status(404).json({ error: "File not found" });
  const filePath = path.join(
    FILES_DIR,
    req.params.roomId,
    fileEntry.storedName,
  );
  if (!fs.existsSync(filePath))
    return res.status(404).json({ error: "File missing from disk" });
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${encodeURIComponent(fileEntry.fileName)}"`,
  );
  res.setHeader(
    "Content-Type",
    fileEntry.fileType || "application/octet-stream",
  );
  fs.createReadStream(filePath).pipe(res);
});

app.get("/api/rooms/:roomId/files/:fileId/preview", (req, res) => {
  const room = rooms.get(req.params.roomId);

  touchRoom(req.params.roomId);

  if (!room) {
    return res.status(404).json({
      error: "Room not found",
    });
  }

  const fileEntry = room.files.find((f) => f.id === req.params.fileId);

  if (!fileEntry) {
    return res.status(404).json({
      error: "File not found",
    });
  }

  if (fileEntry.deleted) {
    return res.status(404).json({
      error: "File has been deleted",
    });
  }

  const filePath = path.join(
    FILES_DIR,
    req.params.roomId,
    fileEntry.storedName,
  );

  if (!fs.existsSync(filePath)) {
    return res.status(404).json({
      error: "File missing from disk",
    });
  }

  res.setHeader(
    "Content-Type",
    fileEntry.fileType || "application/octet-stream",
  );

  // IMPORTANT:
  // Do NOT use "attachment" here.
  res.setHeader("Content-Disposition", "inline");

  fs.createReadStream(filePath).pipe(res);
});

// ─── REST: Delete file ────────────────────────────────────
// app.delete("/api/rooms/:roomId/files/:fileId", (req, res) => {
//   const room = rooms.get(req.params.roomId);
//   if (!room) return res.status(404).json({ error: "Room not found" });

//   const fileIdx = room.files.findIndex((f) => f.id === req.params.fileId);
//   if (fileIdx === -1) return res.status(404).json({ error: "File not found" });

//   const fileEntry = room.files[fileIdx];
//   const { requesterId } = req.body;

//   // Only uploader or room creator can delete
//   if (requesterId !== fileEntry.uploaderId && requesterId !== room.creatorId) {
//     return res.status(403).json({ error: "You can only delete your own files." });
//   }

//   // Delete from disk
//   const filePath = path.join(FILES_DIR, req.params.roomId, fileEntry.storedName);
//   try { if (fs.existsSync(filePath)) fs.unlinkSync(filePath); } catch (e) { console.error("Disk delete error:", e.message); }

//   room.files.splice(fileIdx, 1);
//   saveRooms();

//   io.to(req.params.roomId).emit("file_deleted", { fileId: req.params.fileId, room: getRoomPublicData(room) });
//   console.log(`File deleted: ${fileEntry.fileName} from room ${req.params.roomId}`);
//   res.json({ success: true });
// });

app.delete("/api/rooms/:roomId/files/:fileId", (req, res) => {
  const room = rooms.get(req.params.roomId);

  touchRoom(req.params.roomId);

  if (!room) {
    return res.status(404).json({ error: "Room not found" });
  }

  const fileIdx = room.files.findIndex((f) => f.id === req.params.fileId);

  if (fileIdx === -1) {
    return res.status(404).json({ error: "File not found" });
  }

  const fileEntry = room.files[fileIdx];
  const { requesterId } = req.body;

  // Only uploader or room creator can delete
  if (requesterId !== fileEntry.uploaderId && requesterId !== room.creatorId) {
    return res.status(403).json({
      error: "You can only delete your own files.",
    });
  }

  // --------------------------------------------------
  // SOFT DELETE
  // --------------------------------------------------

  fileEntry.deleted = true;
  fileEntry.deletedAt = new Date().toISOString();
  fileEntry.deletedBy = requesterId;

  // IMPORTANT:
  // Do NOT delete the physical file from disk here.
  //
  // fs.unlinkSync(...) should NOT be called here.

  saveRooms();

  io.to(req.params.roomId).emit("file_deleted", {
    fileId: req.params.fileId,
    room: getRoomPublicData(room),
  });

  console.log(
    `File soft-deleted: ${fileEntry.fileName} from room ${req.params.roomId}`,
  );

  res.json({
    success: true,
    message: "File moved to recycle bin.",
  });
});

app.post("/api/rooms/:roomId/files/:fileId/restore", (req, res) => {
  const room = rooms.get(req.params.roomId);

  touchRoom(req.params.roomId);

  if (!room) {
    return res.status(404).json({ error: "Room not found" });
  }

  const fileEntry = room.files.find((f) => f.id === req.params.fileId);

  if (!fileEntry) {
    return res.status(404).json({ error: "File not found" });
  }

  if (!fileEntry.deleted) {
    return res.status(400).json({
      error: "File is not in the recycle bin.",
    });
  }

  const { requesterId } = req.body;

  // Same permission rule
  if (requesterId !== fileEntry.uploaderId && requesterId !== room.creatorId) {
    return res.status(403).json({
      error: "You can only restore your own files.",
    });
  }

  // Make it active again
  fileEntry.deleted = false;
  delete fileEntry.deletedAt;
  delete fileEntry.deletedBy;

  saveRooms();

  io.to(req.params.roomId).emit("file_restored", {
    fileId: fileEntry.id,
    room: getRoomPublicData(room),
  });

  console.log(
    `File restored: ${fileEntry.fileName} in room ${req.params.roomId}`,
  );

  res.json({
    success: true,
    message: "File restored successfully.",
  });
});

app.delete("/api/rooms/:roomId/files/:fileId/permanent", (req, res) => {
  const room = rooms.get(req.params.roomId);

  touchRoom(req.params.roomId);

  if (!room) {
    return res.status(404).json({ error: "Room not found" });
  }

  const fileIdx = room.files.findIndex((f) => f.id === req.params.fileId);

  if (fileIdx === -1) {
    return res.status(404).json({ error: "File not found" });
  }

  const fileEntry = room.files[fileIdx];
  const { requesterId } = req.body;

  // Only uploader or room creator can permanently delete
  if (requesterId !== fileEntry.uploaderId && requesterId !== room.creatorId) {
    return res.status(403).json({
      error: "You can only permanently delete your own files.",
    });
  }

  // Only allow permanent deletion of soft-deleted files
  if (!fileEntry.deleted) {
    return res.status(400).json({
      error: "File must be in the recycle bin first.",
    });
  }

  const filePath = path.join(
    FILES_DIR,
    req.params.roomId,
    fileEntry.storedName,
  );

  // Delete physical file
  try {
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
  } catch (e) {
    console.error("Permanent disk delete error:", e.message);

    return res.status(500).json({
      error: "Failed to permanently delete file.",
    });
  }

  // Remove metadata completely
  room.files.splice(fileIdx, 1);

  saveRooms();

  io.to(req.params.roomId).emit("file_permanently_deleted", {
    fileId: req.params.fileId,
    room: getRoomPublicData(room),
  });

  console.log(
    `File permanently deleted: ${fileEntry.fileName} from room ${req.params.roomId}`,
  );

  res.json({
    success: true,
    message: "File permanently deleted.",
  });
});

// ─── REST: Get room ───────────────────────────────────────
app.get("/api/rooms/:roomId", (req, res) => {
  const room = rooms.get(req.params.roomId);
  if (!room) return res.status(404).json({ error: "Room not found" });
  res.json(getRoomPublicData(room));
});

// ─── REST: List rooms ─────────────────────────────────────
app.get("/api/rooms", (req, res) => {
  const list = Array.from(rooms.values())
    .filter((r) => !r.ended)
    .map((r) => {
      // Count unique users instead of socket connections
      // const uniqueUserIds = new Set();
      // for (const member of r.members.values()) {
      //   uniqueUserIds.add(member.id);
      // }
      const memberList = r.memberList || [];
      // return { id: r.id, name: r.name, memberCount: uniqueUserIds.size, fileCount: r.files.length, createdAt: r.createdAt };
      return {
        id: r.id,
        name: r.name,
        memberCount: memberList.length,
        fileCount: r.files.filter((file) => !file.deleted).length,
        createdAt: r.createdAt,
      };
    });
  res.json(list);
});

// ─── Socket Events ────────────────────────────────────────
io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on(
    "create_room",
    (
      {
        roomName,
        userName,
        userId: providedUserId,
        userEmail,
        invitedMembers = [],
      },
      callback,
    ) => {
      const roomId = uuidv4().slice(0, 8).toUpperCase();
      const userId = providedUserId || uuidv4();

      touchRoom(roomId);
      // Creator is automatically accepted.
      const memberList = [
        {
          id: userId,
          name: userName,
          email: userEmail || "",
          invitationStatus: "accepted",
        },
      ];

      // Add invited members as pending.
      for (const invited of invitedMembers) {
        const invitedId = invited.id || invited.userId;

        if (!invitedId) continue;

        // Don't add the creator again.
        if (invitedId === userId) continue;

        // Don't add duplicates.
        if (memberList.some((member) => member.id === invitedId)) {
          continue;
        }

        memberList.push({
          id: invitedId,
          name: invited.name || invited.displayName || "Unknown",
          email:
            invited.email || invited.mail || invited.userPrincipalName || "",
          invitationStatus: "pending",
        });
      }

      const room = {
        id: roomId,
        name: roomName?.trim() || `Room ${roomId}`,
        creatorId: userId,
        createdAt: new Date().toISOString(),
        ended: false,

        // Active socket connections
        members: new Map(),

        // Persistent members/invitations
        memberList,

        files: [],

        messages: [],

        pastMembers: [
          {
            id: userId,
            name: userName,
            email: userEmail || "",
          },
        ],
        // Users kicked from this room
        kickedMemberIds: [],
      };

      // Creator is immediately online.
      room.members.set(socket.id, {
        id: userId,
        name: userName,
        email: userEmail || "",
        socketId: socket.id,
      });

      rooms.set(roomId, room);

      saveRooms();

      fs.mkdirSync(path.join(FILES_DIR, roomId), {
        recursive: true,
      });

      socket.join(roomId);

      socket.data = {
        roomId,
        userId,
        userName,
      };

      console.log(`Room created: ${roomId} by ${userName}`);

      callback({
        success: true,
        room: getRoomPublicData(room),
        userId,
      });
    },
  );

  socket.on(
    "join_room",
    ({ roomId, userName, userId: providedUserId, userEmail }, callback) => {
      const room = rooms.get(roomId);

      touchRoom(roomId);

      if (!room) {
        return callback({
          success: false,
          error: "Room not found.",
        });
      }

      if (room.ended) {
        return callback({
          success: false,
          error: "This room session has ended.",
        });
      }

      const userId = providedUserId || uuidv4();

      if (room.kickedMemberIds?.some((id) => String(id) === String(userId))) {
        console.log(
          `Blocked kicked user ${userId} from joining room ${roomId}`,
        );

        return callback({
          success: false,
          kicked: true,
          error: "You have been removed from this room.",
        });
      }

      if (!room.memberList) {
        room.memberList = [];
      }

      // Check whether this user already belongs to the room.
      let memberProfile = room.memberList.find(
        (member) => member.id === userId,
      );

      if (memberProfile) {
        // User was already invited/member.
        memberProfile.invitationStatus = "accepted";

        // Update information in case it changed.
        memberProfile.name = userName || memberProfile.name;
        memberProfile.email = userEmail || memberProfile.email || "";
      } else {
        // Existing behavior allowed anyone with the room ID to join.
        // Keep that behavior by adding them as an accepted member.
        memberProfile = {
          id: userId,
          name: userName,
          email: userEmail || "",
          invitationStatus: "accepted",
        };

        room.memberList.push(memberProfile);
      }

      // Add current socket.
      room.members.set(socket.id, {
        id: userId,
        name: userName,
        email: userEmail || "",
        socketId: socket.id,
      });

      // Track past members for re-invite.
      if (!room.pastMembers) {
        room.pastMembers = [];
      }

      if (!room.pastMembers.find((m) => m.id === userId)) {
        room.pastMembers.push({
          id: userId,
          name: userName,
          email: userEmail || "",
        });
      }

      saveRooms();

      socket.join(roomId);

      socket.data = {
        roomId,
        userId,
        userName,
      };

      // Send updated member information to everyone.
      io.to(roomId).emit("room_members_updated", {
        members: getRoomPublicData(room).members,
      });

      socket.to(roomId).emit("member_joined", {
        member: {
          id: userId,
          name: userName,
        },
        room: getRoomPublicData(room),
      });

      console.log(`${userName} joined room ${roomId}`);

      callback({
        success: true,
        room: getRoomPublicData(room),
        userId,
      });
    },
  );

  socket.on(
    "rejoin_room",
    ({ roomId, userId, userName, userEmail }, callback) => {
      const room = rooms.get(roomId);

      touchRoom(roomId);

      if (!room) {
        return callback({
          success: false,
          error: "Room not found.",
        });
      }

      if (room.ended) {
        return callback({
          success: false,
          error: "This room session has ended.",
        });
      }

      if (room.kickedMemberIds?.some((id) => String(id) === String(userId))) {
        console.log(
          `Blocked kicked user ${userId} from rejoining room ${roomId}`,
        );

        return callback({
          success: false,
          kicked: true,
          error: "You have been removed from this room.",
        });
      }

      if (!room.memberList) {
        room.memberList = [];
      }

      let memberProfile = room.memberList.find(
        (member) => member.id === userId,
      );

      if (memberProfile) {
        memberProfile.invitationStatus = "accepted";
        memberProfile.name = userName || memberProfile.name;
        memberProfile.email = userEmail || memberProfile.email || "";
      } else {
        memberProfile = {
          id: userId,
          name: userName,
          email: userEmail || "",
          invitationStatus: "accepted",
        };

        room.memberList.push(memberProfile);
      }

      room.members.set(socket.id, {
        id: userId,
        name: userName,
        email: userEmail || "",
        socketId: socket.id,
      });

      if (!room.pastMembers) {
        room.pastMembers = [];
      }

      if (!room.pastMembers.find((m) => m.id === userId)) {
        room.pastMembers.push({
          id: userId,
          name: userName,
          email: userEmail || "",
        });
      }

      saveRooms();

      socket.join(roomId);

      socket.data = {
        roomId,
        userId,
        userName,
      };

      io.to(roomId).emit("room_members_updated", {
        members: getRoomPublicData(room).members,
      });

      socket.to(roomId).emit("member_joined", {
        member: {
          id: userId,
          name: userName,
        },
        room: getRoomPublicData(room),
      });

      console.log(`${userName} rejoined room ${roomId}`);

      callback({
        success: true,
        room: getRoomPublicData(room),
        userId,
      });
    },
  );

  socket.on("add_room_invited_member", ({ roomId, member }, callback) => {
    try {
      const room = rooms.get(roomId);

      touchRoom(roomId);

      if (!room) {
        callback?.({
          success: false,
          error: "Room not found",
        });
        return;
      }

      if (!room.memberList) {
        room.memberList = [];
      }

      const existing = room.memberList.find(
        (m) => String(m.id) === String(member.id),
      );

      if (existing) {
        existing.name = member.name || existing.name;
        existing.email = member.email || existing.email || "";
        existing.invitationStatus = "pending";
      } else {
        room.memberList.push({
          id: member.id,
          name: member.name || "",
          email: member.email || "",
          invitationStatus: "pending",
        });
      }

      saveRooms();

      // IMPORTANT:
      // Get the members through getRoomPublicData()
      // so every member gets the computed `online` property.
      const publicRoom = getRoomPublicData(room);

      io.to(roomId).emit("room_members_updated", {
        roomId,
        members: publicRoom.members,
      });

      callback?.({
        success: true,
        member: publicRoom.members.find(
          (m) => String(m.id) === String(member.id),
        ),
      });
    } catch (error) {
      console.error("add_room_invited_member error:", error);

      callback?.({
        success: false,
        error: error.message,
      });
    }
  });

  socket.on("kick_member", ({ roomId, memberId }, callback) => {
    try {
      const room = rooms.get(roomId);

      touchRoom(roomId);

      if (!room) {
        return callback({
          success: false,
          error: "Room not found",
        });
      }

      // --------------------------------------------------
      // Only room creator can kick members
      // --------------------------------------------------

      if (room.creatorId !== socket.data?.userId) {
        return callback({
          success: false,
          error: "Only the room creator can remove members.",
        });
      }

      // --------------------------------------------------
      // Creator cannot kick themselves
      // --------------------------------------------------

      if (String(memberId) === String(room.creatorId)) {
        return callback({
          success: false,
          error: "The room creator cannot be removed.",
        });
      }

      // --------------------------------------------------
      // Find member in persistent member list
      // --------------------------------------------------

      if (!room.memberList) {
        room.memberList = [];
      }

      const memberIndex = room.memberList.findIndex(
        (member) => String(member.id) === String(memberId),
      );

      if (memberIndex === -1) {
        return callback({
          success: false,
          error: "Member is not in this room.",
        });
      }

      const removedMember = room.memberList[memberIndex];

      // --------------------------------------------------
      // IMPORTANT:
      // Find the ACTUAL active socket from room.members.
      //
      // memberList doesn't contain socketId.
      // --------------------------------------------------

      let targetSocket = null;

      for (const [socketId, activeMember] of room.members.entries()) {
        if (String(activeMember.id) === String(memberId)) {
          targetSocket = io.sockets.sockets.get(socketId);

          break;
        }
      }

      // --------------------------------------------------
      // Add user to kicked list
      // --------------------------------------------------

      if (!room.kickedMemberIds) {
        room.kickedMemberIds = [];
      }

      if (!room.kickedMemberIds.some((id) => String(id) === String(memberId))) {
        room.kickedMemberIds.push(memberId);
      }

      // --------------------------------------------------
      // Remove from persistent member list
      // --------------------------------------------------

      room.memberList.splice(memberIndex, 1);

      // --------------------------------------------------
      // Remove from active socket members
      // --------------------------------------------------

      for (const [socketId, activeMember] of room.members.entries()) {
        if (String(activeMember.id) === String(memberId)) {
          room.members.delete(socketId);
          break;
        }
      }

      // --------------------------------------------------
      // Remove kicked user's socket from Socket.IO room
      // --------------------------------------------------

      if (targetSocket) {
        targetSocket.emit("kicked_from_room", {
          roomId,
          roomName: room.name,
          message: "You have been removed from this room.",
        });

        // Remove socket from the Socket.IO room
        targetSocket.leave(roomId);

        // Remove it from our active members
        room.members.delete(targetSocket.id);

        // Clear its FileHive session data
        targetSocket.data = {};
      }

      saveRooms();

      // --------------------------------------------------
      // Send updated member list to everyone remaining
      // --------------------------------------------------

      const publicRoom = getRoomPublicData(room);

      io.to(roomId).emit("room_members_updated", {
        members: publicRoom.members,
      });

      io.to(roomId).emit("member_kicked", {
        roomId,
        memberId,
        member: removedMember,
        members: publicRoom.members,
      });

      callback({
        success: true,
        memberId,
      });
    } catch (error) {
      console.error("kick_member error:", error);

      callback({
        success: false,
        error: "Failed to remove member.",
      });
    }
  });

  socket.on("typing", ({ roomId, userId, userName }) => {
    socket.to(roomId).emit("user_typing", {
      userId,
      userName,
    });
  });

  socket.on("stop_typing", ({ roomId, userId }) => {
    socket.to(roomId).emit("user_stop_typing", {
      userId,
    });
  });

  socket.on("send_chat_message", ({ message }, callback) => {
    const roomId = socket.data?.roomId;
    const userId = socket.data?.userId;
    const userName = socket.data?.userName;

    touchRoom(roomId);

    if (!roomId || !userId) {
      return callback?.({
        success: false,
        message: "You are not currently in a room.",
      });
    }

    const room = rooms.get(roomId);

    if (!room) {
      return callback?.({
        success: false,
        message: "Room not found.",
      });
    }

    if (room.ended) {
      return callback?.({
        success: false,
        message: "This room has ended.",
      });
    }

    if (!room.members.has(socket.id)) {
      return callback?.({
        success: false,
        message: "You are not an active member of this room.",
      });
    }

    const text = typeof message === "string" ? message.trim() : "";

    if (!text) {
      return callback?.({
        success: false,
        message: "Message cannot be empty.",
      });
    }

    if (text.length > 2000) {
      return callback?.({
        success: false,
        message: "Message is too long.",
      });
    }

    const chatMessage = {
      id: uuidv4(),
      senderId: userId,
      senderName: userName || "Unknown User",
      message: text,
      sentAt: new Date().toISOString(),
      roomId,
    };

    if (!Array.isArray(room.messages)) {
      room.messages = [];
    }

    room.messages.push(chatMessage);

    saveRooms();

    io.to(roomId).emit("chat_message", chatMessage);

    callback?.({
      success: true,
      message: chatMessage,
    });
  });

  socket.on("chat_mark_seen", ({ roomId }) => {
    const room = rooms.get(roomId);

    if (!room) return;

    const userId = socket.data?.userId;

    if (!userId) return;

    if (!room.chatLastSeen) {
      room.chatLastSeen = {};
    }

    room.chatLastSeen[userId] = new Date().toISOString();

    saveRooms();
  });

  socket.on("leave_room", (callback) => {
    handleLeave(socket);
    callback?.({ success: true });
  });

  socket.on("end_room", (callback) => {
    const roomId = socket.data?.roomId;
    const room = rooms.get(roomId);
    if (!room || room.creatorId !== socket.data?.userId) {
      return callback?.({
        success: false,
        error: "Only the creator can end the room.",
      });
    }
    room.ended = true;
    room.messages = [];
    saveRooms();
    // Delete all files from disk when session ends
    deleteRoomFiles(roomId);
    io.to(roomId).emit("room_ended", {
      message: "The room creator has ended the session.",
    });
    const socketsInRoom = io.sockets.adapter.rooms.get(roomId);
    if (socketsInRoom) {
      socketsInRoom.forEach((sid) => {
        const s = io.sockets.sockets.get(sid);
        if (s) {
          s.data = {};
          s.leave(roomId);
        }
      });
    }
    console.log(`Room ${roomId} ended — files deleted`);
    callback?.({ success: true });
  });

  socket.on("file_uploading", ({ roomId, uploaderId }) => {
    touchRoom(roomId);
    socket.to(roomId).emit("file_uploading", {
      roomId,
      uploaderId,
    });
  });

  socket.on("file_downloaded", (data) => {
    touchRoom(data.roomId);
    socket.to(data.roomId).emit("file_downloaded", data);
  });

  socket.on("disconnect", () => {
    handleLeave(socket);
    console.log("Client disconnected:", socket.id);
  });

  function handleLeave(socket) {
    const roomId = socket.data?.roomId;

    if (!roomId) return;

    const room = rooms.get(roomId);

    if (!room) return;

    const member = room.members.get(socket.id);

    room.members.delete(socket.id);

    socket.leave(roomId);

    socket.data = {};

    if (member) {
      // Notify everyone with the latest online/offline state.
      io.to(roomId).emit("room_members_updated", {
        members: getRoomPublicData(room).members,
      });

      socket.to(roomId).emit("member_left", {
        member: {
          id: member.id,
          name: member.name,
        },
        room: getRoomPublicData(room),
      });
    }
  }
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => console.log(`FileHive server running on :${PORT}`));
