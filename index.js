import { useState, useEffect, useRef, useCallback, useReducer } from "react";
import { useSocket } from "../SocketContext";
import { useAuth } from "../AuthContext";
import TeamsPanel from "./TeamsPanel";
import Swal from "sweetalert2";
import RestorePanel from "./RestorePanel";
import ChatPanel from "./ChatPanel";
import FilePreviewModal from "./FilePreviewModal";
import ChatDrawer from "./ChatDrawer";
import {
  fileIcon,
  formatBytes,
  formatTime,
  isExecutableFile,
  queueReducer,
} from "../helpers/functions";
import { uploadFileResumable } from "../helpers/resumableUpload";
import ShareFiles from "../svg/ShareFiles";
import RoomHeader from "./RoomHeader";
import MembersPanel from "./MembersPanel";
import FilesPanel from "./FilesPanel";
import FilesSection from "./FilesSection";

const API = process.env.REACT_APP_BACKEND_API_URL;

export default function Room({ session, onLeft }) {
  const { socket } = useSocket();
  const { user, sendTeamsInvite } = useAuth();
  const [room, setRoom] = useState(session.room);
  const [toast, setToast] = useState(null);
  const [isDragging, setIsDragging] = useState(false);
  const [showTeams, setShowTeams] = useState(false);
  const [showRestore, setShowRestore] = useState(false);
  const [showMobileFiles, setShowMobileFiles] = useState(false);
  const [showMobileMembers, setShowMobileMembers] = useState(false);
  const [deletingId, setDeletingId] = useState(null);
  const fileInputRef = useRef();
  const isCreator = room.creatorId === session.userId;
  const isPersonalRoom = room.id.startsWith("MY-");
  const [downloadMap, setDownloadMap] = useState({});
  const [isSomeoneUploading, setIsSomeoneUploading] = useState(false);
  const [reinviteLoading, setReinviteLoading] = useState(false);
  const [previewFileData, setPreviewFileData] = useState(null);
  const [isChatDrawerOpen, setIsChatDrawerOpen] = useState(false);
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [hasNewChatMessage, setHasNewChatMessage] = useState(false);

  // ── Upload queue ─────────────────────────────────────────────────────────────
  const [queue, dispatch] = useReducer(queueReducer, []);
  const processingRef = useRef(false);
  // Ab XHR ki jagah AbortController store hota hai (item.id -> controller)
  const abortMapRef = useRef(new Map());
  const cancelledRef = useRef(new Set());
  const queueRef = useRef(queue);
  const lastProgressRef = useRef(new Map());
  queueRef.current = queue;

  const showToast = useCallback((msg, type = "info") => {
    setToast({ msg, type, id: Date.now() });
    setTimeout(() => setToast(null), 5000);
  }, []);

  const startedRef = useRef(new Set()); // ids already picked up, avoids stale-ref double picks

  // ── Resumable chunked upload ────────────────────────────────────────────────
  // Har file 2 MB ke chunks mein jati hai. Network toote to wahin se resume hoti hai.
  const uploadFile = useCallback(
    (item) => {
      const controller = new AbortController();
      abortMapRef.current.set(item.id, controller);

      socket.emit("file_uploading", {
        roomId: room.id,
        uploaderId: session.userId,
      });

      return uploadFileResumable({
        apiBase: API,
        roomId: room.id,
        file: item.file,
        uploaderId: session.userId,
        uploaderName: session.userName,
        signal: controller.signal,
        onProgress: ({ percent }) => {
          const pct = Math.min(99, percent);
          const key = String(item.id);
          const last = lastProgressRef.current.get(key) ?? 0;

          // Progress kabhi peeche nahi jana chahiye
          if (pct <= last) return;

          lastProgressRef.current.set(key, pct);
          dispatch({ type: "PROGRESS", id: item.id, progress: pct });
        },
      }).finally(() => {
        abortMapRef.current.delete(item.id);
        lastProgressRef.current.delete(String(item.id));
      });
    },
    [socket, room.id, session.userId, session.userName],
  );

  const cancelUpload = useCallback((id) => {
    cancelledRef.current.add(id);

    // Abort karne par helper server se temp file bhi delete kar deta hai
    const controller = abortMapRef.current.get(id);
    if (controller) {
      controller.abort();
      abortMapRef.current.delete(id);
    }

    dispatch({
      type: "CANCEL",
      id,
    });

    startedRef.current.delete(id);
  }, []);

  const processQueue = useCallback(async () => {
    if (processingRef.current) return;
    processingRef.current = true;

    try {
      let next;
      while (
        (next = queueRef.current.find(
          (i) => i.status === "queued" && !startedRef.current.has(i.id),
        ))
      ) {
        startedRef.current.add(next.id);
        dispatch({ type: "START", id: next.id });

        try {
          await uploadFile(next);
          if (!cancelledRef.current.has(next.id)) {
            dispatch({
              type: "DONE",
              id: next.id,
            });
          }
        } catch (err) {
          if (cancelledRef.current.has(next.id)) {
            // User ne cancel kiya, queue aage chalne do
            cancelledRef.current.delete(next.id);
          } else {
            dispatch({ type: "ERROR", id: next.id, error: err.message });
            showToast(`${next.file.name}: ${err.message}`, "error");
          }
        }

        // let React re-render so queueRef.current is fresh before the next loop
        await new Promise((r) => setTimeout(r, 0));
      }
    } finally {
      processingRef.current = false;
    }
  }, [uploadFile, showToast]);

  const getChatLastSeenKey = useCallback(
    () => `filehive_chat_last_seen_${session.userId}_${room.id}`,
    [session.userId, room.id],
  );

  const getChatLastSeen = useCallback(() => {
    const value = localStorage.getItem(getChatLastSeenKey());

    return value || null;
  }, [getChatLastSeenKey]);

  const toggleChat = useCallback(() => {
    setIsChatOpen((prev) => {
      const next = !prev;

      if (next) {
        setHasNewChatMessage(false);
      }

      return next;
    });
  }, []);

  // NOTE: purana socket "upload_progress" listener hata diya gaya hai.
  // Progress ab seedha client ke chunk upload se aata hai (onProgress).

  useEffect(() => {
    const hasQueued = queue.some((i) => i.status === "queued");
    if (hasQueued && !processingRef.current) processQueue();
  }, [queue, processQueue]);

  useEffect(() => {
    const allDone =
      queue.length > 0 &&
      queue.every(
        (i) =>
          i.status === "done" ||
          i.status === "error" ||
          i.status === "cancelled",
      );
    if (allDone) {
      const t = setTimeout(() => dispatch({ type: "CLEAR_DONE" }), 3000);
      return () => clearTimeout(t);
    }
  }, [queue]);

  function enqueueFiles(files) {
    const valid = Array.from(files).filter((f) => {
      if (isExecutableFile(f)) {
        showToast(
          `${f.name} cannot be uploaded because executable files are not allowed.`,
          "error",
        );
        return false;
      }

      if (f.size > 100 * 1024 * 1024) {
        showToast(f.name + " is too large (max 100MB)", "error");
        return false;
      }

      return true;
    });

    if (valid.length) {
      dispatch({
        type: "ENQUEUE",
        files: valid,
      });
    }
  }

  function handleFileInput(e) {
    enqueueFiles(e.target.files);
    e.target.value = "";
  }

  function handleDrop(e) {
    e.preventDefault();
    setIsDragging(false);
    enqueueFiles(e.dataTransfer.files);
  }

  // ── Socket events ─────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!socket) return;

    socket.on("file_shared", ({ file, room: r }) => {
      setRoom(r);

      if (file.uploaderId !== session.userId) {
        setIsSomeoneUploading(false);

        showToast(file.uploadedBy + ' shared "' + file.fileName + '"', "file");
      }
    });
    socket.on("file_deleted", ({ room: r }) => setRoom(r));
    socket.on("file_restored", ({ fileId, room }) => {
      setRoom(room);
    });
    socket.on("file_permanently_deleted", ({ fileId, room }) => {
      setRoom(room);
    });
    socket.on("member_joined", ({ member, room: updatedRoom }) => {
      setRoom(updatedRoom);
      try {
        if (user?.id) {
          const recentKey = "filehive_recent_" + user.id;
          const recent = JSON.parse(localStorage.getItem(recentKey) || "[]");
          const updated = recent.map((r) =>
            r.id === updatedRoom.id
              ? { ...r, pastMembers: updatedRoom.pastMembers || [] }
              : r,
          );
          localStorage.setItem(recentKey, JSON.stringify(updated));
        }
      } catch (err) {
        console.error("Failed to update recent room data:", err);
      }
      showToast(member.name + " joined the room", "join");
    });
    socket.on("member_left", ({ member, room: updatedRoom }) => {
      setRoom(updatedRoom);
      try {
        if (user?.id) {
          const recentKey = "filehive_recent_" + user.id;
          const recent = JSON.parse(localStorage.getItem(recentKey) || "[]");
          const updated = recent.map((r) =>
            r.id === updatedRoom.id
              ? { ...r, pastMembers: updatedRoom.pastMembers || [] }
              : r,
          );
          localStorage.setItem(recentKey, JSON.stringify(updated));
        }
      } catch (err) {
        console.error("Failed to update recent room data:", err);
      }
      showToast(member.name + " left the room", "leave");
    });

    const handleRoomEnded = ({ message, reason }) => {
      if (reason === "inactivity") {
        Swal.fire({
          icon: "info",
          title: "Session expired",
          text: message,
          confirmButtonText: "OK",
          allowOutsideClick: false,
        }).then(() => onLeft());
        return;
      }

      showToast(message, "error");
      setTimeout(() => onLeft(), 2200);
    };

    const handleInactivityWarning = async ({ minutesLeft }) => {
      const message = `This room will close in ${minutesLeft} min due to inactivity.`;

      showToast(message, "error");

      // Only notify when the tab is inactive
      if (document.visibilityState !== "visible") {
        if (Notification.permission === "default") {
          const permission = await Notification.requestPermission();

          if (permission === "granted") {
            new Notification("FileHive", {
              body: message,
              icon: "/favicon.ico",
            });
          }
        } else if (Notification.permission === "granted") {
          new Notification("FileHive", {
            body: message,
            icon: "/favicon.ico",
          });
        }
      }
    };

    socket.on("room_ended", handleRoomEnded);
    socket.on("room_inactivity_warning", handleInactivityWarning);

    socket.on(
      "file_downloaded",
      ({ fileId, fileName, downloaderId, downloaderName }) => {
        // Update count for everyone including self
        setDownloadMap((prev) => {
          const existing = prev[fileId] ? new Set(prev[fileId]) : new Set();
          existing.add(downloaderId);
          return { ...prev, [fileId]: existing };
        });
        if (downloaderId === session.userId) return;
        showToast(
          `${downloaderName} has downloaded "${fileName}" successfully`,
          "download",
        );
      },
    );

    socket.on("file_uploading", ({ roomId, uploaderId }) => {
      if (roomId !== room.id) return;

      // Apni upload par loader mat dikhao
      if (uploaderId === session.userId) return;

      setIsSomeoneUploading(true);
    });

    const handleNewChatMessage = (newMessage) => {
      if (newMessage.roomId !== room?.id) return;

      // Keep Room state synchronized with incoming chat message
      setRoom((prev) => {
        if (!prev) return prev;

        const existingMessages = Array.isArray(prev.messages)
          ? prev.messages
          : [];

        // Prevent duplicate message
        if (existingMessages.some((item) => item.id === newMessage.id)) {
          return prev;
        }

        return {
          ...prev,
          messages: [...existingMessages, newMessage],
        };
      });

      // Own messages don't trigger notification/blinking
      if (newMessage.senderId === session.userId) return;

      // Chat is currently open
      if (isChatOpen) return;

      const lastSeen = localStorage.getItem(
        `filehive_chat_last_seen_${session.userId}_${room.id}`,
      );

      // Never seen chat before
      if (!lastSeen) {
        setHasNewChatMessage(true);
        return;
      }

      const messageTime = new Date(newMessage.sentAt).getTime();
      const lastSeenTime = new Date(lastSeen).getTime();

      if (messageTime > lastSeenTime) {
        setHasNewChatMessage(true);
      }
    };

    socket.on("chat_message", handleNewChatMessage);

    return () => {
      socket.off("file_shared");
      socket.off("file_downloaded");
      socket.off("file_deleted");
      socket.off("member_joined");
      socket.off("member_left");
      socket.off("room_ended");
      socket.off("file_uploading");
      socket.off("chat_message", handleNewChatMessage);
      socket.off("room_ended", handleRoomEnded);
      socket.off("room_inactivity_warning", handleInactivityWarning);
    };
  }, [socket, session.userId, user, showToast, onLeft, room.id, isChatOpen]);

  function downloadFile(file) {
    const a = document.createElement("a");
    a.href = API + "/api/rooms/" + room.id + "/files/" + file.id + "/download";
    a.download = file.fileName;
    // a.target = "_blank";
    a.click();

    // Optimistically update own count
    setDownloadMap((prev) => {
      const existing = prev[file.id] ? new Set(prev[file.id]) : new Set();
      existing.add(session.userId);
      return { ...prev, [file.id]: existing };
    });

    // Tell everyone in the room who downloaded the file
    socket.emit("file_downloaded", {
      roomId: room.id,
      fileId: file.id,
      fileName: file.fileName,
      downloaderId: session.userId,
      downloaderName: session.userName,
    });

    // Show it immediately to the person who downloaded it
    showToast(
      `You have downloaded "${file.fileName}" successfully`,
      "download",
    );
  }

  function previewFile(file, roomId) {
    const url = API + "/api/rooms/" + roomId + "/files/" + file.id + "/preview";
    setPreviewFileData({
      file,
      url,
    });
  }

  async function deleteFile(file) {
    // setDeletingId(file.id);
    try {
      const res = await fetch(
        API + "/api/rooms/" + room.id + "/files/" + file.id,
        {
          method: "DELETE",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ requesterId: session.userId }),
        },
      );
      const data = await res.json();
      if (!res.ok) showToast(data.error || "Delete failed", "error");
    } catch {
      showToast("Delete failed", "error");
    }
    // finally { setDeletingId(null); }
  }
  async function handleLeave() {
    const result = await Swal.fire({
      title: "Leave this room?",
      text: "You will leave this room and won't be able to access it unless invited again.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Yes, leave",
      cancelButtonText: "Cancel",
      reverseButtons: true,
    });

    if (result.isConfirmed) {
      socket.emit("leave_room", () => onLeft());
    }
  }

  async function handleEndRoom() {
    const result = await Swal.fire({
      title: "End the session?",
      text: "All files will be deleted from the server and everyone will be removed from this room.",
      icon: "warning",
      showCancelButton: true,
      confirmButtonText: "Yes, end room",
      cancelButtonText: "Cancel",
      reverseButtons: true,
    });

    if (result.isConfirmed) {
      socket.emit("end_room", (res) => {
        if (res?.success) {
          onLeft();
        }
      });
    }
  }

  function copyRoomCode() {
    navigator.clipboard
      .writeText(room.id)
      .then(() => showToast("Room code copied!", "info"));
  }

  const activeUploads = queue.filter(
    (i) => i.status === "uploading" || i.status === "queued",
  );
  const isUploading = activeUploads.length > 0;
  const currentUpload = queue.find((i) => i.status === "uploading");
  const myFiles = room.files
    .filter((f) => f.uploaderId === session.userId && !f.deleted)
    .sort((a, b) => new Date(b.uploadedAt) - new Date(a.uploadedAt));
  const othersFiles = room.files
    .filter((f) => f.uploaderId !== session.userId)
    .sort((a, b) => new Date(b.uploadedAt) - new Date(a.uploadedAt));

  useEffect(() => {
    if (!socket) return;

    const handleRoomMembersUpdated = ({ members }) => {
      if (!Array.isArray(members)) {
        console.warn("room_members_updated received invalid members:", members);
        return;
      }

      setRoom((prev) => {
        if (!prev) return prev;

        return {
          ...prev,
          members,
          memberCount: members.length,
        };
      });

      // Keep recent-room cache in sync as well.
      try {
        if (user?.id) {
          const recentKey = `filehive_recent_${user.id}`;
          const recent = JSON.parse(localStorage.getItem(recentKey) || "[]");

          const currentRoomId = room?.id;

          const updated = recent.map((r) =>
            r.id === currentRoomId
              ? {
                  ...r,
                  members,
                }
              : r,
          );

          localStorage.setItem(recentKey, JSON.stringify(updated));
        }
      } catch (err) {
        console.error("Failed to update recent room members:", err);
      }
    };

    socket.on("room_members_updated", handleRoomMembersUpdated);

    return () => {
      socket.off("room_members_updated", handleRoomMembersUpdated);
    };
  }, [socket, user?.id, room?.id]);

  const handleReInvite = async (member) => {
    setReinviteLoading(true);
    const memberId = member.id || member.userId;

    try {
      const result = await sendTeamsInvite(
        memberId,
        member.email || member.mail || member.userPrincipalName || "",
        room.id,
        room.name,
      );

      if (result?.success) {
        setRoom((prev) => {
          if (!prev) return prev;

          const updatedMembers = prev.members.map((m) =>
            m.id === memberId
              ? {
                  ...m,
                  invitationStatus: "pending",
                }
              : m,
          );

          return {
            ...prev,
            members: updatedMembers,
          };
        });
      }
    } catch (error) {
      console.error("Invitation error:", error);
    } finally {
      setReinviteLoading(false);
    }
  };

  const handleKickoutMember = (member) => {
    Swal.fire({
      icon: "warning",
      title: "Remove member?",
      text: `Remove ${member.name} from this room?`,
      showCancelButton: true,
      confirmButtonText: "Remove",
      cancelButtonText: "Cancel",
    }).then((result) => {
      if (!result.isConfirmed) return;

      socket.emit(
        "kick_member",
        {
          roomId: room.id,
          memberId: member.id,
        },
        (response) => {
          if (!response?.success) {
            Swal.fire({
              icon: "error",
              title: "Unable to remove member",
              text: response?.error || "Something went wrong.",
            });
          }
        },
      );
    });
  };

  useEffect(() => {
    if (!socket) return;

    const handleMemberKicked = ({ roomId, memberId }) => {
      if (roomId !== room?.id) return;

      setRoom((prev) => {
        if (!prev) return prev;

        return {
          ...prev,
          members: (prev.members || []).filter(
            (member) => member.id !== memberId,
          ),
        };
      });
    };

    socket.on("member_kicked", handleMemberKicked);

    return () => {
      socket.off("member_kicked", handleMemberKicked);
    };
  }, [socket, room?.id]);

  useEffect(() => {
    if (!socket) return;

    const handleKickedFromRoom = async ({ roomId, roomName, message }) => {
      console.log("🚫 You were kicked from room:", roomId);

      // Make sure this is the room currently open
      if (room?.id !== roomId) return;

      await Swal.fire({
        icon: "warning",
        title: "Removed from room",
        text: message || "You have been removed from this room.",
        confirmButtonText: "OK",
      });

      // Close the current FileHive session
      onLeft();
    };

    socket.on("kicked_from_room", handleKickedFromRoom);

    return () => {
      socket.off("kicked_from_room", handleKickedFromRoom);
    };
  }, [socket, room?.id, onLeft]);

  return (
    <div className="room-shell">
      <RoomHeader
        room={room}
        isPersonalRoom={isPersonalRoom}
        isCreator={isCreator}
        copyRoomCode={copyRoomCode}
        setIsChatDrawerOpen={setIsChatDrawerOpen}
        setShowTeams={setShowTeams}
        setShowMobileMembers={setShowMobileMembers}
        showMobileMembers={showMobileMembers}
        setShowRestore={setShowRestore}
        handleLeave={handleLeave}
        handleEndRoom={handleEndRoom}
      />

      <div className="room-body">
        <main className="room-main">
          {/* Drop zone */}
          <div
            className={
              "drop-zone" +
              (isDragging ? " dragging" : "") +
              (isUploading ? " uploading" : "")
            }
            onDragOver={(e) => {
              e.preventDefault();
              setIsDragging(true);
            }}
            onDragLeave={() => setIsDragging(false)}
            onDrop={handleDrop}
            onClick={() => fileInputRef.current?.click()}
          >
            <input
              ref={fileInputRef}
              type="file"
              hidden
              multiple
              onChange={handleFileInput}
            />
            <div className="drop-inner">
              {isUploading ? (
                <>
                  <div className="queue-progress-stack">
                    {queue
                      ?.filter((i) => i.status !== "done")
                      ?.map((item) => (
                        <div
                          key={item.id}
                          className={"queue-item queue-item--" + item.status}
                        >
                          <span className="qi-icon">
                            {fileIcon(item.file.type)}
                          </span>
                          <div className="qi-body">
                            <div className="qi-name">{item.file.name}</div>
                            <div className="qi-bar-wrap">
                              <div
                                className="qi-bar"
                                style={{
                                  width:
                                    item.status === "queued"
                                      ? "0%"
                                      : item.progress + "%",
                                }}
                              />
                            </div>
                          </div>
                          <span className="qi-status">
                            {item.status === "queued" && (
                              <>
                                <span>queued</span>

                                <button
                                  type="button"
                                  className="qi-cancel"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    cancelUpload(item.id);
                                  }}
                                >
                                  Cancel
                                </button>
                              </>
                            )}

                            {item.status === "uploading" && (
                              <>
                                <span>{item.progress}%</span>

                                <button
                                  type="button"
                                  className="qi-cancel"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    cancelUpload(item.id);
                                  }}
                                >
                                  Cancel
                                </button>
                              </>
                            )}

                            {item.status === "error" && "✕"}

                            {item.status === "cancelled" && "Cancelled"}
                          </span>
                        </div>
                      ))}
                  </div>
                  <p className="drop-hint" style={{ marginTop: "8px" }}>
                    Drop more files to add to queue
                  </p>
                </>
              ) : (
                <>
                  <ShareFiles />
                  <p className="drop-title">
                    {isDragging ? "Drop to share" : "Share files"}
                  </p>
                  <p className="drop-hint">
                    Drag & drop or click · Multiple files supported · Max 100MB
                    each
                  </p>
                </>
              )}
            </div>
          </div>

          {/* Files */}
          <FilesSection
            room={room}
            othersFiles={othersFiles}
            myFiles={myFiles}
            downloadFile={downloadFile}
            deleteFile={deleteFile}
            deletingId={deletingId}
            downloadMap={downloadMap}
            previewFile={previewFile}
            previewFileData={previewFileData}
            setPreviewFileData={setPreviewFileData}
            loading={isSomeoneUploading}
          />
        </main>

        {!isPersonalRoom && (
          <ChatDrawer
            room={room}
            session={session}
            isOpen={isChatDrawerOpen}
            onClose={() => setIsChatDrawerOpen(false)}
            currentUserId={user?.id}
          />
        )}
        {!isPersonalRoom && (
          <button
            type="button"
            className={`chat-side-tab ${
              isChatOpen ? "chat-side-tab--open " : ""
            } ${
              hasNewChatMessage && !isChatOpen ? "chat-side-tab--blink" : ""
            }`}
            onClick={toggleChat}
          >
            {isChatOpen ? (
              "CLOSE"
            ) : (
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "space-between",
                  gap: "8px",
                }}
              >
                <h4>CHAT</h4>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  className="lucide lucide-messages-square preview-icon"
                >
                  <path d="M16 10a2 2 0 0 1-2 2H6.828a2 2 0 0 0-1.414.586l-2.202 2.202A.71.71 0 0 1 2 14.286V4a2 2 0 0 1 2-2h10a2 2 0 0 1 2 2z" />
                  <path d="M20 9a2 2 0 0 1 2 2v10.286a.71.71 0 0 1-1.212.502l-2.202-2.202A2 2 0 0 0 17.172 19H10a2 2 0 0 1-2-2v-1" />
                </svg>
              </div>
            )}
          </button>
        )}
      </div>
      {!isPersonalRoom && (
        <div
          className={`chat-panel-wrapper ${isChatOpen ? "chat-panel-open" : ""}`}
        >
          <ChatPanel
            room={room}
            currentUserId={user?.id}
            isOpen={isChatOpen}
            onClose={() => setIsChatOpen(false)}
          />
        </div>
      )}
      {showTeams && (
        <>
          <div className="panel-overlay" onClick={() => setShowTeams(false)} />
          <TeamsPanel
            socket={socket}
            room={room}
            currentUserId={session.userId}
            onClose={() => {
              setShowTeams(false);
              sessionStorage.setItem(`teams-panel-seen-${room.id}`, "true");
            }}
          />
        </>
      )}

      {showRestore && (
        <>
          <div
            className="panel-overlay"
            onClick={() => setShowRestore(false)}
          />
          <RestorePanel
            room={room}
            currentUserId={session.userId}
            onClose={() => {
              setShowRestore(false);
            }}
          />
        </>
      )}

      {/* Files Side Drawer */}
      {showMobileFiles && (
        <FilesPanel
          room={room}
          setShowMobileFiles={setShowMobileFiles}
          downloadFile={downloadFile}
          fileIcon={fileIcon}
          formatBytes={formatBytes}
        />
      )}

      {/* Members Side Drawer */}
      {showMobileMembers && (
        <MembersPanel
          room={room}
          session={session}
          setShowMobileMembers={setShowMobileMembers}
          reinviteLoading={reinviteLoading}
          handleReInvite={handleReInvite}
          handleKickoutMember={handleKickoutMember}
        />
      )}

      {toast && (
        <div key={toast.id} className={"toast toast-" + toast.type}>
          {toast.type === "file" && "📎 "}
          {toast.type === "join" && "👋 "}
          {toast.type === "leave" && "🚪 "}
          {toast.type === "error" && "⚠ "}
          {toast.msg}
        </div>
      )}
    </div>
  );
}
