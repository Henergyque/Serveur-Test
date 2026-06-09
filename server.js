'use strict';

const http    = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const PORT       = process.env.PORT || 3001;
const GAME_TOKEN = process.env.GAME_TOKEN || '';
// Maps de la zone "endgame" : le premier joueur qui y arrive gagne la course.
const FINISH_MAPS = new Set(
  (process.env.FINISH_MAPS || '5,15,16,17').split(',').map(s => Number(s.trim())).filter(Boolean)
);

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocketServer({ server });

// players : playerId → { ws, lobbyId, ready, mapId, x, y, dir, characterName, characterIndex, lastSeen }
const players = new Map();
// lobbies : lobbyId → { id, ownerId, members: Set<playerId>, started }
const lobbies = new Map();

// ─── Helpers ────────────────────────────────────────────────────────────────

function genLobbyId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ';
  let id;
  do {
    id = Array.from({ length: 4 }, () => chars[Math.random() * chars.length | 0]).join('');
  } while (lobbies.has(id));
  return id;
}

function sendTo(playerId, payload) {
  const p = players.get(playerId);
  if (p && p.ws.readyState === 1) p.ws.send(JSON.stringify(payload));
}

function broadcastLobbyUpdate(lobbyId) {
  const lobby = lobbies.get(lobbyId);
  if (!lobby) return;
  const members = Array.from(lobby.members).map(pid => {
    const p = players.get(pid);
    return { playerId: pid, ready: p ? p.ready : false, isOwner: pid === lobby.ownerId };
  });
  const payload = { type: 'lobby_update', lobbyId, members };
  lobby.members.forEach(pid => sendTo(pid, payload));
}

// Prévient les membres restants qu'un joueur a quitté une partie en cours
// (en lobby, broadcastLobbyUpdate suffit ; en jeu, le client a besoin d'un signal dédié)
function notifyPeerLeft(lobby) {
  if (!lobby.started) return;
  lobby.members.forEach(pid => sendTo(pid, { type: 'peer_left' }));
}

function dissolve(lobbyId, excludeId) {
  const lobby = lobbies.get(lobbyId);
  if (!lobby) return;
  lobby.members.forEach(pid => {
    if (pid !== excludeId) sendTo(pid, { type: 'lobby_dissolved' });
    const p = players.get(pid);
    if (p) p.lobbyId = null;
  });
  lobbies.delete(lobbyId);
}

function handleDisconnect(playerId, ws) {
  const player = players.get(playerId);
  if (!player) return;
  if (ws && player.ws !== ws) return; // nouvelle connexion a pris le relais, ignorer
  if (player.lobbyId) {
    const lobby = lobbies.get(player.lobbyId);
    if (lobby) {
      if (lobby.ownerId === playerId) {
        dissolve(player.lobbyId, playerId);
      } else {
        lobby.members.delete(playerId);
        broadcastLobbyUpdate(lobby.id);
        notifyPeerLeft(lobby);
      }
    }
  }
  players.delete(playerId);
}

// ─── HTTP ────────────────────────────────────────────────────────────────────

app.get('/health', (_req, res) => res.json({ ok: true, version: '1.1.0', lobbies: lobbies.size, players: players.size }));

// ─── WebSocket ───────────────────────────────────────────────────────────────

wss.on('connection', (ws) => {
  let playerId = null;
  let authed   = false;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // ── Auth (first message) ──
    if (!authed) {
      if (msg.type !== 'auth') return;
      if (GAME_TOKEN && msg.gameToken !== GAME_TOKEN) { ws.close(4001, 'unauthorized'); return; }
      if (!msg.playerId) { ws.close(4002, 'missing playerId'); return; }
      playerId = String(msg.playerId).slice(0, 64);
      authed = true;
      // Si même playerId reconnecte : enregistre le nouveau d'abord, puis coupe l'ancien.
      // handleDisconnect vérifiera le ws avant de supprimer, donc pas de race condition.
      const old = players.get(playerId);
      players.set(playerId, {
        ws, lobbyId: null, ready: false,
        mapId: 0, x: 0, y: 0, dir: 2,
        characterName: '', characterIndex: 0,
        lastSeen: Date.now()
      });
      if (old && old.ws !== ws) try { old.ws.terminate(); } catch {}
      return;
    }

    const player = players.get(playerId);
    if (!player) return;
    player.lastSeen = Date.now();

    switch (msg.type) {

      case 'create_lobby': {
        if (player.lobbyId) return;
        const id = genLobbyId();
        lobbies.set(id, { id, ownerId: playerId, members: new Set([playerId]), started: false, finished: false, winnerId: null });
        player.lobbyId = id;
        player.ready   = false;
        sendTo(playerId, { type: 'lobby_created', lobbyId: id });
        break;
      }

      case 'join_lobby': {
        if (player.lobbyId) return;
        const lobbyId = String(msg.lobbyId || '').toUpperCase().slice(0, 4);
        const lobby   = lobbies.get(lobbyId);
        if (!lobby)                { sendTo(playerId, { type: 'lobby_error', message: 'Lobby introuvable.' }); return; }
        if (lobby.members.size >= 2) { sendTo(playerId, { type: 'lobby_error', message: 'Lobby plein (max 2 joueurs).' }); return; }
        if (lobby.started)         { sendTo(playerId, { type: 'lobby_error', message: 'Partie déjà commencée.' }); return; }
        lobby.members.add(playerId);
        player.lobbyId = lobbyId;
        player.ready   = false;
        broadcastLobbyUpdate(lobbyId);
        break;
      }

      case 'ready': {
        if (!player.lobbyId) return;
        player.ready = !player.ready;
        broadcastLobbyUpdate(player.lobbyId);
        break;
      }

      case 'start': {
        const lobby = lobbies.get(player.lobbyId);
        if (!lobby || lobby.ownerId !== playerId) return;
        if (lobby.members.size < 2) { sendTo(playerId, { type: 'lobby_error', message: 'En attente d\'un 2e joueur.' }); return; }
        const allReady = Array.from(lobby.members).every(pid => { const p = players.get(pid); return p && p.ready; });
        if (!allReady) { sendTo(playerId, { type: 'lobby_error', message: 'Les deux joueurs doivent être prêts.' }); return; }
        lobby.started = true;
        lobby.members.forEach(pid => sendTo(pid, { type: 'game_start', lobbyId: player.lobbyId }));
        break;
      }

      case 'pos': {
        if (!player.lobbyId) return;
        player.mapId          = Number(msg.mapId)          || 0;
        player.x              = Number(msg.x)              || 0;
        player.y              = Number(msg.y)              || 0;
        player.dir            = Number(msg.dir)            || 2;
        player.characterName  = String(msg.characterName  || '').slice(0, 64);
        player.characterIndex = Number(msg.characterIndex) || 0;

        const lobby = lobbies.get(player.lobbyId);
        if (!lobby) return;

        // Course : premier joueur à atteindre la zone endgame
        if (lobby.started && !lobby.finished && FINISH_MAPS.has(player.mapId)) {
          lobby.finished = true;
          lobby.winnerId = playerId;
          lobby.members.forEach(pid => sendTo(pid, { type: 'race_end', winnerId: playerId }));
        }

        const positions = Array.from(lobby.members).map(pid => {
          const p = players.get(pid);
          if (!p) return null;
          return { playerId: pid, mapId: p.mapId, x: p.x, y: p.y, dir: p.dir, characterName: p.characterName, characterIndex: p.characterIndex };
        }).filter(Boolean);

        // Envoyé à tout le monde, y compris l'envoyeur : son propre flux 20 Hz lui garantit
        // la dernière position connue de l'adversaire même si celui-ci est inactif (menu, AFK)
        // — sinon le ghost n'apparaît pas en arrivant sur sa map.
        lobby.members.forEach(pid => {
          sendTo(pid, { type: 'positions', players: positions });
        });
        break;
      }

      case 'leave_lobby': {
        if (!player.lobbyId) return;
        const lobby = lobbies.get(player.lobbyId);
        if (lobby) {
          if (lobby.ownerId === playerId) {
            dissolve(player.lobbyId, playerId);
          } else {
            lobby.members.delete(playerId);
            broadcastLobbyUpdate(lobby.id);
            notifyPeerLeft(lobby);
          }
        }
        player.lobbyId = null;
        player.ready   = false;
        break;
      }

      case 'ping':
        break; // lastSeen déjà rafraîchi plus haut, rien d'autre à faire
    }
  });

  ws.on('close', () => handleDisconnect(playerId, ws));
  ws.on('error', () => handleDisconnect(playerId, ws));
});

// ─── Cleanup stale connections ───────────────────────────────────────────────

setInterval(() => {
  const now = Date.now();
  players.forEach((p, id) => {
    if (now - p.lastSeen > 30000) {
      console.log('[cleanup] removing stale player', id);
      try { p.ws.terminate(); } catch {}
      handleDisconnect(id);
    }
  });
}, 10000);

// ─── Start ───────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
  console.log('[SG-Multi] Server running on port', PORT);
  if (!GAME_TOKEN) console.warn('[SG-Multi] WARNING: GAME_TOKEN not set — all connections accepted');
});