'use strict';
// Test d'intégration du serveur multiplayer — simule 2 joueurs sur tout le flux.
// Usage : node _test_integration.js  (lance le serveur lui-même sur un port de test)

process.env.PORT = '3919';
process.env.GAME_TOKEN = 'test-token';

require('./server.js');

const WebSocket = require('ws');
const URL = 'ws://127.0.0.1:3919';

let passed = 0, failed = 0;
function ok(cond, label) {
  if (cond) { passed++; console.log('  PASS', label); }
  else      { failed++; console.log('  FAIL', label); }
}

function mkClient(playerId, token) {
  const ws = new WebSocket(URL);
  const inbox = [];
  ws.on('message', d => inbox.push(JSON.parse(d)));
  return new Promise((resolve, reject) => {
    ws.on('open', () => {
      ws.send(JSON.stringify({ type: 'auth', gameToken: token === undefined ? 'test-token' : token, playerId }));
      resolve({ ws, inbox, send: o => ws.send(JSON.stringify(o)) });
    });
    ws.on('error', reject);
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));
const last  = (inbox, type) => [...inbox].reverse().find(m => m.type === type);
const count = (inbox, type) => inbox.filter(m => m.type === type).length;

(async () => {
  await sleep(300); // laisse le serveur démarrer

  console.log('— Auth —');
  const badAuth = new WebSocket(URL);
  await new Promise(r => badAuth.on('open', r));
  const badClose = new Promise(r => badAuth.on('close', code => r(code)));
  badAuth.send(JSON.stringify({ type: 'auth', gameToken: 'wrong', playerId: 'X' }));
  ok(await badClose === 4001, 'mauvais token rejeté (4001)');

  const A = await mkClient('player-A');
  const B = await mkClient('player-B');
  await sleep(100);

  console.log('— Lobby —');
  A.send({ type: 'create_lobby' });
  await sleep(100);
  const created = last(A.inbox, 'lobby_created');
  ok(created && /^[A-Z]{4}$/.test(created.lobbyId), 'lobby créé avec code 4 lettres');
  const CODE = created.lobbyId;

  B.send({ type: 'join_lobby', lobbyId: 'ZZZZ' });
  await sleep(100);
  ok(last(B.inbox, 'lobby_error'), 'join code invalide → lobby_error');

  B.send({ type: 'join_lobby', lobbyId: CODE });
  await sleep(100);
  let upd = last(A.inbox, 'lobby_update');
  ok(upd && upd.members.length === 2, 'lobby_update reçu avec 2 membres');

  console.log('— Start —');
  A.send({ type: 'start' });
  await sleep(100);
  ok(last(A.inbox, 'lobby_error'), 'start sans ready → lobby_error');

  A.send({ type: 'ready' }); B.send({ type: 'ready' });
  await sleep(100);
  B.send({ type: 'start' });   // non-owner : doit être ignoré
  await sleep(100);
  ok(!last(B.inbox, 'game_start'), 'start par non-owner ignoré');

  A.send({ type: 'start' });
  await sleep(100);
  ok(last(A.inbox, 'game_start') && last(B.inbox, 'game_start'), 'game_start reçu par les deux');

  console.log('— Positions —');
  A.inbox.length = 0; B.inbox.length = 0;
  A.send({ type: 'pos', mapId: 1, x: 10, y: 12, dir: 4, progress: 42, characterName: 'Actor1', characterIndex: 0 });
  await sleep(100);
  let posB = last(B.inbox, 'positions');
  ok(posB && posB.players.some(p => p.playerId === 'player-A' && p.x === 10 && p.y === 12), 'B reçoit la position de A');
  ok(posB && posB.players.some(p => p.playerId === 'player-A' && p.progress === 42), 'le progress de A est inclus dans positions');
  ok(last(A.inbox, 'positions'), 'echo : A reçoit aussi le snapshot (adversaire idle visible)');

  console.log('— Course —');
  A.inbox.length = 0; B.inbox.length = 0;
  B.send({ type: 'pos', mapId: 18, x: 3, y: 3, dir: 2, progress: 99, characterName: 'Actor1', characterIndex: 1 });
  await sleep(100);
  ok(!last(A.inbox, 'race_end'), 'pas de race_end à 99%');
  B.send({ type: 'pos', mapId: 18, x: 3, y: 3, dir: 2, progress: 100, characterName: 'Actor1', characterIndex: 1 });
  await sleep(100);
  const endA = last(A.inbox, 'race_end'), endB = last(B.inbox, 'race_end');
  ok(endA && endA.winnerId === 'player-B' && endB && endB.winnerId === 'player-B', 'race_end broadcast à 100%, vainqueur = B');

  B.send({ type: 'pos', mapId: 18, x: 4, y: 4, dir: 2, progress: 100 });
  A.send({ type: 'pos', mapId: 18, x: 1, y: 1, dir: 2, progress: 100 });
  await sleep(100);
  ok(count(A.inbox, 'race_end') === 1 && count(B.inbox, 'race_end') === 1, 'race_end émis une seule fois');

  console.log('— Départ en jeu —');
  A.inbox.length = 0;
  B.send({ type: 'leave_lobby' });
  await sleep(100);
  ok(last(A.inbox, 'peer_left'), 'A reçoit peer_left quand B quitte la partie');

  console.log('— Dissolution par owner —');
  // Nouveau lobby : B rejoint, on démarre, puis A (owner) déconnecte brutalement
  A.send({ type: 'leave_lobby' });
  await sleep(100);
  A.send({ type: 'create_lobby' });
  await sleep(100);
  const CODE2 = last(A.inbox, 'lobby_created').lobbyId;
  B.send({ type: 'join_lobby', lobbyId: CODE2 });
  A.send({ type: 'ready' }); B.send({ type: 'ready' });
  await sleep(100);
  A.send({ type: 'start' });
  await sleep(100);
  B.inbox.length = 0;
  A.ws.terminate(); // crash réseau de l'owner
  await sleep(200);
  ok(last(B.inbox, 'lobby_dissolved'), 'B reçoit lobby_dissolved quand owner déconnecte en jeu');

  console.log('— Reconnexion même playerId —');
  B.send({ type: 'leave_lobby' });
  const A2 = await mkClient('player-B'); // même id que B → l'ancien socket doit être coupé
  await sleep(150);
  A2.send({ type: 'create_lobby' });
  await sleep(150);
  ok(last(A2.inbox, 'lobby_created'), 'nouveau socket actif après reconnexion même playerId');
  ok(B.ws.readyState === WebSocket.CLOSED || B.ws.readyState === WebSocket.CLOSING, 'ancien socket terminé');

  console.log('— Ping —');
  A2.send({ type: 'ping' });
  await sleep(100);
  ok(A2.ws.readyState === WebSocket.OPEN, 'ping accepté sans fermer la connexion');

  console.log('\nRésultat :', passed, 'PASS /', failed, 'FAIL');
  process.exit(failed ? 1 : 0);
})().catch(e => { console.error(e); process.exit(1); });
