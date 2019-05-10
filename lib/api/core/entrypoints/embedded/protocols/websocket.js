/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2018 Kuzzle
 * mailto: support AT kuzzle.io
 * website: http://kuzzle.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

const
  assert = require('assert'),
  debug = require('../../../../../kuzzleDebug')('kuzzle:entry-point:protocols:websocket'),
  Protocol = require('./protocol'),
  Request = require('kuzzle-common-objects').Request,
  ClientConnection = require('../clientConnection'),
  {
    BadRequestError,
    InternalError: KuzzleInternalError
  } = require('kuzzle-common-objects').errors;

/**
 * @class WebsocketProtocol
 */
class WebsocketProtocol extends Protocol {
  constructor () {
    super();

    this.channels = new Map();
    this.connectionPool = new Map();
    this.server = null;
    this.protocol = 'websocket';
    this.kuzzle = null;
    this.heartbeatInterval = null;
  }

  /**
   *
   * @param {EmbeddedEntryPoint} entryPoint
   */
  init (entryPoint) {
    const wsConfig = entryPoint.config.protocols.websocket;
    if (!wsConfig.enabled) {
      return;
    }

    super.init(entryPoint);

    debug('initializing WebSocket Server with config: %a', wsConfig);

    assert(
      Number.isInteger(wsConfig.heartbeat) && wsConfig.heartbeat >= 0,
      `WebSocket: invalid heartbeat value: ${wsConfig.heartbeat}`);

    assert(
      Number.isInteger(wsConfig.idleTimeout) && wsConfig.idleTimeout >= 0,
      `WebSocket: invalid idle timeout value: ${wsConfig.idleTimeout}`);

    assert(
      typeof(wsConfig.compression) === 'boolean',
      `WebSocket: invalid compression value: ${wsConfig.compression}`);

    if (wsConfig.heartbeat > 0) {
      this.heartbeatInterval = setInterval(
        this._doHeartbeat.bind(this),
        wsConfig.heartbeat);
    }

    this.kuzzle = this.entryPoint.kuzzle;
    this.server = this.entryPoint.server;

    this.server.ws('/*', {
      maxPayloadLength: this.maxRequestSize,
      idleTimeout: wsConfig.idleTimeout,
      compression: wsConfig.compression ? 1 : 0,
      open: this.onOpen.bind(this),
      pong: this.onPong.bind(this),
      message: this.onMessage.bind(this),
      close: this.onClose.bind(this)
    });
  }

  /**
   * Connection request
   * @param  {WebSocket} ws
   * @param  {HttpRequest} request
   */
  onOpen(ws, request) {
    const
      ips = this.getIps(ws, request),
      headers = {};

    request.forEach((key, value) => {
      headers[key] = value;
    });

    const connection = new ClientConnection(this.protocol, ips, headers);
    debug('[%s] Creating Websocket connection from %o', connection.id, ips);

    try {
      this.entryPoint.newConnection(connection);
    }
    catch (err) {
      this.entryPoint.log.warn(
        '[websocket] Client connection refused with message "%s": initialization still underway',
        err.message);
      ws.end(1013, err.message);
      return;
    }

    // Attach the ClientConnection object to the WebSocket one for easy & fast
    // retrieval (µWebSockets guarantees that to work until after the "close"
    // event is emitted for that socket)
    ws.kuzzleConnection = connection;

    this.connectionPool.set(connection.id, {
      alive: true,
      socket: ws,
      channels: []
    });
  }

  onClose(ws, code, message) {
    this.entryPoint.removeConnection(ws.kuzzleConnection.id);

    const poolConnection = this.connectionPool.get(ws.kuzzleConnection.id);

    if (!poolConnection) {
      return;
    }

    debug(
      '[%s] WebSocket closed with code %d (message: "%s")',
      poolConnection.id,
      code,
      Buffer.from(message).toString());

    this.connectionPool.delete(poolConnection.id);
    poolConnection.alive = false;

    for (const channel of poolConnection.channels) {
      const ids = this.channels.get(channel);

      if (ids) {
        ids.delete(poolConnection.id);

        if (ids.size === 0) {
          this.channels.delete(channel);
        }
      }
    }
  }

  onPong(ws) {
    debug('[%s] received a `pong` event', ws.kuzzleConnection.id);

    const connection = this.connectionPool.get(ws.kuzzleConnection.id);

    if (connection) {
      connection.alive = true;
    }
  }

  onMessage(ws, data) {
    const id = ws.kuzzleConnection.id;

    if (data.byteLength === 0 || !this.connectionPool.has(id)) {
      return;
    }

    let parsed;

    debug('[%s] onClientMessage: %a', id, data);

    try {
      parsed = JSON.parse(Buffer.from(data));
    }
    catch (e) {
      /*
       we cannot add a "room" information since we need to extract
       a request ID from the incoming data, which is apparently
       not a valid JSON
       So... the error is forwarded as-is to the client, hoping they know
       what to do with it.
       */
      this._send(id, JSON.stringify(new BadRequestError(e.message)));
      return;
    }

    try {
      const request = new Request(parsed, {connection: ws.kuzzleConnection});

      this.entryPoint.execute(request, result => {
        if (result.content && typeof result.content === 'object') {
          result.content.room = result.requestId;
        }

        this._send(id, JSON.stringify(result.content));
      });
    }
    catch (e) {
      const errobj = {
        room: parsed.requestId,
        status: 400,
        error: {
          message: e.message
        }
      };

      this.entryPoint.kuzzle.pluginsManager.trigger(
        'log:error', new KuzzleInternalError(e.message));

      return this._send(id, JSON.stringify(errobj));
    }
  }

  broadcast(data) {
    /*
     Avoids stringifying the payload multiple times just to update the room:
     - we start deleting the last character, which is the closing JSON
       bracket ('}')
     - we then only have to inject the following string to each channel:
       ,"room":"<roomID>"}

     So, instead of stringifying the payload for each channel, we only concat
     a new substring to the original payload.
     */
    const payload = JSON.stringify(data.payload).slice(0, -1) + ',"room":"';

    debug('broadcast: %a', data);

    for (const channelId of data.channels) {
      const ids = this.channels.get(channelId);
      if (ids) {
        const channelPayload = payload + channelId + '"}';

        for (const connectionId of ids) {
          this._send(connectionId, channelPayload);
        }
      }
    }
  }

  notify(data) {
    const payload = data.payload;

    debug('notify: %a', data);

    data.channels.forEach(channel => {
      payload.room = channel;
      this._send(data.connectionId, JSON.stringify(payload));
    });
  }

  joinChannel(channel, connectionId) {
    debug('joinChannel: %s %s', channel, connectionId);

    const connection = this.connectionPool.get(connectionId);

    if (!connection || !connection.alive) {
      return;
    }

    let ids = this.channels.get(channel);

    if (!ids) {
      ids = new Set();
      this.channels.set(channel, ids);
    }

    ids.add(connectionId);
    connection.channels.push(channel);
  }

  leaveChannel(channel, connectionId) {
    debug('leaveChannel: %s %s', channel, connectionId);

    const
      connection = this.connectionPool.get(connectionId),
      ids = this.channels.get(channel);

    if (!connection || !ids) {
      return;
    }

    ids.delete(connectionId);

    if (ids.size === 0) {
      this.channels.delete(channel);
    }

    const index = connection.channels.indexOf(channel);
    if (index !== -1) {
      connection.channels.splice(index, 1);
    }
  }

  disconnect(clientId, message = 'Connection closed by remote host') {
    debug('[%s] disconnect', clientId);

    const connection = this.connectionPool.get(clientId);
    if (!connection) {
      connection.alive = false;
      connection.socket.end(1011, message);
    }
  }

  _send (id, data) {
    debug('[%s] send: %a', id, data);

    const connection = this.connectionPool.get(id);

    if (connection && connection.alive) {
      connection.socket.send(data, false);
    }
  }

  _doHeartbeat() {
    debug('[WebSocket] Heartbeat');
    for (const connection of this.connectionPool.values()) {
      // did not respond since we last sent a PING request
      if (connection.alive === false) {
        // correctly triggers the 'close' event handler on that socket
        connection.socket.close();
      } else {
        connection.alive = false;
        connection.socket.ping();
      }
    }
  }
}

module.exports = WebsocketProtocol;
