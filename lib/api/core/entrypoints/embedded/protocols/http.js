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
  debug = require('../../../../../kuzzleDebug')('kuzzle:entry-point:protocols:http'),
  bytes = require('bytes'),
  url = require('url'),
  ipaddr = require('ipaddr.js'),
  ClientConnection = require('../clientConnection'),
  Protocol = require('./protocol'),
  { Readable, Writable } = require('stream'),
  HttpFormDataStream = require('../service/httpFormDataStream'),
  {
    Request,
    errors: {
      KuzzleError,
      BadRequestError,
      SizeLimitError
    }
  } = require('kuzzle-common-objects'),
  zlib = require('zlib');

const
  defaultAllowedMethods = [
    'GET',
    'POST',
    'PUT',
    'PATCH',
    'DELETE',
    'HEAD',
    'OPTIONS'
  ].join(','),
  defaultAllowedHeaders = [
    'Content-Type',
    'Access-Control-Allow-Headers',
    'Authorization',
    'X-Requested-With',
    'Content-Length',
    'Content-Encoding',
    'X-Kuzzle-Volatile'
  ].join(',');

/**
 * @class HttpProtocol
 */
class HttpProtocol extends Protocol {
  constructor() {
    super();

    this.maxFormFileSize = 0;
    this.server = null;
    this.maxEncodingLayers = 0;
    this.decoders = {};
  }

  /**
   * Initialize the HTTP server
   *
   * @param {EmbeddedEntryPoint} entryPoint
   */
  init(entryPoint) {
    super.init(entryPoint);

    debug('initializing http Server with config: %a', entryPoint.config);

    this.maxFormFileSize = bytes.parse(
      entryPoint.config.protocols.http.maxFormFileSize);
    this.maxEncodingLayers = entryPoint.config.protocols.http.maxEncodingLayers;
    this.server = entryPoint.server;

    for (const numericParameter of ['maxFormFileSize', 'maxEncodingLayers']) {
      if (this[numericParameter] === null || isNaN(this[numericParameter])) {
        throw new Error(`Invalid HTTP "${numericParameter}" parameter value: expected a numeric value`);
      }
    }

    this.decoders = this._setDecoders();

    this.server.any('/*', (httpConnection, request) => {
      const
        ipbuf = new Uint8Array(httpConnection.getRemoteAddress()),
        ips = [ipaddr.fromByteArray(ipbuf).toString()],
        headers = {};

      request.forEach((key, value) => {
        headers[key] = value;
      });

      if (typeof headers['x-forwarded-for'] === 'string') {
        ips.push(...request.getHeader('x-forwarded-for')
          .split(',').map(s => s.trim()).filter(s => s.filter()));
      }

      const
        connection = new ClientConnection('HTTP/1.0', ips, headers),
        payload = {
          ips,
          headers,
          requestId: connection.id,
          url: request.getUrl(),
          method: request.getMethod(),
          content: ''
        };

      debug('[%s] receiving HTTP request: %a', connection.id, payload);
      this.entryPoint.newConnection(connection);

      if (headers['content-length'] > this.maxRequestSize) {
        return this._replyWithError(
          connection.id,
          payload,
          httpConnection,
          new SizeLimitError('Maximum HTTP request size exceeded'));
      }

      let stream;

      if (
        !headers['content-type'] ||
        headers['content-type'].startsWith('application/json')
      ) {
        stream = this._createWriteStream(connection.id, payload);
      } else {
        try {
          stream = new HttpFormDataStream({
            headers,
            limits: {fileSize: this.maxFormFileSize}
          }, payload, request);
        } catch (error) {
          return this._replyWithError(
            connection.id,
            payload,
            httpConnection,
            new BadRequestError(error));
        }
      }

      const reader = this._wrapToReadableStream(httpConnection);
      let pipes;

      try {
        pipes = this._uncompress(reader, headers);
      } catch(err) {
        return this._replyWithError(
          connection.id, payload, httpConnection, err);
      }

      // We attach our writable stream to the last pipe of the chain
      if (pipes.length > 0) {
        pipes[pipes.length-1].pipe(stream);
      } else {
        reader.pipe(stream);
      }

      // We forwarded all pipe errors to the request's event handler
      reader.on('error', err => {
        const kerr =
          err instanceof KuzzleError ? err : new BadRequestError(err);

        // remove all pipes before flushing the stream
        reader.unpipe();
        reader.removeAllListeners().resume();
        stream.removeAllListeners().end();

        // When an error occurs on a Readable Stream, the
        // registered pipes are NOT freed automatically
        pipes.forEach(pipe => pipe.close());

        return this._replyWithError(
          connection.id, payload, httpConnection, kerr);
      });

      stream.on('finish', () => {
        debug('[%s] End Request', connection.id);
        payload.headers['content-type'] = 'application/json';
        this._sendRequest(connection.id, httpConnection, payload);
      });
    });
  }

  /**
   * Send a request to Kuzzle and forwards the response back to the client
   *
   * @param {string} connectionId - connection Id
   * @param {HttpResponse} response
   * @param {Object} payload
   */
  _sendRequest (connectionId, response, payload) {
    debug('[%s] sendRequest: %a', connectionId, payload);

    if (payload.json) {
      payload.content = JSON.stringify(payload.json);
    }

    this.entryPoint.kuzzle.router.http.route(payload, result => {
      this.entryPoint.logAccess(result, payload);
      const resp = result.response.toJSON();

      if (payload.requestId !== resp.requestId) {
        resp.requestId = payload.requestId;

        if (!resp.raw) {
          resp.content.requestId = payload.requestId;
        }
      }

      debug('sending HTTP request response to client: %a', resp);

      if (resp.headers) {
        for (const header of Object.keys(resp.headers)) {
          response.writeHeader(header, resp.headers[header]);
        }
      }

      const data = this._contentToPayload(resp, payload.url);

      this._compress(data, response, payload.headers, (err, buf) => {
        return this._replyWithError(connectionId, payload, response, new BadRequestError('foo'));
        if (err) {
          const kuzerr = err instanceof KuzzleError ? err : new BadRequestError(err);
          return this._replyWithError(connectionId, payload, response, kuzerr);
        }

        // response.writeStatus(resp.status.toString());
        console.log('...writing status...')
        response.writeStatus('400 BadRequestError');
        response.end(buf);
        this.entryPoint.removeConnection(connectionId);
      });
    });
  }

  /**
   * Forward an error response to the client
   *
   * @param {string} connectionId
   * @param {Object} payload
   * @param {HttpResponse} response - uWebSockets HttpResponse object
   * @param {Object} error
   */
  _replyWithError(connectionId, payload, response, error) {
    const result = {
      raw: true,
      content: JSON.stringify(error)
    };

    debug('[%s] replyWithError: %a', connectionId, error);

    this.entryPoint.logAccess(
      new Request(payload, {error, connectionId}),
      payload);

    response.writeStatus(`${error.status} ${error.constructor.name}`);

    response.writeHeader('Content-Type', 'application/json');
    response.writeHeader('Access-Control-Allow-Origin', '*');
    response.writeHeader('Access-Control-Allow-Methods', defaultAllowedMethods);
    response.writeHeader('Access-Control-Allow-Headers', defaultAllowedHeaders);

    response.end(result.content);

    this.entryPoint.removeConnection(connectionId);
  }

  /**
   * Convert a Kuzzle query result into an appropriate payload format
   * to send back to the client
   *
   * @param {Object} result
   * @param {String} invokedUrl - invoked URL. Used to check if the ?pretty
   *                              argument was passed by the client, changing
   *                              the payload format
   * @return {Buffer}
   */
  _contentToPayload(result, invokedUrl) {
    let data;

    if (result.raw) {
      if (typeof result.content === 'object') {
        /*
         This object can be either a Buffer object, a stringified Buffer object,
         or anything else.
         In the former two cases, we create a new Buffer object, and in the latter,
         we stringify the content.
         */
        if (result.content instanceof Buffer || (result.content.type === 'Buffer' && Array.isArray(result.content.data))) {
          data = result.content;
        }
        else {
          data = JSON.stringify(result.content);
        }
      }
      else {
        // scalars are sent as-is
        data = result.content;
      }
    }
    else {
      let indent = 0;
      const parsedUrl = url.parse(invokedUrl, true);

      if (parsedUrl.query && parsedUrl.query.pretty !== undefined) {
        indent = 2;
      }

      data = JSON.stringify(result.content, undefined, indent);
    }

    return Buffer.from(data);
  }

  /**
   * Chain decoders streams to the one provided in the arguments, and
   * return an array of created streams, in the same order than the chain.
   *
   * If no decoder is needed, returns an empty array.
   *
   * @param  {stream.Readable} reader - data stream
   * @param  {Object} headers - request
   * @return {Array.<stream.Readable>}
   * @throws {BadRequestError} If invalid compression algorithm is set
   *                           or if the value does not comply to the
   *                           way the Kuzzle server is configured
   */
  _uncompress(reader, headers) {
    const pipes = [];

    if (headers['content-encoding']) {
      const encodings = headers['content-encoding']
        .split(',')
        .map(e => e.trim().toLowerCase());

      // encodings are listed in the same order they have been applied
      // this means that we need to invert the list to correctly
      // decode the message
      encodings.reverse();

      if (encodings.length > this.maxEncodingLayers) {
        throw new BadRequestError('Too many encodings');
      }

      for (const encoding of encodings) {
        let pipe;

        if (this.decoders[encoding]) {
          pipe = this.decoders[encoding]();
        } else {
          reader.removeAllListeners().resume();
          throw new BadRequestError(`Unsupported compression algorithm "${encoding}"`);
        }

        if (pipe) {
          const lastPipe = pipes.length > 0 ? pipes[pipes.length-1] : reader;
          lastPipe.pipe(pipe);
          pipes.push(pipe);

          // forward zlib errors to the request global error handler
          pipe.on('error', error => reader.emit('error', error));
        }
      }
    }

    return pipes;
  }

  /**
   * Create a stream.Readable object from a uWebSockets response
   * @param  {HttpResponse} response - uWebSockets response
   * @return {stream.Readable}
   */
  _wrapToReadableStream(response, request) {
    const reader = new Readable({
      read: () => true
    });

    response.onAborted(() => {
      reader.emit('error', new Error('Aborted'));
    });

    response.onData((chunk, isLast) => {
      if (chunk.byteLength > 0) {
        reader.push(new Uint8Array(chunk));
      }

      if (isLast) {
        reader.push(null); // signals an EOF
      }
    });

    return reader;
  }

  /**
   * Create a new Writable stream configured to receive a HTTP request
   * @param  {string} connectionId
   * @param  {Object} payload
   * @return {stream.Writable}
   */
  _createWriteStream(connectionId, payload) {
    const maxRequestSize = this.maxRequestSize; // prevent context mismatch
    let streamLength = 0;

    return new Writable({
      write(chunk, encoding, callback) {
        /*
         * The content-length header can be bypassed and
         * is not reliable enough. We have to enforce the HTTP
         * max size limit while reading the stream too
         */
        streamLength += chunk.length;

        if (streamLength > maxRequestSize) {
          return callback(new SizeLimitError('Maximum HTTP request size exceeded'));
        }

        const str = chunk.toString();

        debug('[%s] writing chunk: %a', connectionId, str);
        payload.content += str;
        callback();
      }
    });
  }

  /**
   * Initialize the decoders property according to the current
   * server configuration
   * @return {Objet} A set of all supported decoders
   */
  _setDecoders() {
    const
      allowCompression = this.entryPoint.config.protocols.http.allowCompression,
      disabledfn = () => {
        throw new BadRequestError('Compression support is disabled');
      };

    if (typeof allowCompression !== 'boolean') {
      throw new Error('Invalid HTTP "allowCompression" parameter value: expected a boolean value');
    }

    // for now, we accept gzip, deflate and identity
    const decoders = {};

    decoders.gzip = allowCompression ? zlib.createGunzip : disabledfn;
    decoders.deflate = allowCompression ? zlib.createInflate : disabledfn;
    decoders.identity = () => null;

    return decoders;
  }

  /**
   * Compress an outgoing message according to the
   * specified accept-encoding HTTP header
   *
   * @param  {Buffer} data     - data to compress
   * @param  {ServerResponse} response
   * @param  {Object} headers
   * @param  {Function} callback
   */
  _compress(data, response, headers, callback) {
    if (headers && headers['accept-encoding']) {
      const allowedEncodings = new Set(headers['accept-encoding']
        .split(',')
        .map(e => e.trim().toLowerCase()));

      // gzip should be preferred over deflate
      if (allowedEncodings.has('gzip')) {
        response.writeHeader('Content-Encoding', 'gzip');
        return zlib.gzip(data, callback);
      } else if (allowedEncodings.has('deflate')) {
        response.writeHeader('Content-Encoding', 'deflate');
        return zlib.deflate(data, callback);
      }
    }

    callback(null, data);
  }
}

module.exports = HttpProtocol;
