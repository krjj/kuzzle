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
  debug = require('../../../../../kuzzleDebug')('kuzzle:entry-point:protocols:http'),
  bytes = require('bytes'),
  url = require('url'),
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

    for (const value of ['maxFormFileSize', 'maxEncodingLayers']) {
      assert(
        this[value] !== null && !isNaN(this[value]),
        `Invalid HTTP "${value}" parameter value: expected a numeric value, got ${this[value]}`);
    }

    this.decoders = this._setDecoders();

    this.server.any('/*', this.processHttpRequest.bind(this));
  }

  /**
   * Processes a HTTP request
   *
   * @param  {HttpResponse} httpResponse - uWebSockets response object
   * @param  {HttpRequest} httpRequest   - uWebSockets request object
   */
  processHttpRequest(httpResponse, httpRequest) {
    const
      ips = this.getIps(httpResponse, httpRequest),
      headers = {};

    httpRequest.forEach((key, value) => {
      headers[key] = value;
    });

    const
      connection = new ClientConnection('HTTP/1.1', ips, headers),
      // emulates the (deprecated) kuzzle proxy http query format
      proxyRequest = {
        ips,
        headers,
        requestId: connection.id,
        url: httpRequest.getUrl(),
        method: httpRequest.getMethod(),
        content: ''
      };

    debug('[%s] receiving HTTP request: %a', connection.id, proxyRequest);
    this.entryPoint.newConnection(connection);

    if (headers['content-length'] > this.maxRequestSize) {
      this._replyWithError(
        connection,
        proxyRequest,
        httpResponse,
        new SizeLimitError('Maximum HTTP request size exceeded'));
      return;
    }

    let stream;

    if (
      !headers['content-type'] ||
      headers['content-type'].startsWith('application/json')
    ) {
      stream = this._createWriteStream(connection, proxyRequest);
    } else {
      try {
        stream = new HttpFormDataStream({
          headers,
          limits: {fileSize: this.maxFormFileSize}
        }, proxyRequest, httpRequest);
      } catch (error) {
        this._replyWithError(
          connection,
          proxyRequest,
          httpResponse,
          new BadRequestError(error));
        return;
      }
    }

    const reader = this._wrapToReadableStream(httpResponse);
    let pipes;

    try {
      pipes = this._uncompress(reader, headers);
    } catch(err) {
      return this._replyWithError(
        connection, proxyRequest, httpResponse, err);
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

      this._replyWithError(connection, proxyRequest, httpResponse, kerr);
      return;
    });

    stream.on('finish', () => {
      debug('[%s] End Request', connection.id);
      proxyRequest.headers['content-type'] = 'application/json';
      this._sendRequest(connection, httpResponse, proxyRequest);
    });
  }

  /**
   * Send a request to Kuzzle and forwards the response back to the client
   *
   * @param {ClientConnection} connection
   * @param {HttpResponse} response - uWebSockets response object
   * @param {Object} proxyRequest
   */
  _sendRequest (connection, response, proxyRequest) {
    debug('[%s] sendRequest: %a', connection.id, proxyRequest);

    if (proxyRequest.json) {
      proxyRequest.content = JSON.stringify(proxyRequest.json);
    }

    this.entryPoint.kuzzle.router.http.route(proxyRequest, request => {
      this.entryPoint.logAccess(request, proxyRequest);
      const data = this._getResponsePayload(request, proxyRequest);

      debug('sending HTTP request response to client: %a', data);

      this._compress(data, request, proxyRequest.headers, (err, buf) => {
        if (err) {
          const kuzerr = err instanceof KuzzleError ?
            err : new BadRequestError(err);

          this._replyWithError(connection, proxyRequest, response, kuzerr);
          return;
        }

        response.writeStatus(request.status.toString());

        for (const header of Object.keys(request.response.headers)) {
          response.writeHeader(header, request.response.headers[header]);
        }

        response.end(buf);
        this.entryPoint.removeConnection(connection.id);
      });
    });
  }

  /**
   * Forward an error response to the client
   *
   * @param {ClientConnection} connection
   * @param {Object} proxyRequest
   * @param {HttpResponse} response - uWebSockets HttpResponse object
   * @param {KuzzleError} error
   */
  _replyWithError(connection, proxyRequest, response, error) {
    const result = {
      raw: true,
      content: JSON.stringify(error)
    };

    debug('[%s] replyWithError: %a', connection.id, error);

    this.entryPoint.logAccess(
      new Request(proxyRequest, {error, connectionId: connection.id}),
      proxyRequest);

    response.writeStatus(`${error.status} ${error.constructor.name}`);

    response.writeHeader('Content-Type', 'application/json');
    response.writeHeader('Access-Control-Allow-Origin', '*');
    response.writeHeader('Access-Control-Allow-Methods', defaultAllowedMethods);
    response.writeHeader('Access-Control-Allow-Headers', defaultAllowedHeaders);

    response.end(result.content);

    this.entryPoint.removeConnection(connection.id);
  }

  /**
   * Convert a Kuzzle query result into an appropriate payload format
   * to send back to the client
   *
   * @param {Request} request
   * @param {Object} proxyRequest
   * @return {Buffer}
   */
  _getResponsePayload(request, proxyRequest) {
    let data = request.response.toJSON();

    if (proxyRequest.requestId !== data.requestId) {
      data.requestId = proxyRequest.requestId;

      if (!data.raw) {
        data.content.requestId = proxyRequest.requestId;
      }
    }

    if (data.raw) {
      if (typeof data.content === 'object') {
        /*
         This object can be either a Buffer object, a stringified Buffer object,
         or anything else.
         In the former two cases, we create a new Buffer object, and in the
         latter, we stringify the content.
         */
        if (data.content instanceof Buffer ||
          (data.content.type === 'Buffer' && Array.isArray(data.content.data))
        ) {
          data = data.content;
        } else {
          data = JSON.stringify(data.content);
        }
      } else {
        // scalars are sent as-is
        data = data.content;
      }
    } else {
      let indent = 0;
      const parsedUrl = url.parse(proxyRequest.url, true);

      if (parsedUrl.query && parsedUrl.query.pretty !== undefined) {
        indent = 2;
      }

      data = JSON.stringify(data.content, undefined, indent);
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
   * @param  {Object} headers - HTTP request headers
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
  _wrapToReadableStream(response) {
    const reader = new Readable({
      read: () => true
    });

    response.onAborted(() => {
      // do nothing
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
   * @param  {ClientConnection} connection
   * @param  {Object} proxyRequest
   * @return {stream.Writable}
   */
  _createWriteStream(connection, proxyRequest) {
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
          callback(new SizeLimitError('Maximum HTTP request size exceeded'));
          return;
        }

        const str = chunk.toString();

        debug('[%s] writing chunk: %a', connection.id, str);
        proxyRequest.content += str;
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
   * @param  {Buffer}   data     - data to compress
   * @param  {Request}  request
   * @param  {Object}   headers  - HTTP request headers
   * @param  {Function} callback
   */
  _compress(data, request, headers, callback) {
    if (headers && headers['accept-encoding']) {
      const allowedEncodings = new Set(headers['accept-encoding']
        .split(',')
        .map(e => e.trim().toLowerCase()));

      // gzip should be preferred over deflate
      if (allowedEncodings.has('gzip')) {
        request.response.setHeader('Content-Encoding', 'gzip');
        zlib.gzip(data, callback);
        return;
      } else if (allowedEncodings.has('deflate')) {
        request.response.setHeader('Content-Encoding', 'deflate');
        zlib.deflate(data, callback);
        return;
      }
    }

    callback(null, data);
  }
}

module.exports = HttpProtocol;
