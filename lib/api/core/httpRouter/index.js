/*
 * Kuzzle, a backend software, self-hostable and ready to use
 * to power modern apps
 *
 * Copyright 2015-2017 Kuzzle
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
  RoutePart = require('./routePart'),
  Request = require('kuzzle-common-objects').Request,
  {
    InternalError: KuzzleInternalError,
    BadRequestError,
    NotFoundError
  } = require('kuzzle-common-objects').errors,
  URL = require('url');

const
  LeadingSlashRegex = /\/+$/,
  CharsetRegex = /charset=([\w-]+)/i,
  TrimSlashesRegex = /^\/+(.*)(\/+)?/;

/**
 * Attach handler to routes and dispatch a HTTP
 * message to the right handler
 *
 * Handlers will be called with the following arguments:
 *   - request: received HTTP request
 *   - response: HTTP response object
 *   - data: URL query arguments and/or POST data, if any
 *
 * @class Router
 */
class Router {
  constructor(kuzzle) {
    this.kuzzle = kuzzle;

    this.defaultHeaders = {
      'content-type': 'application/json',
      'Access-Control-Allow-Origin': kuzzle.config.http.accessControlAllowOrigin || '*',
      'Access-Control-Allow-Methods': 'GET,POST,PUT,DELETE,OPTIONS,HEAD',
      'Access-Control-Allow-Headers': 'Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With'
    };

    this.routes = {
      GET: new RoutePart(),
      POST: new RoutePart(),
      PUT: new RoutePart(),
      DELETE: new RoutePart(),
      HEAD: new RoutePart()
    };

    // Add an automatic HEAD route on the '/' url, answering with default headers
    this.attach('HEAD', '/', (request, cb) => {
      Object.assign(request.input.args, this.defaultHeaders);
      request.setResult({}, 200);
      cb(request);
    });
  }

  attach (method, url, handler) {
    const rootKey = method.toUpperCase();

    if (!this.routes[rootKey]) {
      this.routes[rootKey] = new RoutePart();
    }

    let node = this.routes[rootKey];

    const trimedUrl = url.trim().replace(TrimSlashesRegex, '$1');
    for (const part of trimedUrl.split(/\/+/)) {
      // the tree is built here
      node = node.buildNext(part);
    }

    if (node.handler !== null) {
      throw new KuzzleInternalError(`Unable to attach URL ${url}: URL path already exists`);
    }
    node.handler = handler;
  }

  /**
   * Route an incoming HTTP httpRequest to the right handler
   *
   * @param {object} httpRequest - HTTP httpRequest formatted by Kuzzle Proxy
   * @param {function} cb
   */
  route (httpRequest, cb) {
    if (!this.routes[httpRequest.method]) {
      const request = new Request({requestId: httpRequest.requestId}, {}, 'rest');
      request.response.setHeaders(this.defaultHeaders);

      if (httpRequest.method.toUpperCase() === 'OPTIONS') {
        Object.assign(request.input.args, httpRequest.headers);
        request.setResult({}, 200);

        return this.kuzzle.pluginsManager.trigger('http:options', request)
          .then(result => {
            cb(result);
            return null;
          })
          .catch(error => replyWithError(cb, request, error));
      }

      return replyWithError(cb, request, new BadRequestError(`Unrecognized HTTP method ${httpRequest.method}`));
    }

    httpRequest.url = httpRequest.url.replace(LeadingSlashRegex, '');

    const routeHandler = this.routes[httpRequest.method].getHandler(httpRequest);
    routeHandler.getRequest().response.setHeaders(this.defaultHeaders);

    if (routeHandler.handler === null) {
      return replyWithError(cb, routeHandler.getRequest(), new NotFoundError(`API URL not found: ${routeHandler.url}`));
    }

    if (httpRequest.content.length <= 0) {
      return routeHandler.invokeHandler(cb);
    }

    if (httpRequest.headers['content-type']
      && !httpRequest.headers['content-type'].startsWith('application/json')) {
      return replyWithError(cb, routeHandler.getRequest(), new BadRequestError(`Invalid request content-type. Expected "application/json", got: "${httpRequest.headers['content-type']}"`));
    }

    {
      const encoding = CharsetRegex.exec(httpRequest.headers['content-type']);

      if (encoding !== null && encoding[1].toLowerCase() !== 'utf-8') {
        return replyWithError(cb, routeHandler.getRequest(), new BadRequestError(`Invalid request charset. Expected "utf-8", got: "${encoding[1].toLowerCase()}"`));
      }
    }

    try {
      routeHandler.addContent(httpRequest.content);
      routeHandler.invokeHandler(cb);
    }
    catch (e) {
      replyWithError(cb, routeHandler.getRequest(), new BadRequestError('Unable to convert HTTP body to JSON'));
    }
  }

  /**
   * @param {IncomingMessage} request
   * @private
   */
  _getRequestHandler (request) {
    const
      rootNode = this.routes[request.method.toUpperCase()],
      parsed = URL.parse(request.url),
      pathname = parsed.pathname || '';

    if (!rootNode) {
      return null;
    }

    const parts = pathname
      .trim()
      .replace(TrimSlashesRegex, '$1')
      .split(/\/+/);

    return rootNode.getHandler(parts);
  }
}


/**
 * Reply to a callback function with an HTTP error
 *
 * @param {function} cb
 * @param {Request} request
 * @param {Error} error
 */
function replyWithError(cb, request, error) {
  request.setError(error);

  cb(request);
}

/**
 * @type {Router}
 */
module.exports = Router;
