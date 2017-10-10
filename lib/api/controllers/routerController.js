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
  HttpRouter = require('../core/httpRouter'),
  PluginImplementationError = require('kuzzle-common-objects').errors.PluginImplementationError,
  generateSwagger = require('../core/swagger'),
  jsonToYaml = require('json2yaml');

/**
 * @class RouterController
 * @property action
 * @param {Kuzzle} kuzzle
 */
class RouterController {
  constructor(kuzzle) {
    this.kuzzle = kuzzle;
    this.connections = {};
    this.http = new HttpRouter(kuzzle);
  }

  /**
   * Declares a new connection on a given protocol. Called by proxyBroker.
   *
   * @param {RequestContext} requestContext
   */
  newConnection(requestContext) {
    if (!requestContext.connectionId || !requestContext.protocol) {
      this.kuzzle.pluginsManager.trigger('log:error', new PluginImplementationError('Rejected new connection - invalid arguments:' + JSON.stringify(requestContext)));
    }
    else {
      this.connections[requestContext.connectionId] = requestContext;

      this.kuzzle.statistics.newConnection(this.connections[requestContext.connectionId]);
    }
  }

  /**
   * Called by proxyBroker: removes a connection from the connection pool.
   *
   * @param {RequestContext} requestContext
   */
  removeConnection(requestContext) {
    if (!requestContext.connectionId || !requestContext.protocol) {
      return this.kuzzle.pluginsManager.trigger('log:error', new PluginImplementationError('Unable to remove connection - invalid arguments:' + JSON.stringify(requestContext.context)));
    }

    if (!this.connections[requestContext.connectionId]) {
      return this.kuzzle.pluginsManager.trigger('log:error', new PluginImplementationError('Unable to remove connection - unknown connectionId:' + JSON.stringify(requestContext.connectionId)));
    }

    delete this.connections[requestContext.connectionId];

    this.kuzzle.hotelClerk.removeCustomerFromAllRooms(requestContext);

    this.kuzzle.statistics.dropConnection(requestContext);
  }

  /**
   * Check that the provided connectionId executing is still alive
   * 
   * @param  {RequestContext} requestContext
   */
  isConnectionAlive(requestContext) {
    // We only care about connections that stay open
    if (requestContext.protocol === 'http') {
      return true;
    }

    return this.connections[requestContext.connectionId] !== undefined;
  }

  /**
   * Initializes the HTTP routes for the Kuzzle HTTP API.
   */
  init() {
    // create and mount a new router for plugins
    for (const route of this.kuzzle.pluginsManager.routes) {
      this.http.attach(route.verb, `/_plugin/${route.url}`, (request, cb) => this._executeHttp(route, request, cb));
    }

    this.http.attach('GET', '/swagger.json', (request, cb) => {
      request.setResult(generateSwagger(this.kuzzle), {
        status: 200,
        raw: true,
        headers: {'content-type': 'application/json'}
      });

      cb(request);
    });

    this.http.attach('GET', '/swagger.yml', (request, cb) => {
      request.setResult(jsonToYaml.stringify(generateSwagger(this.kuzzle)), {
        status: 200,
        raw: true,
        headers: {'content-type': 'application/yaml'}
      });

      cb(request);
    });

    // Register API routes
    for (const route of this.kuzzle.config.http.routes) {
      this.http.attach(route.verb, route.url, (request, cb) => this._executeHttp(route, request, cb));
    }
  }

  _executeHttp (route, request, cb) {
    request.input.controller = route.controller;
    request.input.action = route.action;

    this.kuzzle.funnel.execute(request, (err, result) => cb(result));
  }
}

/**
 * @type {RouterController}
 */
module.exports = RouterController;
