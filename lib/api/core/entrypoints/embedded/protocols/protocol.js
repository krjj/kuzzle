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
const
  assert = require('assert'),
  bytes = require('bytes'),
  ipaddr = require('ipaddr.js');

class Protocol {
  constructor() {
    this.maxRequestSize = null;
    this.entryPoint = null;
  }

  init(entryPoint) {
    this.entryPoint = entryPoint;
    this.maxRequestSize = bytes.parse(entryPoint.config.maxRequestSize);

    assert(
      this.maxRequestSize !== null && !isNaN(this.maxRequestSize),
      'Invalid "maxRequestSize" parameter value: expected a numeric value');
  }

  broadcast () {
    // do nothing by default
  }

  joinChannel (channel, connectionId) {
    // do nothing by default
    return {channel, connectionId};
  }

  leaveChannel (channel, connectionId) {
    // do nothing by default
    return {channel, connectionId};
  }

  notify () {
    // do nothing by default
  }

  /**
   * Gets the list of IPs from a HTTP/WebSocket connection request
   *
   * @param  {HttpResponse|WebSocket} connection
   * @param  {HttpRequest} httpRequest  - connection request
   * @return {Promise.<Array>}
   */
  getIps(connection, httpRequest) {
    const
      ipbuf = new Uint8Array(connection.getRemoteAddress()),
      ips = [ipaddr.fromByteArray(ipbuf).toString()],
      xForwardedFor = httpRequest.getHeader['x-forwarded-for'];

    if (xForwardedFor && xForwardedFor.length > 0) {
      for (const ip of xForwardedFor.split(',')) {
        const trimmed = ip.trim();

        if (trimmed.length) {
          ips.push(trimmed);
        }
      }
    }

    return ips;
  }
}

module.exports = Protocol;
