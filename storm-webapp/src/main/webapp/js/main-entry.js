/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Webpack entry point for the main Storm UI pages:
 * index, topology, component, supervisor, owner,
 * search_result, deep_search_result, logviewer_search.
 *
 * Uses CommonJS require() to avoid ESM/CJS interop issues
 * with factory-pattern modules like DataTables.
 */

// --- CSS ---
require('bootstrap/dist/css/bootstrap.min.css');
require('datatables.net-dt/css/dataTables.dataTables.css');
require('datatables.net-bs/css/dataTables.bootstrap.css');
require('../lib/jsonFormatter.min.css');
require('../css/style.css');

// --- jQuery (must be first, made global for inline scripts) ---
var $ = require('jquery');
window.$ = $;
window.jQuery = $;

// --- DataTables + Bootstrap 3 integration ---
require('datatables.net');
require('datatables.net-bs');

// --- Mustache + jQuery integration ---
var Mustache = require('mustache');
window.Mustache = Mustache;
$.mustache = function (template, view, partials) {
  return Mustache.render(template, view, partials);
};
$.fn.mustache = function (view, partials) {
  return $(this).map(function (i, elm) {
    var template = $.trim($(elm).html());
    var output = $.mustache(template, view, partials);
    return $(output).get();
  });
};

// --- Cookie handling ---
var Cookies = require('js-cookie');
// The old jquery.cookies API used $.cookies.get/set — shim it
$.cookies = {
  get: function (name) {
    var val = Cookies.get(name);
    if (val === 'true') return true;
    if (val === 'false') return false;
    return val;
  },
  set: function (name, value, opts) {
    var cookieOpts = {};
    if (opts && opts.path) cookieOpts.path = opts.path;
    if (opts && opts.expiresAt) cookieOpts.expires = new Date(opts.expiresAt);
    Cookies.set(name, value, cookieOpts);
  }
};

// --- URL parser ---
var Url = require('domurl');
window.Url = Url;
// Shim $.url() used by inline scripts: $.url("?paramName")
$.url = function (param) {
  var u = new Url(window.location.href);
  if (param && param.charAt(0) === '?') {
    return u.query[param.substring(1)] || '';
  }
  return u;
};

// --- jQuery BlockUI ---
require('jquery-blockui');

// --- JSON Formatter (project-local jQuery plugin, no npm equivalent) ---
require('../lib/jsonFormatter.min.js');

// --- npm libraries ---
require('typeahead.js');
require('bootstrap');
var moment = require('moment');
window.moment = moment;

// --- Custom Storm UI code ---
require('./script');
