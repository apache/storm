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
require('datatables.net-bs5/css/dataTables.bootstrap5.css');
require('../lib/jsonFormatter.css');
require('../css/style.css');

// --- jQuery (must be first, made global for inline scripts) ---
var $ = require('jquery');
window.$ = $;
window.jQuery = $;

// --- jQuery 4 compatibility shims for APIs removed in jQuery 4.0 ---
// These are used by inline <script> blocks in the HTML pages and by
// jQuery plugins (e.g. jquery-blockui) that haven't been updated for jQuery 4.
if (!$.parseJSON) {
  $.parseJSON = JSON.parse;
}
if (!$.trim) {
  $.trim = function (str) { return str == null ? '' : String(str).trim(); };
}
if (!$.isArray) {
  $.isArray = Array.isArray;
}
if (!$.isFunction) {
  $.isFunction = function (obj) { return typeof obj === 'function'; };
}
if (!$.isNumeric) {
  $.isNumeric = function (obj) { return !isNaN(parseFloat(obj)) && isFinite(obj); };
}
if (!$.type) {
  $.type = function (obj) {
    if (obj == null) return obj + '';
    var t = typeof obj;
    return t === 'object' || t === 'function'
      ? Object.prototype.toString.call(obj).slice(8, -1).toLowerCase()
      : t;
  };
}

// --- DataTables + Bootstrap 3 integration ---
require('datatables.net');
require('datatables.net-bs5');

// --- Mustache + jQuery integration ---
var Mustache = require('mustache');
window.Mustache = Mustache;
$.mustache = function (template, view, partials) {
  return Mustache.render(template, view, partials);
};
$.fn.mustache = function (view, partials) {
  return $(this).map(function (i, elm) {
    var template = $(elm).html().trim();
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
require('../lib/jsonFormatter.js');

// --- npm libraries ---
require('typeahead.js');
var moment = require('moment');
window.moment = moment;

// --- Bootstrap 5 (vanilla JS, no jQuery plugin) ---
var bootstrap = require('bootstrap/dist/js/bootstrap.bundle.js');
window.bootstrap = bootstrap;

// --- Bootstrap 5 tooltip shim ---
// The inline HTML scripts call $('[data-toggle="tooltip"]').tooltip() or
// $('[data-bs-toggle="tooltip"]').tooltip(). Bootstrap 5 uses vanilla JS,
// so we shim the jQuery .tooltip() method.
$.fn.tooltip = $.fn.tooltip || function () {
  return this.each(function () {
    // Support both BS3 data-toggle and BS5 data-bs-toggle attributes
    if (this.getAttribute('data-bs-toggle') === 'tooltip' ||
        this.getAttribute('data-toggle') === 'tooltip') {
      new bootstrap.Tooltip(this);
    }
  });
};

// --- Custom Storm UI code ---
require('./script');
