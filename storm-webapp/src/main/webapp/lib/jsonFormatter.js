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
 * jQuery plugin for pretty-printing JSON values inside table cells.
 * Replaces the third-party jsonFormatter plugin (which used eval()).
 *
 * Usage:  $('td').jsonFormatter();
 *
 * Produces syntax-highlighted, collapsible JSON using the same CSS
 * class names as the original plugin for backward compatibility.
 */
(function ($) {
  'use strict';

  var defaults = {
    tab: '  ',
    quoteKeys: true,
    collapsible: true,
    hideOriginal: true
  };

  function escapeHtml(str) {
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function indent(level, text, settings) {
    var pad = '';
    for (var i = 0; i < level; i++) {
      pad += settings.tab;
    }
    if (text && text.charAt(text.length - 1) !== '\n') {
      text += '\n';
    }
    return pad + text;
  }

  function processValue(value, level, hasTrailingComma, isChild, settings) {
    var comma = hasTrailingComma ? "<span class='jsonFormatter-coma'>,</span> " : '';
    var type = typeof value;

    if (value === null) {
      return span('null', '', comma, level, isChild, 'jsonFormatter-null', settings);
    }

    if (type === 'undefined') {
      return span('undefined', '', comma, level, isChild, 'jsonFormatter-null', settings);
    }

    if (type === 'number') {
      return span(value, '', comma, level, isChild, 'jsonFormatter-number', settings);
    }

    if (type === 'boolean') {
      return span(value, '', comma, level, isChild, 'jsonFormatter-boolean', settings);
    }

    if (type === 'string') {
      return span(escapeHtml(value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')), '"', comma, level, isChild, 'jsonFormatter-string', settings);
    }

    if (Array.isArray(value)) {
      return processArray(value, level, comma, isChild, settings);
    }

    if (type === 'object') {
      return processObject(value, level, comma, isChild, settings);
    }

    return span(escapeHtml(String(value)), '', comma, level, isChild, 'jsonFormatter-string', settings);
  }

  function span(val, quote, comma, level, addIndent, cssClass, settings) {
    var html = "<span class='" + cssClass + "'>" + quote + val + quote + comma + "</span>";
    return addIndent ? indent(level, html, settings) : html;
  }

  function processArray(arr, level, comma, isChild, settings) {
    if (arr.length === 0) {
      return indent(level, "<span class='jsonFormatter-arrayBrace'>[ ]</span>" + comma, settings);
    }

    var expander = settings.collapsible
      ? "<span class='jsonFormatter-expander jsonFormatter-expanded'></span><span class='jsonFormatter-collapsible'>"
      : '';
    var closer = settings.collapsible ? '</span>' : '';
    var html = indent(level, "<span class='jsonFormatter-arrayBrace'>[</span>" + expander, settings);

    for (var i = 0; i < arr.length; i++) {
      html += processValue(arr[i], level + 1, i < arr.length - 1, true, settings);
    }

    html += indent(level, closer + "<span class='jsonFormatter-arrayBrace'>]</span>" + comma, settings);
    return html;
  }

  function processObject(obj, level, comma, isChild, settings) {
    var keys = Object.keys(obj);
    if (keys.length === 0) {
      return indent(level, "<span class='jsonFormatter-objectBrace'>{ }</span>" + comma, settings);
    }

    var expander = settings.collapsible
      ? "<span class='jsonFormatter-expander jsonFormatter-expanded'></span><span class='jsonFormatter-collapsible'>"
      : '';
    var closer = settings.collapsible ? '</span>' : '';
    var html = indent(level, "<span class='jsonFormatter-objectBrace'>{</span>" + expander, settings);

    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      var q = settings.quoteKeys ? '"' : '';
      html += indent(level + 1,
        "<span class='jsonFormatter-propertyName'>" + q + escapeHtml(key) + q + "</span>: " +
        processValue(obj[key], level + 1, i < keys.length - 1, false, settings),
        settings);
    }

    html += indent(level, closer + "<span class='jsonFormatter-objectBrace'>}</span>" + comma, settings);
    return html;
  }

  $.fn.jsonFormatter = function (options) {
    var settings = $.extend({}, defaults, options);

    return this.each(function () {
      var $el = $(this);
      var raw = $el.html().trim();
      if (raw === '') {
        raw = '""';
      }

      var obj;
      try {
        obj = JSON.parse(raw);
      } catch (e) {
        // Not valid JSON, leave the element unchanged
        return;
      }

      var html = processValue(obj, 0, false, false, settings);
      var $original = $el.wrapInner("<div class='jsonFormatter-original'></div>");
      if (settings.hideOriginal) {
        $original.find('.jsonFormatter-original').hide();
      }
      $original.append("<PRE class='jsonFormatter-codeContainer'>" + html + '</PRE>');

      // Collapsible click handler
      if (settings.collapsible) {
        $el.on('click', '.jsonFormatter-expander', function () {
          var $sibling = $(this).next();
          if ($sibling.length < 1) return;
          if ($(this).hasClass('jsonFormatter-expanded')) {
            $sibling.hide();
            $(this).removeClass('jsonFormatter-expanded').addClass('jsonFormatter-collapsed');
          } else {
            $sibling.show();
            $(this).removeClass('jsonFormatter-collapsed').addClass('jsonFormatter-expanded');
          }
        });
      }
    });
  };
})(jQuery);
