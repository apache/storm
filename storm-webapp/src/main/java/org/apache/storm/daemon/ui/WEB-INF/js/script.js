/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
$(function () {
    $(".js-only").show();
});

//Add in custom sorting for some data types
$.extend( $.fn.dataTableExt.oSort, {
  // The 'resource-num' type is for sorting resource guarantees 
  // that have 'N/A' in the type. It's just the same as 'num', but
  // we preprocess to sort N/A below 0. Otherwise, if we just use
  // type 'num', DataTables always puts N/A at the *end* of the list 
  // (for both asc and desc sort), which is confusing.
  "resource-num-pre": function(raw) {
    if (raw == 'N/A') {
      return Number.NEGATIVE_INFINITY;
    } else {
      return $.fn.dataTableExt.oSort['num-pre'](raw);
    }
  },
  "resource-num-asc": $.fn.dataTableExt.oSort['num-asc'],
  "resource-num-desc": $.fn.dataTableExt.oSort['num-desc'],

  "time-str-pre": function (raw) {
    var s = $(raw).text();
    if (s == "") {
      s = raw;
    }
    if (s.search('All time') != -1) {
      return 1000000000;
    }
    var total = 0;
    $.each(s.split(' '), function (i, v) {
       var amt = parseInt(v);
       if (v.search('ms') != -1) {
         total += amt;
       } else if (v.search('s') != -1) {
         total += amt * 1000;
       } else if (v.search('m') != -1) {
         total += amt * 1000 * 60;
       } else if (v.search('h') != -1) {
         total += amt * 1000 * 60 * 60;
       } else if (v.search('d') != -1) {
         total += amt * 1000 * 60 * 60 * 24;
       }
     });
     return total;
   },
   "time-str-asc": function ( a, b ) {
      return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },
    "time-str-desc": function ( a, b ) {
      return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
});

var minEntriesToShowPagination = 20;
function dtAutoPage(selector, conf) {
  if ($(selector.concat(" tr")).length <= minEntriesToShowPagination) {
    $.extend(conf, {paging: false});
  }
  return $(selector).DataTable(conf);
}

function toggleSys() {
    var sys = $.cookies.get('sys') || false;
    sys = !sys;

    var exDate = new Date();
    exDate.setDate(exDate.getDate() + 365);

    $.cookies.set('sys', sys, {'path':'/', 'expiresAt':exDate.toUTCString()});
    window.location = window.location;
}

function ensureInt(n) {
    var isInt = /^\d+$/.test(n);
    if (!isInt) {
        alert("'" + n + "' is not integer.");
    }

    return isInt;
}

function sendRequest(id, action, extra, body, cb){
   var opts = {
        type:'POST',
        url:'/api/v1/topology/' + id + '/' + action
    };

    if (body) {
        opts.data = JSON.stringify(body);
        opts.contentType = 'application/json; charset=utf-8';
    }

    opts.url += extra ? "/" + extra : "";

    $.ajax(opts).always(function(data){
        cb (data);
    }).fail (function(){
        alert("Error while communicating with Nimbus.");
    });
}

function confirmComponentAction(topologyId, componentId, componentName, action, param, defaultParamValue, paramText, actionText) {
    var opts = {
        type:'POST',
        url:'/api/v1/topology/' + topologyId + '/component/' + componentId + '/' + action
    };
    if (actionText === undefined) {
        actionText = action;
    }
    if (param) {
        var paramValue = prompt('Do you really want to ' + actionText + ' component "' + componentName + '"? ' +
                                  'If yes, please, specify ' + paramText + ':',
                                  defaultParamValue);
        if (paramValue != null && paramValue != "" && ensureInt(paramValue)) {
            opts.url += '/' + paramValue;
        } else {
            return false;
        }
    } else {
        if (typeof defaultParamValue !== 'undefined') {
            opts.url +=  '/' + defaultParamValue;
        }
        if (!confirm('Do you really want to ' + actionText + ' component "' + componentName + '"?')) {
            return false;
        }
    }

    $("input[type=button]").attr("disabled", "disabled");
    $.ajax(opts).always(function () {
        window.location.reload();
    }).fail(function () {
        alert("Error while communicating with Nimbus.");
    });

    return false;
}

function confirmAction(id, name, action, param, defaultParamValue, paramText, actionText) {
    var opts = {
        type:'POST',
        url:'/api/v1/topology/' + id + '/' + action
    };
    if (actionText === undefined) {
        actionText = action;
    }
    if (param) {
        var paramValue = prompt('Do you really want to ' + actionText + ' topology "' + name + '"? ' +
                              'If yes, please, specify ' + paramText + ':',
                              defaultParamValue);

        if (paramValue != null && paramValue != "" && ensureInt(paramValue)) {
            opts.url += '/' + paramValue;
        } else {
            return false;
        }
    } else {
        if (typeof defaultParamValue !== 'undefined') {
            opts.url +=  '/' + defaultParamValue;
        }
        if (!confirm('Do you really want to ' + actionText + ' topology "' + name + '"?')) {
            return false;
        }
    }

    $("input[type=button]").attr("disabled", "disabled");
    $.ajax(opts).always(function () {
        window.location.reload();
    }).fail(function () {
        alert("Error while communicating with Nimbus.");
    });

    return false;
}

$(function () {
  $('[data-toggle="tooltip"]').tooltip()
})

function formatConfigData(data) {
    var mustacheFormattedData = {'config':[]};
    for (var prop in data) {
       if(data.hasOwnProperty(prop)) {
           mustacheFormattedData['config'].push({
               'key': prop,
               'value': JSON.stringify(data[prop])
           });
       }
    }
    return mustacheFormattedData;
}

function formatErrorTimeSecs(response){
    if('componentErrors' in response) {
        var errors = response["componentErrors"];
        for(var i = 0 ; i < errors.length ; i++){
            var time = errors[i]['time'];
            errors[i]['time'] = moment.utc(time).local().format("ddd, DD MMM YYYY HH:mm:ss Z");
        }
    }
    return response;
}


function renderToggleSys(div) {
    var sys = $.cookies.get("sys") || false;
    if(sys) {
       div.append("<span data-original-title=\"Use this to toggle inclusion of storm system components.\" class=\"tip right\"><input onclick=\"toggleSys()\" value=\"Hide System Stats\" type=\"button\" class=\"btn btn-default\"></span>");
    } else {
       div.append("<span class=\"tip right\" title=\"Use this to toggle inclusion of storm system components.\"><input onclick=\"toggleSys()\" value=\"Show System Stats\" type=\"button\" class=\"btn btn-default\"></span>");
    }
}

function topologyActionJson(id, encodedId, name, status, msgTimeout, loggersTotal, debug, samplingPct) {
    var jsonData = {};
    jsonData["id"] = id;
    jsonData["encodedId"] = encodedId;
    jsonData["name"] = name;
    jsonData["msgTimeout"] = msgTimeout;
    jsonData["activateStatus"] = (status === "INACTIVE") ? "enabled" : "disabled";
    jsonData["deactivateStatus"] = (status === "ACTIVE") ? "enabled" : "disabled";
    jsonData["rebalanceStatus"] = (status === "ACTIVE" || status === "INACTIVE" ) ? "enabled" : "disabled";
    jsonData["killStatus"] = (status !== "KILLED") ? "enabled" : "disabled";
    jsonData["startDebugStatus"] = (status === "ACTIVE" && loggersTotal!=0 && !debug) ? "enabled" : "disabled";
    jsonData["stopDebugStatus"] = (status === "ACTIVE" && debug) ? "enabled" : "disabled";
    jsonData["loggersDisabled"] = loggersTotal==null || loggersTotal==0;
    jsonData["currentSamplingPct"] = samplingPct;
    return jsonData;
}

function componentActionJson(encodedTopologyId, encodedId, componentName, status, loggersTotal, debug, samplingPct) {
    var jsonData = {};
    jsonData["encodedTopologyId"] = encodedTopologyId;
    jsonData["encodedId"] = encodedId;
    jsonData["componentName"] = componentName;
    jsonData["startDebugStatus"] = (status === "ACTIVE" && loggersTotal!=0 && !debug) ? "enabled" : "disabled";
    jsonData["stopDebugStatus"] = (status === "ACTIVE" && debug) ? "enabled" : "disabled";
    jsonData["loggersDisabled"] = loggersTotal==null || loggersTotal==0;
    jsonData["currentSamplingPct"] = samplingPct;
    return jsonData;
}

function topologyActionButton(id,name,status,actionLabel,command,wait,defaultWait) {
    var buttonData = {};
    buttonData["buttonStatus"] = status ;
    buttonData["actionLabel"] = actionLabel;
    buttonData["command"] = command;
    buttonData["isWait"] = wait;
    buttonData["defaultWait"] = defaultWait;
    return buttonData;
}

$.blockUI.defaults.css = {
    border: 'none',
    padding: '15px',
    backgroundColor: '#000',
    '-webkit-border-radius': '10px',
    '-moz-border-radius': '10px',
    'border-radius': '10px',
    opacity: .5,
    color: '#fff',margin:0,width:"30%",top:"40%",left:"35%",textAlign:"center"
};

// add a url query param to static (templates normally) ajax requests
// for cache busting
function getStatic(url, cb) {
    return $.ajax({
        url: url,
        data: {
            '_ts': '${packageTimestamp}'
        },
        success: cb
    });
};

function makeSupervisorWorkerStatsTable (response, elId, parentId) {
    makeWorkerStatsTable (response, elId, parentId, "supervisor");
};

function makeTopologyWorkerStatsTable (response, elId, parentId) {
    makeWorkerStatsTable (response, elId, parentId, "topology");
};

var formatComponents = function (row) {
    if (!row) return;
    var result = '';
    Object.keys(row.componentNumTasks || {}).sort().forEach (function (component){
        var numTasks = row.componentNumTasks[component];
        result += '<a class="worker-component-button btn btn-xs btn-primary" href="/component.html?id=' + 
                        component + '&topology_id=' + row.topologyId + '">';
        result += component;
        result += '<span class="badge">' + numTasks + '</span>';
        result += '</a>';
    });
    return result;
};

var format = function (row){
    var result = '<div class="worker-child-row">Worker components: ';
    result += formatComponents (row) || 'N/A';
    result += '</div>';
    return result;
};

// Build a table of per-worker resources and components (when permitted)
var makeWorkerStatsTable = function (response, elId, parentId, type) {
    var showCpu = response.schedulerDisplayResource;

    var columns = [
        {
            data: 'host', 
            render: function (data, type, row){
                return type === 'display' ? 
                    ('<a href="/supervisor.html?host=' + data + '">' + data + '</a>') :
                    data;
            }
        },
        {
            data: 'port',
            render: function (data, type, row) {
                var logLink = row.workerLogLink;
                return type === 'display' ?
                    ('<a href="' + logLink + '">' + data + '</a>'): 
                    data;
            }
        },
        { 
            data: function (row, type){
                // if we are showing or filtering, using the formatted
                // uptime, else use the seconds (for sorting)
                return (type === 'display' || type === 'filter') ? 
                    row.uptime :
                    row.uptimeSeconds;
            }
        },
        { data: 'executorsTotal' },
        { 
            data: function (row){
                return row.assignedMemOnHeap + row.assignedMemOffHeap;
            }
        },
    ];

    if (showCpu) {
        columns.push ({ data: 'assignedCpu' });
    }

    columns.push ({ 
        data: function (row, type, obj, dt) {
            var components = Object.keys(row.componentNumTasks || {});
            if (components.length === 0){
                // if no components returned, it means the worker
                // topology isn't one the user is authorized to see
                return "N/A";
            }

            if (type == 'filter') {
                return components;
            }

            if (type == 'display') {
                // show a button to toggle the component row
                return '<button class="btn btn-xs btn-info details-control" type="button">' +
                       components.length + ' components</button>';
            }

            return components.length;
        }
    });

    switch (type){
        case 'topology':
            // the topology page has the supervisor id as the second column in the worker table
            columns.splice(1, 0, {
                data: 'supervisorId', 
                render: function (data, type, row){
                    return type === 'display' ? 
                        ('<a href="/supervisor.html?id=' + data + '">' + data + '</a>') :
                        data;
                }
            });
            break;
        case 'supervisor':
            // the supervisor page has the topology name as the first column in the worker table
            columns.unshift ({
                data: function (row, type){
                    return type === 'display' ? 
                        ('<a href="/topology.html?id=' + row.topologyId + '">' + row.topologyName + '</a>') :
                        row.topologyId;
                }
            });
            // the supervisor page has owner as the third column in the worker table
            columns.splice(2, 0, {
                data: 'owner',
                render: function(data, type, row) {
                    return type === 'display' ?
                        ('<a href="/owner.html?id=' + data + '">' + data + '</a>') :
                        data;
                }
            });
            break;
    }

    var initializeComponents = function (){
        var show = $.cookies.get("showComponents") || false;

        // toggle all components visibile/invisible
        $(elId + ' tr').each(function (){
            var dt = $(elId).dataTable();
            showComponents(dt.api().row(this), show);
        });
    };

    var pagingEnabled = response && response.workers && 
                            response.workers.length > minEntriesToShowPagination;

    var workerStatsTable = $(elId).DataTable({
        paging: pagingEnabled,
        data: response.workers,
        autoWidth: false,
        columns: columns,
        initComplete: function (){
            // add a "Toggle Components" button
            renderToggleComponents ($(elId + '_filter'), elId);
        },
        drawCallback: function (){
            initializeComponents();
        }
    });

    // Add event listener for opening and closing components row
    // on a per component basis
    $(elId + ' tbody').on('click', 'button.details-control', function () {
        var tr = $(this).closest('tr');
        var row = workerStatsTable.row(tr);
        showComponents(row, !row.child.isShown());
    });

    $(parentId + ' #toggle-on-components-btn').on('click', 'input', function (){
        toggleComponents(elId);
    });

    $(elId + ' [data-toggle="tooltip"]').tooltip();
};

function renderToggleComponents(div, targetTable) {
     var showComponents = $.cookies.get("showComponents") || false;
     div.append("<span id='toggle-on-components-btn' class=\"tip right\" " +
                "title=\"Use this to toggle visibility of worker components.\">"+
                    "<input value=\"Toggle Components\" type=\"button\" class=\"btn btn-info\">" + 
                "</span>");
}

function showComponents(row, open) {
    var tr = $(this).closest('tr');
    if (!open) {
        // This row is already open - close it
        row.child.hide();
        tr.removeClass('shown');
    } else {
        // Open this row
        row.child (format (row.data())).show();
        tr.addClass('shown');
    }
}

function toggleComponents(elId) {
    var show = $.cookies.get('showComponents') || false;
    show = !show;

    var exDate = new Date();
    exDate.setDate(exDate.getDate() + 365);

    $.cookies.set('showComponents', show, {'path':'/', 'expiresAt':exDate.toUTCString()});
    $(elId + ' tr').each(function (){
        var dt = $(elId).dataTable();
        showComponents(dt.api().row(this), show);
    });
}

function open_visualization(sys) {
    window.open('/visualize.html?id='+ $.url("?id") + "&sys=" + sys, '_blank');
}

function show_visualization(sys) {
    getStatic("/templates/topology-page-template.html", function(template) {
        jsError(function() {
            var topologyVisualization = $("#visualization-container");
            if (topologyVisualization.find("canvas").length == 0) {
                topologyVisualization.append(
                    Mustache.render($(template)
                        .filter("#topology-visualization-container-template")
                        .html(),
                        {id: $.url("?id"), sys: sys}));
            }
        });

        $("#visualization-container").show(500);
        $("#show-hide-visualization").attr('value', 'Hide Visualization');
        $("#show-hide-visualization").unbind("click");
        $("#show-hide-visualization").click(function () { hide_visualization(sys) });
    });
}

function hide_visualization(sys) {
    $("#visualization-container").hide(500);
    $("#show-hide-visualization").attr('value', 'Show Visualization');
    $("#show-hide-visualization").unbind("click");
    $("#show-hide-visualization").click(function () { show_visualization(sys) });
    $("#visualization-container").empty();
}

function jsError(other) {
  try {
    other();
  } catch (err) {
    getStatic("/templates/json-error-template.html", function(template) {
      $("#json-response-error").append(Mustache.render($(template).filter("#json-error-template").html(),{error: "JS Error", errorMessage: err}));
    });
  }
}

function getResourceGuaranteeRemainingFormat(type, data) {
    if (type === 'display' && typeof data === "number") {
        var resourceGuaranteeRemaining = parseFloat(data);
        if (resourceGuaranteeRemaining > 0.0) {
            return '<span class="resource-guarantee-remaining-positive">+' + data + '</span>'
        }
        if (resourceGuaranteeRemaining < 0.0) {
            return '<span class="resource-guarantee-remaining-negative">' + data + '</span>'
        }
    }
    return data;
}

var makeOwnerSummaryTable = function(response, elId, parentId) {
    var showCpu = response.schedulerDisplayResource;

    var columns = [
    {
        data: 'owner',
        render: function(data, type, row) {
            return type === 'display' ?
                ('<a href="/owner.html?id=' + data + '">' + data + '</a>') :
                data;
        }
    }, {
        data: 'totalTopologies',
    }, {
        data: 'totalExecutors',
    }, {
        data: 'totalWorkers',
    }, {
        data: 'totalMemoryUsage',
    }];

    if (showCpu) {
        columns.push({
            data: 'memoryGuarantee',
            type: 'resource-num'
        });
        columns.push({
            data: 'memoryGuaranteeRemaining',
            type: 'resource-num',
            render: function(data, type, row) {
                return getResourceGuaranteeRemainingFormat(type, data);
            }
        });
        columns.push({
            data: 'totalCpuUsage'
        });
        columns.push({
            data: 'cpuGuarantee',
            type: 'resource-num'
        });
        columns.push({
            data: 'cpuGuaranteeRemaining',
            type: 'resource-num',
            render: function(data, type, row) {
                return getResourceGuaranteeRemainingFormat(type, data);
            }
        });
        columns.push({
            data: 'isolatedNodes'
        });
    }

    var userSummaryTable = dtAutoPage(elId, {
        data: response.owners,
        autoWidth: false,
        columns: columns,
    });

    $(elId + ' [data-toggle="tooltip"]').tooltip();
};

function getPageRenderedTimestamp(eId) {
    document.getElementById(eId).innerHTML = "Page rendered at: " + Date();
};
