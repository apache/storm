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

var visNS = {
    // Update / refresh setting
    shouldUpdate: true,
    updateFreqMs: 30000,

    // Holds our network
    network: null,

    // Holds nodes and edge definitions
    nodes: new vis.DataSet(),
    edges: new vis.DataSet(),

    // References our visualization container element
    networkContainer: null,

    // Holds map of Sanitized Stream Id => Stream Id
    availableStreamsHash: { },

    // vis.js options
    options: {
        edges:{
            arrows: {
                to: {enabled: true, scaleFactor:1}
            },
            hoverWidth: 1.5,
            shadow:{
                enabled: true,
                color: 'rgba(0,0,0,0.5)',
                size:10,
                x:5,
                y:5
            },
            smooth: {
                type: "continuous",
                forceDirection: "none"
            }
        },
        nodes: {
            color: {
                border: '#2B7CE9',
                background: '#97C2FC',
                highlight: {
                    border: '#2B7CE9',
                    background: '#D2E5FF'
                },
                hover: {
                    border: '#2B7CE9',
                    background: '#D2E5FF'
                }
            },
            shadow:{
              enabled: true,
              color: 'rgba(0,0,0,0.5)',
              size:10,
              x:5,
              y:5
            },
        },
        physics:{
            enabled: false
        },
        layout: {
            randomSeed: 31337,
            improvedLayout:true,
            hierarchical: {
                enabled: true,
                levelSeparation: 150,
                nodeSpacing: 300,
                treeSpacing: 200,
                blockShifting: true,
                edgeMinimization: true,
                parentCentralization: true,
                direction: 'UD',        // UD, DU, LR, RL
                sortMethod: 'directed'   // hubsize, directed
            }
        },
        interaction: {
            navigationButtons: false
        }
    }
};

function parseResponse(json) {
    console.log("Updating network");
    // parse json
    for (var componentId in json) {
        parseNode(json[componentId], componentId);
    }

    // Create network if it does not exist yet
    if (visNS.network == null) {
        createNetwork()
    }
}

function createNetwork() {
    var data = {
        "nodes": visNS.nodes,
        "edges": visNS.edges
    };
    // Create network
    visNS.network = new vis.Network(visNS.networkContainer, data, visNS.options);

    // Create event handlers
    visNS.network.on("click", function (params) {
        handleClickEvent(params);
    });
    visNS.network.on("deselectNode", function(params) {
        handleNodeClickEvent(null);
    });

    // Then disable layout
    visNS.network.setOptions({layout: {hierarchical: false } });
}

function handleClickEvent(params) {
    if (params["nodes"].length == 1) {
        handleNodeClickEvent(params["nodes"][0])
    }
}

function handleNodeClickEvent(nodeId) {
    // if nodeId is null, hide
    if (nodeId == null) {
        // Ensure slider is hidden
        $("#bolt_slideout_inner").css("display", "none");
        return;
    }

    // add new values
    var nodeDetails = json[nodeId];
    //console.log(nodeDetails);

    // Prep the json slightly
    nodeDetails[":id"] = nodeId;
    nodeDetails[":inputs_length"] = nodeDetails[":inputs"].length;
    nodeDetails[":stats_length"] = nodeDetails[":stats"].length;

    // Conditionally link to component
    for (x=0; x<nodeDetails[":inputs"].length; x++) {
        var showLink = true;
        if (nodeDetails[":inputs"][x][":component"].startsWith("__")) {
            showLink = false;
        }
        nodeDetails[":inputs"][x][":show_link"] = showLink;
    }

    // Calculate uptime in a nice format.
    for (x=0; x<nodeDetails[":stats"].length; x++) {
        nodeDetails[":stats"][x][":uptime_str"] = secondsToString(nodeDetails[":stats"][x][":uptime_secs"]);
    }

    // Render and display template.
    var template = $('#bolt_info_template').html();
    var html = Mustache.to_html(template, nodeDetails);
    $("#bolt-details").html(html);

    // Ensure slider is viewable
    $("#bolt_slideout_inner").css("display", "inline");
    return;
}

function parseNode(nodeJson, nodeId) {
    // Conditional hack for system components
    if (isSystemComponent(nodeId)) {
        nodeJson[":type"] = "system";
    }

    // Determine node color
    var col = "#D2E5FF"
    var selectedCol = "#97C2FC";
    var shape = "dot"

    if (nodeJson[":type"] === "bolt") {
        // Determine color based on capacity
        var cap = Math.min(nodeJson[":capacity"], 1);
        var red = Math.floor(cap * 225) + 30;
        var green = Math.floor(255 - red);
        var blue = Math.floor(green/5);
        col = "rgba(" + red + "," + green + "," + blue + ",1)"
        selectedCol = "rgba(" + (red+2) + "," + (green) + "," + (green + 2) + ",1)"
    }
    if (nodeJson[":type"] === "spout") {
        shape = "triangleDown";
        col = "#D2E5FF";
        selectedCol = "#97C2FC"
    }
    if (nodeJson[":type"] === "system") {
        shape = "diamond";
        col = "#ffe6cc";
        selectedCol = "#ffc180";
    }

    // Generate title
    var title = "<b>" + nodeId + "</b><br/>";
    title += "Capacity: " + nodeJson[":capacity"] + "<br/>";
    title += "Latency: " + nodeJson[":latency"]

    // Construct the node
    var node = {
        "id": nodeId,
        "label": nodeId,
        "color": {
            "background": col,
            "highlight": {
                "background": selectedCol
            }
        },
        "shape": shape,
        "shadow": {
            "enabled": true
        },
        "title": title,
        "size": 45
    };

    // Construct edges
    for (var index in nodeJson[":inputs"]) {
        var inputComponent = nodeJson[":inputs"][index];
        parseEdge(inputComponent, nodeId);
    }

    if (node != null) {
        visNS.nodes.update(node);
    }
}

function isSystemComponent(nodeId) {
    return nodeId.startsWith("__");
}

function parseEdge(edgeJson, sourceId) {
    //console.log(edgeJson);

    // Make this stream available
    addAvailableStream(edgeJson[":stream"], edgeJson[":sani-stream"])

    // Create a unique Id
    var id = edgeJson[":component"] + ":" + sourceId + ":" + edgeJson[":sani-stream"];

    // Determine if stream is enabled
    if (!isStreamEnabled(edgeJson[":sani-stream"])) {
        // Remove edge
        visNS.edges.remove({ "id": id });
        return
    }

    if (!visNS.edges.get(id)) {
        visNS.edges.update({
            "id": id,
            "from": edgeJson[":component"],
            "to": sourceId,
            "label": edgeJson[":stream"],
            "title": "From: " + edgeJson[":component"] + "<br>To: " + sourceId + "<br>Grouping: " + edgeJson[":grouping"]
        });
    }
}

function addAvailableStream(streamId, streamIdSanitized) {
    // Create a master list of all streams
    if (visNS.availableStreamsHash[streamIdSanitized] == null) {
        visNS.availableStreamsHash[streamIdSanitized] = streamId;
        updateAvailableStreams();
    }
}

function updateAvailableStreams() {
    var container = jQuery("#available-streams");
    $.each(visNS.availableStreamsHash, function(streamIdSanitized, streamName) {
        var entry = jQuery(container).find("#stream-" + streamIdSanitized)
        if (entry.length == 0) {
            var checked = "checked"
            if (streamName.startsWith("__")) {
                checked = ""
            }
            // Render template
            var template = $('#stream_selector_template').html();
            var html = Mustache.to_html(template, {"checked": checked, "streamName": streamName, "streamNameSanitized": streamIdSanitized});
            container.append(html);

            // keep list of streams in sorted order
            jQuery("#available-streams li").sort(asc_sort).appendTo('#available-streams');
        }
    });
}

// Called when a stream's checkbox is selected/unselected.
function checkStream(checkBox) {
    // reload data
    parseResponse(json);
}

// Redraws edges
function redrawStreams() {
    visNS.edges.forEach(function(edge) { visNS.edges.remove(edge); visNS.edges.add(edge); });
}

// Returns true if a stream is enabled
function isStreamEnabled(streamIdSanitized) {
    return jQuery("input#stream-" + streamIdSanitized).is(':checked')
}

var update = function() {
    if(visNS.shouldUpdate) {
        $.ajax({
            url: "/api/v1/topology/"+$.url("?id")+"/visualization?sys="+$.url("?sys"),
            success: function (data, status, jqXHR) {
                json = data;
                parseResponse(data);
                setTimeout(update, visNS.updateFreqMs);
            }
        });
    } else {
        setTimeout(update, visNS.updateFreqMs);
    }
}

function secondsToString(seconds) {
    var numDays = Math.floor(seconds / 86400);
    var numHours = Math.floor((seconds % 86400) / 3600);
    var numMinutes = Math.floor(((seconds % 86400) % 3600) / 60);
    var numSeconds = ((seconds % 86400) % 3600) % 60;

    var returnStr = "";
    if (numDays > 0) {
        returnStr += numDays + " days ";
    }
    if (numHours > 0) {
        returnStr += numHours + " hours ";
    }
    if (numMinutes > 0) {
        returnStr += numMinutes + " mins ";
    }
    if (numSeconds > 0) {
        returnStr += numSeconds + " seconds";
    }
    return returnStr;
}

// ascending sort
function asc_sort(a, b){
    return ($(b).text()) < ($(a).text()) ? 1 : -1;
}

$(document).ready(function() {
    visNS.networkContainer = document.getElementById("mynetwork");
    update();
});

