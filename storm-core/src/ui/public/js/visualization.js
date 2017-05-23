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

var _showSystem = false;
var _showAcker = false;
var _showMetrics = false;
var container;

var options = {
    edges:{
        arrows: {
            to:     {enabled: true, scaleFactor:1}
        },
        hoverWidth: 1.5,
        shadow:{
            enabled: true,
            color: 'rgba(0,0,0,0.5)',
            size:10,
            x:5,
            y:5
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
};

// Holds all stream names
var availableStreamsHash = { }

// Holds our network
var network;

// Holds nodes and edge definitions
var nodes = new vis.DataSet();
var edges = new vis.DataSet();

// Update/refresh settings
var should_update = true
var update_freq_ms = 30000

function parseResponse(json) {
    console.log("Updating network");

    // parse json
    for (var componentId in json) {
        parseNode(json[componentId], componentId);
    }

    // Create network if it does not exist yet
    if (network == null) {
        createNetwork()
    }
}

function createNetwork() {
    var data = {
        nodes,
        edges
    };
    // Create network
    network = new vis.Network(container, data, options);

    // Create event handlers
    network.on("click", function (params) {
        handleClickEvent(params);
    });

    // Then disable layout
    network.setOptions({layout: {hierarchical: false } });
}

function handleClickEvent(params) {
    // for debugging
    // console.log(JSON.stringify(params, null, 4));
    if (params["nodes"].length == 1) {
        handleNodeClickEvent(params["nodes"][0])
    }
}

function handleNodeClickEvent(nodeId) {
    // add new values
    var nodeDetails = json[nodeId];
    console.log(nodeDetails);

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
    console.log("parsing node " + nodeId);
    // Conditionally hide certain nodes
    if (!showNode(nodeId)) {
        return
    }

    // Determine node color
    var col = "#97C2FC"
    var shape = "dot"
    if (nodeJson[":type"] === "bolt") {
        // Determine color based on capacity
        var cap = Math.min(nodeJson[":capacity"], 1);
        var red = Math.floor(cap * 225) + 30;
        var green = Math.floor(255 - red);
        var blue = Math.floor(green/5);
        col = "rgba(" + red + "," + green + "," + blue + ",1)"
    }
    if (nodeJson[":type"] === "spout") {
        shape = "triangleDown";
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
            "background": col
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
        nodes.update(node);
    }
}

function showNode(nodeId) {
    if (nodeId == "__system" && !_showSystem) {
        return false;
    }
    if (nodeId == "__acker" && !_showAcker) {
        return false
    }
    if (nodeId == "__metrics" && !__showMetrics) {
        return false
    }
    return true
}

function parseEdge(edgeJson, sourceId) {
    // Make this stream available
    addAvailableStream(edgeJson[":stream"])

    // Create a unique Id
    var id = edgeJson[":component"] + ":" + sourceId + ":" + edgeJson[":stream"];

    // Determine if stream is enabled
    if (!isStreamEnabled(edgeJson[":stream"])) {
        // Remove edge
        edges.remove({ "id": id });
        return
    }

    if (!edges.get(id)) {
        console.log("Updating edge " + id );
        edges.update({
            "id": id,
            "from": edgeJson[":component"],
            "to": sourceId,
            "label": edgeJson[":stream"],
            "title": "From: " + edgeJson[":component"] + "<br>To: " + sourceId + "<br>Grouping: " + edgeJson[":grouping"]
        });
    }
}

function addAvailableStream(streamId) {
    // Create a master list of all streams
    if (availableStreamsHash[streamId] == null) {
        availableStreamsHash[streamId] = true;
        updateAvailableStreams();
    }
}

function updateAvailableStreams() {
    var container = jQuery("#available-streams");
    for (var streamName in availableStreamsHash) {
        var entry = jQuery(container).find("#stream-" + streamName)
        if (entry.length == 0) {
            var checked = "checked"
            if (streamName.startsWith("__")) {
                checked = ""
            }
            // Render template
            var template = $('#stream_selector_template').html();
            var html = Mustache.to_html(template, {"checked": checked, "streamName": streamName});
            container.append(html);
        }
    }
}

// Called when a stream's checkbox is selected/unselected.
function checkStream(checkBox) {
    var stream = jQuery(checkBox).attr("id").replace("stream-","")

    // reload data
    parseResponse(json);
}

function isStreamEnabled(streamId) {
    return jQuery("input#stream-" + streamId).is(':checked')
}

var update = function() {
    console.log("In update " + should_update);
    if(should_update) {
        $.ajax({
            url: "/api/v1/topology/"+$.url("?id")+"/visualization",
            success: function (data, status, jqXHR) {
                json = data;
                parseResponse(data);
                setTimeout(update, update_freq_ms);
            }
        });
    } else {
        setTimeout(update, update_freq_ms);
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

$(document).ready(function() {
    container = document.getElementById("mynetwork");
    update();
});