/*
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
//vis
function VisNetWork() {

}
VisNetWork.prototype = {
    newOptions: function (flags) {
        var fontFace = 'lato,roboto,"helvetica neue","segoe ui",arial,helvetica,sans-serif';
        var maxNodeRadius = 25;

        var verticalMargin = 28;
        var horizontalMargin = 50;
        var treeDistance = 100;
        var levelDistance = 120;
        var chartMinHeight = 250;
        var chartMinWidth = 400;
        return {
            autoResize: true, // The network will automatically detect size changes and redraw itself accordingly
            interaction: {
                hover: true,
                zoomView: false
            },
            //height: 100 + "%",
            height: Math.max(chartMinHeight,
                (treeDistance * flags.breadth + verticalMargin * 2)) + 'px',
            //width: 600+"px",
            width: Math.max(chartMinWidth,
                (maxNodeRadius * (flags.depth + 1) + levelDistance * flags.depth + horizontalMargin * 2)) + 'px',
            layout: {
                hierarchical: {
                    sortMethod: 'directed',
                    direction: 'LR',
                    levelSeparation: levelDistance
                }
            },
            nodes: {
                shape: 'dot',
                font: {
                    size: 13,
                    face: fontFace,
                    strokeColor: '#fff',
                    strokeWidth: 5
                },
                scaling: {
                    min: 10,
                    max: maxNodeRadius
                }
            },
            edges: {
                arrows: {
                    to: true
                },
                font: {
                    size: 11,
                    face: fontFace,
                    align: 'middle'
                },
                color: {
                    opacity: 1
                },
                smooth: true,
                scaling: {
                    min: 1,
                    max: 5
                }
            },
            physics: {
                solver: "repulsion",
                repulsion: {
                    centralGravity: 1
                }
            }
        };
    },

    newData: function (data) {
        var self = this;
        var nodes = [];
        var edges = [];
        var spouts = [];
        data.nodes.forEach(function (n) {
            if (n.spout) {
                spouts.push(n.id);
            }
            var color = n.spout ? "#FAA123" : null;
            nodes.push({
                id: n.id,
                label: n.label,
                level: n.level,
                value: n.value,
                title: (n.title && n.title.length > 0) ? n.title : undefined,
                color: color,
                hidden: n.hidden
            });
        });

        //add origin source
        if (spouts.length > 0) {
            nodes.push({
                id: 0,
                label: "",
                level: -1,
                value: 0,
                hidden: true
            });
        }

        //connect origin source to spout
        spouts.forEach(function (s) {
            edges.push({
                from: 0,
                to: s,
                value: 0,
                hidden: true
            });
        });

        var value_range = this.array_range(data.edges.map(function (e) {
            return e.value;
        }), [0,0]);

        var cycle_value_range = this.array_range(data.edges.map(function (e) {
            return e.cycleValue;
        }), [5,50]);

        data.edges.forEach(function (e) {
            var arrow_factor = self.rangeMapper(e.value, value_range, self.edgeArrowSizeRange(), 0.25);
            var all_colors = self.edgeColorRange();
            var color_index = Math.floor(self.rangeMapper(e.cycleValue, cycle_value_range, [0, all_colors.length], -1));
            var edge_color = self.colorMapper(color_index);
            edges.push({
                id: e.id,
                from: e.from,
                to: e.to,
                value: e.value,
                title: (e.title && e.title.length) > 0 ? e.title : undefined,
                arrows: {to: {scaleFactor: arrow_factor}},
                color: {
                    color: edge_color,
                    highlight: edge_color,
                    hover: edge_color
                },
                hidden: e.hidden
            });
        });
        return {
            nodes: new vis.DataSet(nodes),
            edges: new vis.DataSet(edges)
        };
    },

    //get the min and max value in an array
    array_range: function (arr, defaultRange) {
        var min = defaultRange[0], max = defaultRange[1];
        var all_zero = true;
        arr.forEach(function (e) {
            if (e < min) {
                min = e;
            }
            if (e > max) {
                max = e;
            }
            if (e != 0) {
                all_zero = false;
            }
        });

        if (all_zero) {
            return [0, 0];
        } else {
            return [min, max];
        }
    },

    rangeMapper: function (value, in_range, out_range, defaultValue) {
        if (in_range[0] == in_range[1]) return defaultValue;
        return (value - in_range[0]) * (out_range[1] - out_range[0]) / (in_range[1] - in_range[0]) + out_range[0];
    },

    edgeArrowSizeRange: function () {
        return [0.5, 0.1];
    },

    edgeColorRange: function () {
        // "#F23030"
        return ["#17BF00", "#FF9F19", "#BB0000"];
    },

    colorMapper: function (index) {
        var colors = this.edgeColorRange();
        if (index == -1) {
            return undefined;
        } else if (index == colors.length) {
            index -= 1;
        }

        return colors[index];
    }
};

function VisTable() {

}

VisTable.prototype = {
    newData: function (data) {
        var ret = {};
        var compHead = ["Emitted", "SendTps", "RecvTps"];
        var streamHead = ["TPS", "TupleLifeCycle(ms)"];
        data.nodes.forEach(function (e) {
            ret["component-" + e.id] = {
                title: e.label,
                head: compHead,
                mapValue: e.mapValue,
                valid: true
            };
        });

        data.edges.forEach(function (e) {
            ret["stream-" + e.id] = {
                title: e.from + " -> " + e.to,
                head: streamHead,
                mapValue: e.mapValue,
                valid: true
            };
        });

        return ret;
    }
};

//highcharts
Highcharts.SparkLine = function (options, callback) {
    var defaultOptions = {
        chart: {
            renderTo: (options.chart && options.chart.renderTo) || this,
            backgroundColor: null,
            borderWidth: 0,
            type: options.type,
            margin: [0, 0, 0, 0],
            width: options.width || 150,
            height: 100,
            style: {
                overflow: 'visible'
            },
            skipClone: true
        },
        title: {
            text: ''
        },
        credits: {
            enabled: false
        },
        xAxis: {
            labels: {
                enabled: false
            },
            title: {
                text: null
            },
            startOnTick: false,
            endOnTick: false,
            tickPositions: [],
            categories: options.categories
        },
        yAxis: {
            min: 0,
            minRange: 0.1,
            endOnTick: false,
            startOnTick: true,
            labels: {
                enabled: false
            },
            title: {
                text: null
            },
            tickPositions: [0]
        },
        legend: {
            enabled: false
        },
        tooltip: {
            useHTML: true,
            headerFormat: '<span style="font-size: 10px">{point.x}</span><br/>',
            pointFormat: '<b>{point.name}</b>'  //{series.name}:
        },
        plotOptions: {
            series: {
                animation: false,
                lineWidth: 1,
                shadow: false,
                states: {
                    hover: {
                        lineWidth: 1
                    }
                },
                marker: {
                    enabled: false,
                    radius: 1,
                    states: {
                        hover: {
                            radius: 2
                        }
                    }
                },

                fillOpacity: 0.25
            },
            column: {
                negativeColor: '#910000',
                borderColor: 'silver'
            }
        }
    };
    options = Highcharts.merge(defaultOptions, options);

    return new Highcharts.Chart(options, callback);
};

function ThumbChart() {

}
ThumbChart.prototype = {
    newOptions: function (series, width) {
        var self = this;
        var chart = {};
        var type = "spline";
        if (series.name == "MemoryUsed" || series.name == "CpuUsedRatio") {
            type = "areaspline"
        }

        return {
            series: [{
                name: series.name,
                data: self.newData(series)
            }],
            type: type,
            chart: chart,
            width: width,
            categories: series.category
        };
    },

    newData: function (json) {
        var data = [];
        var len = json.data.length;
        for (var i = 0; i < len; i += 1) {
            data.push({
                name: json.label[i],
                y: json.data[i]
            });
        }
        return data;
    }
};
