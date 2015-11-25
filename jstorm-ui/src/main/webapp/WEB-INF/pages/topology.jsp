<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="ct" uri="http://jstorm.alibaba.com/jsp/tags" %>
<%@ taglib prefix="spring" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ page buffer="1024kb" autoFlush="false" %>
<%--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
  --%>

<html>
<head>
    <jsp:include page="layout/_head.jsp"/>
    <link href="assets/css/vis.min.css" rel="stylesheet"/>
</head>
<body>
<jsp:include page="layout/_header.jsp"/>

<div class="container-fluid">
    <div>
        <!-- Nav tabs -->
        <ul class="nav nav-tabs nav-menu" role="tablist">
            <li role="presentation" class="active">
                <a href="#topology-summary" aria-controls="topology-summary" role="tab" data-toggle="tab"
                   id="summary-tab">
                    Topology Summary
                </a>
            </li>
            <li role="presentation">
                <a href="#component-metrics" aria-controls="component-metrics" role="tab" data-toggle="tab"
                   id="comp-tab">
                    Component Metrics
                </a>
            </li>
            <li role="presentation">
                <a href="#worker-metrics" aria-controls="worker-metrics" role="tab" data-toggle="tab" id="worker-tab">
                    Worker Metrics <span class="badge">${workerData.size()}</span>
                </a>
            </li>
            <li role="presentation">
                <a href="#task-stats" aria-controls="worker-metrics" role="tab" data-toggle="tab" id="task-tab">
                    Task Stats <span class="badge">${taskData.size()}</span>
                </a>
            </li>
        </ul>

        <!-- Tab panes -->
        <div class="tab-content">
            <div role="tabpanel" class="tab-pane fade in active" id="topology-summary">

                <!-- ========================================================== -->
                <!------------------------- topology summary --------------------->
                <!-- ========================================================== -->
                <h2>Topology Summary</h2>
                <table class="table table-bordered table-hover table-striped center">
                    <thead>
                    <tr>
                        <th>Topology Name</th>
                        <th>Topology Id</th>
                        <th>Status</th>
                        <th>Uptime</th>
                        <th>Num workers</th>
                        <th>Num tasks</th>
                        <th>Conf</th>
                        <th>Error</th>
                    </tr>
                    </thead>
                    <tbody>
                    <c:choose>
                        <c:when test="${topology != null}">
                            <tr>
                                <td>${topology.name}</td>
                                <td>${topology.id}</td>
                                <td><ct:status status="${topology.status}"/></td>
                                <td><ct:pretty type="uptime" input="${topology.uptimeSecs}"/></td>
                                <td>${topology.numWorkers}</td>
                                <td>${topology.numTasks}</td>
                                <td>
                                    <a href="conf?name=${clusterName}&type=topology&topology=${topology.id}"
                                       target="_blank">
                                        conf
                                    </a>
                                </td>
                                <td>
                                    <c:if test="${topology.errorInfo eq 'Y'}">
                                        <span class="error-msg">${topology.errorInfo}</span>
                                    </c:if>
                                </td>
                            </tr>
                        </c:when>
                        <c:otherwise>
                            <tr>
                                <td colspan="20">
                                    <c:choose>
                                        <c:when test="${flush != null}">
                                            ${flush}
                                        </c:when>
                                        <c:otherwise>
                                            No records found.
                                        </c:otherwise>
                                    </c:choose>
                                </td>
                            </tr>
                        </c:otherwise>
                    </c:choose>
                    </tbody>
                </table>


                <c:if test="${topologyHead.size() > 0}">
                    <!-- ========================================================== -->
                    <!------------------------- Topology Stats --------------------->
                    <!-- ========================================================== -->
                    <h2>
                        Topology Stats
                    </h2>
                    <table class="table table-bordered table-striped center text-wrap">
                        <thead class="center">
                        <tr>
                            <c:forEach var="head" items="${topologyHead}">
                                <th><ct:pretty input="${head}" type="head"/></th>
                            </c:forEach>
                        </tr>
                        </thead>
                        <tbody>
                        <c:choose>
                            <c:when test="${topologyData != null}">
                                <tr>
                                    <c:forEach var="head" items="${topologyHead}">
                                        <c:choose>
                                            <c:when test="${head eq 'MemoryUsed'}">
                                                <td data-order="${topologyData.metrics.get(head)}">
                                                    <ct:pretty input="${topologyData.metrics.get(head)}"
                                                               type="filesize"/>
                                                </td>
                                            </c:when>
                                            <c:otherwise>
                                                <td>${topologyData.metrics.get(head)}</td>
                                            </c:otherwise>
                                        </c:choose>
                                    </c:forEach>
                                </tr>
                                <tr id="chart-tr" class="hidden">
                                    <c:forEach var="head" items="${topologyHead}">
                                        <td class="topo-chart">
                                            <div class="chart-canvas" data-id="chart-${head}"></div>
                                        </td>
                                    </c:forEach>
                                </tr>
                            </c:when>
                            <c:otherwise>
                                <tr class="center">
                                    <td colspan="12">
                                        No records found.
                                    </td>
                                </tr>
                            </c:otherwise>
                        </c:choose>
                        </tbody>
                    </table>
                </c:if>

                <h2>Topology Graph</h2>

                <div class="clearfix">
                    <div id="topology-graph"></div>
                    <div id="graph-event">
                        <div v-show="valid" style="display: none;">
                            <h4 style="margin-top: 0;">{{title}}</h4>
                            <table class="table table-bordered table-hover table-striped">
                                <thead>
                                <tr>
                                    <th>Window</th>
                                    <th v-repeat="head">
                                        {{$value}}
                                    </th>
                                </tr>
                                </thead>
                                <tbody>
                                <tr>
                                    <td>1 min</td>
                                    <td v-repeat="head">
                                        {{mapValue[$value][60]}}
                                    </td>
                                </tr>
                                <tr>
                                    <td>10 min</td>
                                    <td v-repeat="head">
                                        {{mapValue[$value][600]}}
                                    </td>
                                </tr>
                                <tr>
                                    <td>2 hour</td>
                                    <td v-repeat="head">
                                        {{mapValue[$value][7200]}}
                                    </td>
                                </tr>
                                <tr>
                                    <td>1 day</td>
                                    <td v-repeat="head">
                                        {{mapValue[$value][86400]}}
                                    </td>
                                </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
                <p id="topology-graph-tips" class="text-muted">acker has been removed for clarity, the stream color represent tuple life cycle time.</p>
            </div>
            <%--<div role="tabpanel" class="tab-pane fade" id="topology-dag"></div>--%>
            <div role="tabpanel" class="tab-pane fade" id="component-metrics">
                <!-- ========================================================== -->
                <!------------------------- Component Metrics --------------------->
                <!-- ========================================================== -->
                <h2>
                    Component Metrics
                    <%--<span id="more-metrics" class="glyphicon glyphicon-collapse-down more-metrics" title="Show more metrics info"--%>
                    <%--data-toggle="tooltip" data-placement="bottom"></span>--%>
                    |
        <span class="window">
            Window:
            <c:set var="topo_url"
                   value="topology?id=${topology.id}&cluster=${clusterName}"/>
            <a href="${topo_url}&win=60#comp-tab" class="label ${cls_60}">1 min</a>
            <a href="${topo_url}&win=600#comp-tab" class="label ${cls_600}">10 min</a>
            <a href="${topo_url}&win=7200#comp-tab" class="label ${cls_7200}">2 hours</a>
            <a href="${topo_url}&win=86400#comp-tab" class="label ${cls_86400}">1 day</a>
        </span>
                </h2>

                <table class="table table-bordered table-hover table-striped sortable" data-table="sort">
                    <thead class="center">
                    <tr>
                        <th>Component</th>
                        <th>Tasks</th>
                        <c:forEach var="head" items="${componentHead}">
                            <th><ct:pretty input="${head}" type="head"/></th>
                        </c:forEach>
                        <th>Error</th>
                    </tr>
                    </thead>
                    <tbody class="text-right">
                    <c:forEach var="comp" items="${componentData}">
                        <tr>
                            <td class="text-left" data-order="${comp.sortedKey}">
                                <c:choose>
                                    <c:when test="${comp.type eq 'bolt'}">
                        <span class="glyphicon glyphicon-flash" title="Bolt" data-toggle="tooltip"
                              data-placement="bottom"></span>
                                    </c:when>
                                    <c:otherwise>
                        <span class="glyphicon glyphicon-random" title="Spout" data-toggle="tooltip"
                              data-placement="bottom"></span>
                                    </c:otherwise>
                                </c:choose>
                                <a href="component?cluster=${clusterName}&topology=${topology.id}&component=${comp.componentName}"
                                   target="_blank">
                                        ${comp.componentName}
                                </a>
                                <%--<c:if test="${comp.parentComponent.size() > 0}">--%>
                                    <%--<div class="sub-comp hidden">--%>
                                        <%--<c:forEach var="parent" items="${comp.parentComponent}">--%>
                                            <%--${parent}<br/>--%>
                                        <%--</c:forEach>--%>
                                    <%--</div>--%>
                                <%--</c:if>--%>
                            </td>
                            <td class="middle">${comp.parallel}</td>
                            <c:forEach var="head" items="${componentHead}">
                                <td>${comp.metrics.get(head)}
                                    <%--<ct:submetric metric="${comp.subMetrics}"--%>
                                                  <%--parentComp="${comp.parentComponent}"--%>
                                                  <%--metricName="${head}"/>--%>
                                </td>
                            </c:forEach>
                            <td class="center"><ct:error e="${comp.errors}"/></td>
                        </tr>
                    </c:forEach>
                    </tbody>
                </table>


                <h2>User Defined Metrics</h2>
                <table class="table table-bordered table-hover table-striped sortable center" data-table="sort">
                    <thead>
                    <tr>
                        <th>Name</th>
                        <th>Component</th>
                        <th>Type</th>
                        <th>Value</th>
                    </tr>
                    </thead>
                    <tbody>
                    <c:forEach var="udm" items="${userDefinedMetrics}">
                        <tr>
                            <td>${udm.metricName}</td>
                            <td>
                                <a href="component?cluster=${clusterName}&topology=${topology.id}&component=${udm.componentName}"
                                   target="_blank">
                                        ${udm.componentName}
                                </a></td>
                            <td>${udm.type}</td>
                            <td class="text-right">${udm.value}</td>
                        </tr>
                    </c:forEach>
                    </tbody>
                </table>
            </div>
            <div role="tabpanel" class="tab-pane fade" id="worker-metrics">
                <!-- ========================================================== -->
                <!------------------------- Worker Metrics --------------------->
                <!-- ========================================================== -->
                <h2>Worker Metrics
                    <small>(Total: ${workerData.size()})</small>
                    |
        <span class="window">
            Window:
            <a href="${topo_url}&win=60#worker-tab" class="label ${cls_60}">1 min</a>
            <a href="${topo_url}&win=600#worker-tab" class="label ${cls_600}">10 min</a>
            <a href="${topo_url}&win=7200#worker-tab" class="label ${cls_7200}">2 hours</a>
            <a href="${topo_url}&win=86400#worker-tab" class="label ${cls_86400}">1 day</a>
        </span>
                </h2>
                <%--<c:if test="${workerData.size() > 10}">--%>
                <%--<span class="text-muted">Too many worker metrics, only top 10 are displayed in topology page. For more details,--%>
                <%--please refer to supervisor pages.</span>--%>
                <%--</c:if>--%>
                <table class="table table-bordered table-hover table-striped center" data-table="full">
                    <thead>
                    <tr>
                        <th>Host</th>
                        <th>Port</th>
                        <th>Netty</th>
                        <c:forEach var="head" items="${workerHead}">
                            <th><ct:pretty input="${head}" type="head"/></th>
                        </c:forEach>
                    </tr>
                    </thead>
                    <tbody>
                    <c:forEach var="worker" items="${workerData}" varStatus="index">
                        <tr>
                            <td>
                                <a href="supervisor?cluster=${clusterName}&host=${worker.host}">${worker.host}</a>
                            </td>
                            <td>${worker.port}</td>
                            <td>
                                <a href="netty?cluster=${clusterName}&topology=${topology.id}&host=${worker.host}"
                                   target="_blank">
                                    netty
                                </a>
                            </td>
                            <c:forEach var="head" items="${workerHead}">
                                <c:choose>
                                    <c:when test="${head eq 'MemoryUsed'}">
                                        <td data-order="${worker.metrics.get(head)}">
                                            <ct:pretty input="${worker.metrics.get(head)}" type="filesize"/>
                                        </td>
                                    </c:when>
                                    <c:otherwise>
                                        <td>${worker.metrics.get(head)}</td>
                                    </c:otherwise>
                                </c:choose>
                            </c:forEach>
                        </tr>
                    </c:forEach>
                    </tbody>
                </table>
            </div>

            <div role="tabpanel" class="tab-pane fade" id="task-stats">
                <!-- ========================================================== -->
                <!------------------------- Task Stats --------------------->
                <!-- ========================================================== -->
                <h2>Task Stats
                    <small>(Total: ${taskData.size()})</small>
                </h2>
                <table class="table table-bordered table-hover table-striped center" data-table="full">
                    <thead>
                    <tr>
                        <th>Task Id</th>
                        <th>Component</th>
                        <th>Type</th>
                        <th>Host</th>
                        <th>Port</th>
                        <th>Uptime</th>
                        <th>Status</th>
                        <th>Error</th>
                        <th>Log</th>
                        <th>jstack</th>
                    </tr>
                    </thead>
                    <tbody>
                    <c:forEach var="task" items="${taskData}">
                        <tr>
                            <td>
                                <a href="task?cluster=${clusterName}&topology=${topology.id}&component=${task.component}&id=${task.task_id}"
                                   target="_blank">
                                        ${task.task_id}
                                </a>
                            </td>
                            <td>
                                <a href="component?cluster=${clusterName}&topology=${topology.id}&component=${task.component}"
                                   target="_blank">
                                        ${task.component}
                                </a>
                            </td>
                            <td>${task.type}</td>
                            <td>${task.host}</td>
                            <td>${task.port}</td>
                            <td><ct:pretty type="uptime" input="${task.uptime}"/></td>
                            <td><ct:status status="${task.status}"/></td>
                            <td><ct:error e="${task.errors}"/></td>
                            <td>
                                <a href="log?cluster=${clusterName}&host=${task.host}&wport=${task.port}&port=${supervisorPort}&tid=${topology.id}"
                                   target="_blank">view log</a></td>
                            <td>
                                <a href="jstack?cluster=${clusterName}&host=${task.host}&wport=${task.port}&port=${supervisorPort}"
                                   target="_blank">view jstack</a>
                            </td>
                        </tr>
                    </c:forEach>
                    </tbody>
                </table>
            </div>
        </div>

    </div>


</div>

<jsp:include page="layout/_footer.jsp"/>
<script src="assets/js/highcharts.js"></script>
<script src="assets/js/vis.min.js"></script>
<script src="assets/js/storm.js"></script>
<script>
    $(function () {
        $('[data-toggle="popover"]').popover();
        $('[data-toggle="tooltip"]').tooltip();

        var container_width = $('.container-fluid').width();

        // we set ajax sync, to make sure thrift response in order.
        $.ajaxSetup({
            async: false
        });

        //draw metrics highcharts
        $.getJSON("api/v1/cluster/${clusterName}/topology/${topology.id}/summary/metrics", function (data) {
            var thumbChart = new ThumbChart();
            data.forEach(function (e) {
                var selector = 'div[data-id="chart-' + e.name + '"]';
                var width = (container_width / data.length) - 2;
                $(selector).highcharts("SparkLine", thumbChart.newOptions(e, width));
            });

            $("#chart-tr").toggleClass("hidden");
        });

        //init vue model
        var vm = new Vue({
            el: '#graph-event',
            data: {
                title: "",
                head: [],
                mapValue: [],
                valid: false
            }
        });

        //draw vis topology graph
        $.getJSON("api/v1/cluster/${clusterName}/topology/${topology.id}/graph", function (data) {
            if (data.error){
                $('#topology-graph').hide();
                $('#topology-graph-tips').html("<p class='text-muted'>" + data.error + "</p>");
                return;
            }else{
                data = data.data;
            }

            tableData = new VisTable().newData(data);

            var visStyle = new VisNetWork();
            var visData = visStyle.newData(data);
            var options = visStyle.newOptions({depth: data.depth, breadth: data.breadth});

            // reset topology graph width and height
            var container = document.getElementById('topology-graph');
            container.setAttribute("style", "width:" + options.width + ";height:" + options.height);

            //reset graph event style
            var event = document.getElementById('graph-event');
            var event_width = container_width - $('#topology-graph').width() - 15;
            if (event_width < 400) {
                event.setAttribute("style", "clear: both; width: 500px");
            } else {
                event_width = Math.min(500, event_width);
                event.setAttribute("style", "width:" + event_width + "px; margin-left: 10px;");
            }
            // initialize your network!
            var network = new vis.Network(container, visData, options);
            network.on("click", function (params) {
                var id = undefined;
                if (params.nodes.length > 0) {
                    id = "component-" + params.nodes[0];
                } else if (params.edges.length > 0) {
                    id = "stream-" + params.edges[0];
                }

                if (id) {
                    vm.$data = tableData[id];
                }
            });

            //do the hash , after draw the graph
            var hash = window.location.hash;
            $(hash).tab('show');
        });
    });

</script>
</body>
</html>