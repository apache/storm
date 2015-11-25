<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="spring" uri="http://www.springframework.org/tags" %>
<%@ taglib prefix="ct" uri="http://jstorm.alibaba.com/jsp/tags" %>
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
</head>
<body>
<jsp:include page="layout/_header.jsp"/>

<div class="container-fluid">
    <!-- ========================================================== -->
    <!------------------------- nimbus summary --------------------->
    <!-- ========================================================== -->
    <h2>Nimbus Summary</h2>
    <table class="table table-bordered table-hover table-striped center">
        <thead>
        <tr>
            <th>Role</th>
            <th>Host</th>
            <th>Uptime</th>
            <th>Supervisors</th>
            <th>Ports Usage</th>
            <th>Tasks</th>
            <th>Topologies</th>
            <th>Version</th>
            <th>Conf</th>
            <th>Logs</th>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="nb" items="${nimbus}" varStatus="index">
            <tr>
                <c:choose>
                    <c:when test="${index.first}">
                        <td>Nimbus Master</td>
                    </c:when>
                    <c:otherwise>
                        <td>Nimbus Slave</td>
                    </c:otherwise>
                </c:choose>
                <td>${nb.host}</td>
                <td>${nb.uptime_secs}</td>
                <c:if test="${index.first}">
                    <td rowspan="${nimbus.size()}" class="middle">${nb.supervisor_num}</td>
                    <td rowspan="${nimbus.size()}" class="middle">${nb.used_port_num} / ${nb.total_port_num}</td>
                    <td rowspan="${nimbus.size()}" class="middle">${nb.task_num}</td>
                    <td rowspan="${nimbus.size()}" class="middle">${nb.topology_num}</td>
                </c:if>
                <td>${nb.version}</td>
                <td><a href="conf?name=${clusterName}" target="_blank">conf</a></td>
                <td>
                    <a href="files?cluster=${clusterName}&host=${nimbus.get(0).ip}&port=${nimbusPort}"
                       target="_blank">log files</a> |
                    <a href="log?cluster=${clusterName}&host=${nimbus.get(0).ip}&port=${nimbusPort}&file=nimbus.log"
                       target="_blank">nimbus log</a></td>
            </tr>
        </c:forEach>
        </tbody>
    </table>

    <c:if test="${clusterData != null}">
        <!-- ========================================================== -->
        <!------------------------- nimbus stats --------------------->
        <!-- ========================================================== -->
        <h2>Nimbus Stats</h2>
        <table class="table table-bordered table-striped center text-wrap">
            <thead class="center">
            <tr>
                <c:forEach var="head" items="${clusterHead}">
                    <th><ct:pretty input="${head}" type="head"/></th>
                </c:forEach>
            </tr>
            </thead>
            <tbody>
            <c:choose>
                <c:when test="${clusterData != null}">
                    <tr>
                        <c:forEach var="head" items="${clusterHead}">
                            <c:choose>
                                <c:when test="${head eq 'MemoryUsed'}">
                                    <td data-order="${clusterData.metrics.get(head)}">
                                        <ct:pretty input="${clusterData.metrics.get(head)}"
                                                   type="filesize"/>
                                    </td>
                                </c:when>
                                <c:otherwise>
                                    <td>${clusterData.metrics.get(head)}</td>
                                </c:otherwise>
                            </c:choose>
                        </c:forEach>
                    </tr>
                    <tr id="chart-tr" class="hidden">
                        <c:forEach var="head" items="${clusterHead}">
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

    <!-- ========================================================== -->
    <!------------------------- topology summary --------------------->
    <!-- ========================================================== -->
    <h2>Topology Summary</h2>
    <table class="table table-bordered table-hover table-striped sortable center"
           data-table="${topologies.size() > PAGE_MAX ? "full" : "sort"}">
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
        <c:forEach var="topo" items="${topologies}">
            <tr>
                <td><a href="topology?id=${topo.id}&cluster=${clusterName}">${topo.name}</a></td>
                <td>${topo.id}</td>
                <td><ct:status status="${topo.status}"/></td>
                <td><ct:pretty type="uptime" input="${topo.uptimeSecs}"/></td>
                <td>${topo.numWorkers}</td>
                <td>${topo.numTasks}</td>
                <td>
                    <a href="conf?name=${clusterName}&type=topology&topology=${topo.id}" target="_blank">
                        conf
                    </a>
                </td>
                <td>
                    <c:choose>
                        <c:when test="${topo.errorInfo != null}">
                            <a href="topology?id=${topo.id}&cluster=${clusterName}#comp-tab"
                               title='See topology page for detail'
                               class="error-msg">${topo.errorInfo}</a>
                        </c:when>
                        <c:otherwise>
                            <!-- nothing -->
                        </c:otherwise>
                    </c:choose>
                </td>
            </tr>
        </c:forEach>
        </tbody>
    </table>

    <!-- ========================================================== -->
    <!------------------------- supervisor summary --------------------->
    <!-- ========================================================== -->
    <h2>Supervisor Summary
        <small>(Total: ${supervisors.size()})</small>
    </h2>
    <table class="table table-bordered table-hover table-striped sortable center"
           data-table="${supervisors.size() > PAGE_MAX ? "full" : "sort"}">
        <thead>
        <tr>
            <th>IP</th>
            <th>Host</th>
            <th>Uptime</th>
            <th>Ports Usage</th>
            <th>Conf</th>
            <th>Logs</th>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="sv" items="${supervisors}">
            <tr>
                <td><a href="supervisor?cluster=${clusterName}&host=${sv.host}">
                        ${sv.host}</a></td>
                <td><ct:host ip="${sv.host}"/></td>
                <td><ct:pretty type="uptime" input="${sv.uptimeSecs}"/></td>
                <td>${sv.numUsedWorkers} / ${sv.numWorkers}</td>
                <td>
                    <a href="conf?name=${clusterName}&type=supervisor&host=${sv.host}" target="_blank">
                        conf
                    </a>
                </td>
                <td>
                    <a href="files?cluster=${clusterName}&host=${sv.host}&port=${supervisorPort}"
                       target="_blank">log files</a> |
                    <a href="log?cluster=${clusterName}&host=${sv.host}&port=${supervisorPort}&file=supervisor.log"
                       target="_blank">supervisor log</a></td>
            </tr>
        </c:forEach>
        </tbody>
    </table>
    <!-- ========================================================== -->
    <!------------------------- zookeeper summary --------------------->
    <!-- ========================================================== -->
    <h2>Zookeeper Summary</h2>
    <table class="table table-bordered table-hover table-striped center">
        <thead>
        <tr>
            <th>IP</th>
            <th>Host</th>
            <th>Port</th>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="zk" items="${zkServers}" varStatus="index">
            <tr>
                <td>${zk.ip}</td>
                <td>${zk.host}</td>
                <c:if test="${index.first}">
                    <td rowspan="${zkServers.size()}" style="vertical-align: middle;">
                            ${zk.port}
                    </td>
                </c:if>
            </tr>
        </c:forEach>
        </tbody>
    </table>
</div>

<jsp:include page="layout/_footer.jsp"/>
<script src="assets/js/highcharts.js"></script>
<script src="assets/js/storm.js"></script>
<script>
    $(function () {
        $('[data-toggle="tooltip"]').tooltip();

        //draw metrics highcharts
        $.getJSON("api/v1/cluster/${clusterName}/summary/metrics", function (data) {
            var thumbChart = new ThumbChart();
            data.forEach(function (e) {
                var selector = 'div[data-id="chart-' + e.name + '"]';
                var width = ($('.container-fluid').width() / data.length) - 2;
                $(selector).highcharts("SparkLine", thumbChart.newOptions(e, width));
            });

            $("#chart-tr").toggleClass("hidden");
        });
    });
</script>
</body>
</html>