<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
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
    <!------------------------- supervisor summary --------------------->
    <!-- ========================================================== -->
    <h2>Supervisor Summary</h2>
    <table class="table table-bordered table-hover table-striped center">
        <thead>
        <tr>
            <th>Host</th>
            <th>IP</th>
            <th>Uptime</th>
            <th>Ports Usage</th>
            <th>Conf</th>
            <th>Logs</th>
        </tr>
        </thead>
        <tbody>
        <c:choose>
            <c:when test="${supervisor != null}">
                <tr>
                    <td><ct:host ip="${supervisor.host}"/></td>
                    <td>${supervisor.host}</td>
                    <td data-order="${supervisor.uptimeSecs}"><ct:pretty type="uptime" input="${supervisor.uptimeSecs}"/></td>
                    <td>${supervisor.numUsedWorkers} / ${supervisor.numWorkers}</td>
                    <td>
                        <a href="conf?name=${clusterName}&type=supervisor&host=${supervisor.host}" target="_blank">
                            conf
                        </a>
                    </td>
                    <td>
                        <a href="files?cluster=${clusterName}&host=${supervisor.host}&port=${supervisorPort}"
                           target="_blank">log files</a> |
                        <a href="log?cluster=${clusterName}&host=${supervisor.host}&port=${supervisorPort}&file=supervisor.log"
                           target="_blank">supervisor log</a></td>
                </tr>
            </c:when>
            <c:otherwise>
                <tr>
                    <td colspan="20">
                        No data available in table
                    </td>
                </tr>
            </c:otherwise>
        </c:choose>
        </tbody>
    </table>

    <!-- ========================================================== -->
    <!------------------------- Used Worker Summary --------------------->
    <!-- ========================================================== -->
    <h2>Used Worker Summary</h2>
    <table class="table table-bordered table-hover table-striped sortable center"
           data-table="${workerSummary.size() > PAGE_MAX ? "full" : "sort"}">
        <thead>
        <tr>
            <th>Port</th>
            <th>Netty</th>
            <th>Uptime</th>
            <th>Topology</th>
            <th>Task List</th>
            <th>Worker Log</th>
            <th>Worker JStack</th>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="worker" items="${workerSummary}">
            <tr>
                <td class="middle">${worker.port}</td>
                <td class="middle">
                    <a href="netty?cluster=${clusterName}&topology=${worker.topology}&host=${host}&port=${worker.port}">
                        netty</a></td>
                <td class="middle" data-order="${worker.uptime}"><ct:pretty type="uptime" input="${worker.uptime}"/></td>
                <td class="middle">
                    <a href="topology?cluster=${clusterName}&id=${worker.topology}" target="_blank">
                            ${worker.topology}</a></td>
                <td class="text-left">
                    <ul>
                        <c:forEach var="task" items="${worker.tasks}">
                            <li>
                                <a href="task?cluster=${clusterName}&topology=${worker.topology}&component=${task.component}&id=${task.taskId}"
                                   target="_blank">${task.component}-${task.taskId} </a></li>
                        </c:forEach>
                    </ul>
                </td>
                <td class="middle">
                    <a href="log?cluster=${clusterName}&host=${host}&port=${supervisorPort}&tid=${worker.topology}&wport=${worker.port}"
                       target="_blank">view log</a>
                </td>
                <td class="middle">
                    <a href="jstack?cluster=${clusterName}&host=${host}&port=${supervisorPort}&wport=${worker.port}"
                       target="_blank">view jstack</a>
                </td>
            </tr>
        </c:forEach>
        </tbody>
    </table>

    <!-- ========================================================== -->
    <!------------------------- Worker Metrics --------------------->
    <!-- ========================================================== -->
    <h2>Worker Metrics</h2>
    <table class="table table-bordered table-hover table-striped sortable center"
           data-table="${workerMetrics.size() > PAGE_MAX ? "full" : "sort"}">
        <thead>
        <tr>
            <th>Port</th>
            <c:forEach var="head" items="${workerHead}">
                <th><ct:pretty input="${head}" type="head"/></th>
            </c:forEach>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="worker" items="${workerMetrics}" varStatus="index">
            <tr>
                <td>${worker.port}</td>
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


    <!-- ========================================================== -->
    <!------------------------- Netty Metrics --------------------->
    <!-- ========================================================== -->
    <%--<h2>Netty Metrics--%>
    <%--|--%>
    <%--<span class="window">--%>
    <%--Window:--%>
    <%--<a href="supervisor?cluster=${clusterName}&host=${host}&win=60" class="label ${cls_60}">1 min</a>--%>
    <%--<a href="supervisor?cluster=${clusterName}&host=${host}&win=600" class="label ${cls_600}">10 min</a>--%>
    <%--<a href="supervisor?cluster=${clusterName}&host=${host}&win=7200" class="label ${cls_7200}">2 hours</a>--%>
    <%--<a href="supervisor?cluster=${clusterName}&host=${host}&win=86400" class="label ${cls_86400}">1 day</a>--%>
    <%--</span>--%>
    <%--</h2>--%>
    <%--<table class="table table-bordered table-hover table-striped sortable center">--%>
    <%--<thead>--%>
    <%--<tr>--%>
    <%--<th>From</th>--%>
    <%--<th>To</th>--%>
    <%--<th>Netty Server Transmit Time</th>--%>
    <%--</tr>--%>
    <%--</thead>--%>
    <%--<tbody>--%>
    <%--<c:choose>--%>
    <%--<c:when test="${nettyMetrics.size() > 0}">--%>
    <%--<c:forEach var="netty" items="${nettyMetrics}" varStatus="index">--%>
    <%--<tr>--%>
    <%--<td>${netty.from}</td>--%>
    <%--<td>${netty.to}</td>--%>
    <%--<td>${netty.transmit_time}</td>--%>
    <%--</tr>--%>
    <%--</c:forEach>--%>
    <%--</c:when>--%>
    <%--<c:otherwise>--%>
    <%--<tr>--%>
    <%--<td colspan="7">--%>
    <%--No records found.--%>
    <%--</td>--%>
    <%--</tr>--%>
    <%--</c:otherwise>--%>
    <%--</c:choose>--%>
    <%--</tbody>--%>
    <%--</table>--%>

</div>

<jsp:include page="layout/_footer.jsp"/>
<script>
    $(function () {
        $('[data-toggle="popover"]').popover();
        $('[data-toggle="tooltip"]').tooltip();
    });

</script>
</body>
</html>