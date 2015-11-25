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
    <!------------------------- Component Summary --------------------->
    <!-- ========================================================== -->
    <h2>
        Component Summary
        <%--<span id="more-metric" class="glyphicon glyphicon-collapse-down more-metrics" title="Show more metrics info"--%>
        <%--data-toggle="tooltip" data-placement="bottom"></span>--%>
        |
        <span class="window" id="metric">
            Window:
            <a href="component?cluster=${clusterName}&topology=${topologyId}&component=${compName}&win=60"
               class="label ${cls_60}">1 min</a>
            <a href="component?cluster=${clusterName}&topology=${topologyId}&component=${compName}&win=600"
               class="label ${cls_600}">10 min</a>
            <a href="component?cluster=${clusterName}&topology=${topologyId}&component=${compName}&win=7200"
               class="label ${cls_7200}">2 hours</a>
            <a href="component?cluster=${clusterName}&topology=${topologyId}&component=${compName}&win=86400"
               class="label ${cls_86400}">1 day</a>
        </span>
    </h2>

    <table class="table table-bordered table-hover table-striped center">
        <thead>
        <tr>
            <th>Topology</th>
            <th>Component</th>
            <th>Tasks</th>
            <c:forEach var="head" items="${compHead}">
                <th><ct:pretty input="${head}" type="head"/></th>
            </c:forEach>
            <th>Error</th>
        </tr>
        </thead>
        <tbody>
        <c:choose>
            <c:when test="${comp != null}">
                <tr>
                    <td>${topologyName}</td>
                    <td class="text-left" data-order="${comp.type}">
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
                            ${comp.componentName}
                        <c:if test="${comp.parentComponent.size() > 0}">
                            <div class="comp-sub hidden">
                                <c:forEach var="parent" items="${comp.parentComponent}">
                                    ${parent}<br/>
                                </c:forEach>
                            </div>
                        </c:if>
                    </td>
                    <td class="middle">${comp.parallel}</td>
                    <c:forEach var="head" items="${compHead}">
                        <td>${comp.metrics.get(head)}
                            <ct:submetric metric="${comp.subMetrics}" parentComp="${comp.parentComponent}"
                                          metricName="${head}" clazz="comp-sub"/>
                        </td>
                    </c:forEach>
                    <td><ct:error e="${comp.errors}"/></td>
                </tr>
            </c:when>
            <c:otherwise>
                <tr class="center">
                    <td colspan="13">
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

    <!-- ========================================================== -->
    <!------------------------- Task Stats --------------------->
    <!-- ========================================================== -->
    <h2>Task Stats</h2>
    <table class="table table-bordered table-hover table-striped sortable center"
           data-table="${tasks.size() > PAGE_MAX ? "full" : "sort"}">
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
        <c:forEach var="task" items="${tasks}">
            <tr>
                <td>
                    <a href="task?cluster=${clusterName}&topology=${topologyId}&component=${task.component}&id=${task.task_id}"
                       target="_blank">
                            ${task.task_id}
                    </a>
                </td>
                <td>${task.component}</td>
                <td>${task.type}</td>
                <td>
                    <a href="supervisor?cluster=${clusterName}&host=${task.host}">
                            ${task.host}</a></td>
                <td>${task.port}</td>
                <td><ct:pretty type="uptime" input="${task.uptime}"/></td>
                <td><ct:status status="${task.status}"/></td>
                <td><ct:error e="${task.errors}"/></td>
                <td>
                    <a href="log?cluster=${clusterName}&host=${task.host}&wport=${task.port}&port=${supervisorPort}&tid=${topologyId}"
                       target="_blank">view log</a></td>
                <td>
                    <a href="jstack?cluster=${clusterName}&host=${task.host}&wport=${task.port}&port=${supervisorPort}"
                       target="_blank">view jstack</a>
                </td>
            </tr>
        </c:forEach>

        </tbody>
    </table>
    <!-- ========================================================== -->
    <!------------------------- Task Metrics --------------------->
    <!-- ========================================================== -->
    <h2>Task Metrics
        <%--<span id="more-metrics" class="glyphicon glyphicon-collapse-down more-metrics" title="Show more metrics info"--%>
        <%--data-toggle="tooltip" data-placement="bottom"></span>--%>
    </h2>
    <table class="table table-bordered table-hover table-striped sortable"
           data-table="${taskData.size() > PAGE_MAX ? "full" : "sort"}">
        <thead class="center">
        <tr>
            <th>Task</th>
            <c:forEach var="head" items="${taskHead}">
                <th><ct:pretty input="${head}" type="head"/></th>
            </c:forEach>
        </tr>
        </thead>
        <tbody class="text-right">
        <c:forEach var="t" items="${taskData}">
            <tr>
                <td class="text-center">
                    <a href="task?cluster=${clusterName}&topology=${topologyId}&component=${t.componentName}&id=${t.taskId}"
                       target="_blank">
                            ${t.taskId}</a>
                    <c:if test="${t.parentComponent.size() > 0}">
                        <div class="sub-comp hidden">
                            <c:forEach var="parent" items="${t.parentComponent}">
                                ${parent}<br/>
                            </c:forEach>
                        </div>
                    </c:if>
                </td>
                <c:forEach var="head" items="${taskHead}">
                    <td>${t.metrics.get(head)}
                        <ct:submetric metric="${t.subMetrics}" parentComp="${t.parentComponent}"
                                      metricName="${head}"/>
                    </td>
                </c:forEach>
            </tr>
        </c:forEach>
        </tbody>
    </table>

</div>

<jsp:include page="layout/_footer.jsp"/>
<script>
    $(function () {
        $('[data-toggle="popover"]').popover();
        $('[data-toggle="tooltip"]').tooltip();

        $('#more-metrics').click(function () {
            if ($(this).hasClass('glyphicon-collapse-down')) {
                $('.sub-comp').removeClass('hidden');
                $(this).removeClass('glyphicon-collapse-down');
                $(this).addClass('glyphicon-collapse-up');
                $(this).attr("data-original-title", "Hidden detail metrics info");
            } else {
                $('.sub-comp').addClass('hidden');
                $(this).removeClass('glyphicon-collapse-up');
                $(this).addClass('glyphicon-collapse-down');
                $(this).attr("data-original-title", "Show detail metrics info");
            }
        });

        $('#more-metric').click(function () {
            if ($(this).hasClass('glyphicon-collapse-down')) {
                $('.comp-sub').removeClass('hidden');
                $(this).removeClass('glyphicon-collapse-down');
                $(this).addClass('glyphicon-collapse-up');
                $(this).attr("data-original-title", "Hidden detail metrics info");
            } else {
                $('.comp-sub').addClass('hidden');
                $(this).removeClass('glyphicon-collapse-up');
                $(this).addClass('glyphicon-collapse-down');
                $(this).attr("data-original-title", "Show detail metrics info");
            }
        });

    });

</script>
</body>
</html>