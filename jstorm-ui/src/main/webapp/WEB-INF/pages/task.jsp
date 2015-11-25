<%@ taglib prefix="spring" uri="http://www.springframework.org/tags" %>
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
    <!------------------------- Task summary --------------------->
    <!-- ========================================================== -->
    <h2>Task Summary
        |
        <span class="window">
            Window:
            <a href="task?cluster=${clusterName}&topology=${topologyId}&component=${compName}&id=${task.task_id}&win=60"
               class="label ${cls_60}">1 min</a>
            <a href="task?cluster=${clusterName}&topology=${topologyId}&component=${compName}&id=${task.task_id}&win=600"
               class="label ${cls_600}">10 min</a>
            <a href="task?cluster=${clusterName}&topology=${topologyId}&component=${compName}&id=${task.task_id}&win=7200"
               class="label ${cls_7200}">2 hours</a>
            <a href="task?cluster=${clusterName}&topology=${topologyId}&component=${compName}&id=${task.task_id}&win=86400"
               class="label ${cls_86400}">1 day</a>
        </span>
    </h2>
    <table class="table table-bordered table-hover table-striped center" data-empty="${flush}">
        <thead>
        <tr>
            <th>Task Id</th>
            <th>Component</th>
            <th>Worker</th>
            <th>Uptime</th>
            <th>Status</th>
        </tr>
        </thead>
        <tbody>
        <c:if test="${task != null}">
            <tr>
                <td>${task.task_id}</td>
                <td><a href="component?cluster=${clusterName}&topology=${topologyId}&component=${task.component}">
                        ${task.component}</a></td>
                <td>${task.host}:${task.port}</td>
                <td><ct:pretty type="uptime" input="${task.uptime}"/></td>
                <td><ct:status status="${task.status}"/></td>
            </tr>
        </c:if>
        </tbody>
    </table>

    <!-- ========================================================== -->
    <!------------------------- Task Metrics --------------------->
    <!-- ========================================================== -->
    <h2>Task Metrics
    </h2>
    <table class="table table-bordered table-hover table-striped">
        <thead class="center">
        <tr>
            <th>Task Id</th>
            <c:forEach var="head" items="${taskHead}">
                <th><ct:pretty input="${head}" type="head"/></th>
            </c:forEach>
        </tr>
        </thead>
        <tbody class="text-right">
        <c:choose>
            <c:when test="${taskMetric != null}">
                <tr>
                    <td class="text-center">
                            ${taskMetric.taskId}
                        <c:if test="${taskMetric.parentComponent.size() > 0}">
                            <div class="sub-comp">
                                <c:forEach var="parent" items="${taskMetric.parentComponent}">
                                    ${parent}<br/>
                                </c:forEach>
                            </div>
                        </c:if>
                    </td>

                    <c:forEach var="head" items="${taskHead}">
                        <c:set var="isHidden"
                               value="${taskMetric.subMetrics.size() == 0 || !taskMetric.metrics.containsKey(head)}"/>
                        <td>${taskMetric.metrics.get(head)}
                            <ct:submetric metric="${taskMetric.subMetrics}" parentComp="${taskMetric.parentComponent}"
                                          metricName="${head}" isHidden="${isHidden}"/>
                        </td>
                    </c:forEach>
                </tr>
            </c:when>
            <c:otherwise>
                <tr class="center">
                    <td colspan="20">
                        No data available in table
                    </td>
                </tr>
            </c:otherwise>
        </c:choose>
        </tbody>
    </table>

    <!-- ========================================================== -->
    <!------------------------- Stream Metrics --------------------->
    <!-- ========================================================== -->
    <h2>
        Stream Metrics
    </h2>
    <table class="table table-bordered table-hover table-striped sortable"
           data-table="${streamData.size() > PAGE_MAX ? "full" : "sort"}">
        <thead class="center">
        <tr>
            <th>StreamId</th>
            <c:forEach var="head" items="${streamHead}">
                <th><ct:pretty input="${head}" type="head"/></th>
            </c:forEach>
        </tr>
        </thead>
        <tbody class="text-right">
        <c:forEach var="comp" items="${streamData}">
            <tr>
                <td class="stream" data-order="${comp.streamId}">
                    <span>${comp.streamId}</span>
                    <c:if test="${comp.parentComponent.size() > 0}">
                        <a class="detail" title="show parent stream metrics" aria-controls="stream-${comp.streamId}">+detail</a>
                        <div aria-label="stream-${comp.streamId}" class="sub-comp" style="display: none;">
                            <c:forEach var="parent" items="${comp.parentComponent}">
                                ${parent}<br/>
                            </c:forEach>
                        </div>
                    </c:if>
                </td>
                <c:forEach var="head" items="${streamHead}">
                    <td>${comp.metrics.get(head)}
                        <c:set var="isHidden" value="${comp.subMetrics.size() == 0 || !comp.metrics.containsKey(head)}"/>
                        <div aria-label="stream-${comp.streamId}" style="display: none;">
                            <ct:submetric metric="${comp.subMetrics}" parentComp="${comp.parentComponent}"
                                          metricName="${head}" isHidden="${isHidden}"/>
                        </div>
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
        $(".detail").click(function () {
            var aria = $(this).attr("aria-controls");
            var selector = 'div[aria-label="' + aria + '"]';
            console.log(selector);
            $(selector).fadeToggle("slow");
            if ($(this).text() == '+detail') {
                $(this).attr("title", "hide parent stream metrics");
                $(this).text('-detail');
            } else {
                $(this).attr("title", "show parent stream metrics");
                $(this).text('+detail');
            }
        });
    });
</script>
</body>
</html>