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
    <!------------------------- netty summary --------------------->
    <!-- ========================================================== -->
    <h2>Netty Summary</h2>
    <table class="table table-bordered table-hover table-striped center">
        <thead>
        <tr>
            <th>Host</th>
            <th>IP</th>
            <th>Topology</th>
        </tr>
        </thead>
        <tbody>
        <tr>
            <td><ct:host ip="${host}"/></td>
            <td>${host}</td>
            <td>${topologyId}</td>
        </tr>
        </tbody>
    </table>


    <!-- ========================================================== -->
    <!------------------------- Netty Metrics --------------------->
    <!-- ========================================================== -->
    <h2>Netty Metrics
        |
        <span class="window">
            Window:
            <c:set var="netty_url"
                   value="netty?cluster=${clusterName}&topology=${topologyId}&host=${host}"/>
            <a href="${netty_url}&win=60" class="label ${cls_60}">1 min</a>
            <a href="${netty_url}&win=600" class="label ${cls_600}">10 min</a>
            <a href="${netty_url}&win=7200" class="label ${cls_7200}">2 hours</a>
            <a href="${netty_url}&win=86400" class="label ${cls_86400}">1 day</a>
        </span>
    </h2>
    <table class="table table-bordered table-hover table-striped sortable center" data-empty="${flush}"
           data-table="${nettyMetrics.size() > PAGE_MAX ? "full" : "sort"}">
        <thead>
        <tr>
            <th>From</th>
            <th>To</th>
            <c:forEach var="head" items="${nettyHead}">
                <th><ct:pretty input="${head}" type="head"/></th>
            </c:forEach>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="netty" items="${nettyMetrics}" varStatus="index">
            <tr>
                <td>
                    <c:choose>
                        <c:when test="${netty.from}">
                            <span class="text-info">${netty.fromWorker}</span>
                        </c:when>
                        <c:otherwise>
                            <span class="text-muted">${netty.fromWorker}</span>
                        </c:otherwise>
                    </c:choose>
                </td>
                <td>
                    <c:choose>
                        <c:when test="${netty.to}">
                            <span class="text-info">${netty.toWorker}</span>
                        </c:when>
                        <c:otherwise>
                            <span class="text-muted">${netty.toWorker}</span>
                        </c:otherwise>
                    </c:choose>
                </td>
                <c:forEach var="head" items="${nettyHead}">
                    <td>${netty.metrics.get(head)}</td>
                </c:forEach>
            </tr>
        </c:forEach>
        </tbody>
    </table>
    <ct:page pageSize="${pageSize}" curPage="${curPage}"
             urlBase="netty?cluster=${clusterName}&topology=${topologyId}&host=${host}&port=${port}"/>
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