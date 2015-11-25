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
    <!------------------------- clusters summary --------------------->
    <!-- ========================================================== -->
    <h1>Clusters Summary
        <small>clusters:${clusters.size()} nodes:${nodes}</small>
    </h1>
    <table class="table table-bordered table-hover table-striped sortable center"
           data-table="${clusters.size() > PAGE_MAX ? "full" : "sort"}">
        <thead>
        <tr>
            <th>Cluster Name</th>
            <th>Supervisors</th>
            <th>Ports Usage</th>
            <th>Topologies</th>
            <th>Version</th>
            <th>Conf</th>
        </tr>
        </thead>
        <tbody>
        <c:forEach var="c" items="${clusters}" varStatus="index">
            <tr>
                <td><a href="cluster?name=${c.name}">${c.name}</a></td>
                <td>${c.supervisor_num}</td>
                <td>${c.used_ports} / ${c.total_ports}</td>
                <td>${c.topology_num}</td>
                <td>${c.version}</td>
                <td><a href="conf?name=${c.name}" target="_blank">conf</a></td>
            </tr>
        </c:forEach>
        </tbody>
    </table>

</div>

<jsp:include page="layout/_footer.jsp"/>
</body>
</html>