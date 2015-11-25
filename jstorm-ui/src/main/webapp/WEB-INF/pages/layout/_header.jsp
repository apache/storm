<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
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

<nav class="navbar navbar-default navbar-static-top">
    <div class="container-fluid">
        <div class="navbar-header">
            <a class="navbar-brand" href="${pageContext.request.contextPath}/">
                <img alt="JStorm" src="assets/imgs/jstorm.png" id="logo">
            </a>
        </div>
        <!-- Collect the nav links, forms, and other content for toggling -->
        <div class="collapse navbar-collapse" id="bs-example-navbar-collapse-1">
            <ul class="nav navbar-nav">
                <c:if test="${clusterName != null}">
                    <%--<li><a href="/">Clusters</a></li>--%>
                    <c:choose>
                        <c:when test="${page eq 'cluster'}">
                            <li class="active"><a href="#" onclick="location.reload()">Cluster</a></li>
                        </c:when>
                        <c:otherwise>
                            <li><a href="cluster?name=${clusterName}">Cluster</a></li>
                            <c:choose>
                                <c:when test="${page eq 'topology'}">
                                    <li class="active"><a href="#" onclick="location.reload()">Topology</a></li>
                                </c:when>
                                <c:otherwise>
                                    <c:if test="${topologyId != null}">
                                        <li><a href="topology?cluster=${clusterName}&id=${topologyId}">Topology</a>
                                        </li>
                                        <c:if test="${compName != null}">
                                            <c:choose>
                                                <c:when test="${page eq 'component'}">
                                                    <li class="active"><a href="#" onclick="location.reload()">Component</a></li>
                                                </c:when>
                                                <c:otherwise>
                                                    <li>
                                                        <a href="component?name=${clusterName}&topology=${topologyId}&component=${compName}">
                                                            Component</a></li>
                                                    <li class="active"><a href="#" onclick="location.reload()">Task</a></li>
                                                </c:otherwise>
                                            </c:choose>
                                        </c:if>
                                    </c:if>
                                    <c:if test="${page eq 'supervisor'}">
                                        <li class="active"><a href="#" onclick="location.reload()">Supervisor</a></li>

                                    </c:if>
                                    <c:if test="${page eq 'netty'}">
                                        <li><a href="supervisor?cluster=${clusterName}&host=${host}">
                                            Supervisor</a></li>
                                        <li class="active"><a href="#" onclick="location.reload()">Netty</a> </li>
                                    </c:if>
                                </c:otherwise>
                            </c:choose>
                            <c:if test="${page eq 'conf'}">
                                <li class="active"><a href="#" onclick="location.reload()">Configuration</a> </li>
                            </c:if>
                        </c:otherwise>
                    </c:choose>
                </c:if>
            </ul>
            <!-- /.navbar-collapse -->
        </div>
    </div>
</nav>

<c:if test="${error != null}">
    <pre>
        ${error}
    </pre>
</c:if>
