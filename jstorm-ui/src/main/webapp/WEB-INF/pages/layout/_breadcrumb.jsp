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

<ol class="breadcrumb">
    <c:if test="${clusterName != null}">
        <li><a href="clusters">Clusters</a></li>
        <c:choose>
            <c:when test="${page eq 'cluster'}">
                <li class="active">Cluster</li>
            </c:when>
            <c:otherwise>
                <li><a href="cluster?name=${clusterName}">Cluster</a></li>
                <c:choose>
                    <c:when test="${page eq 'topology'}">
                        <li class="active">Topology</li>
                    </c:when>
                    <c:otherwise>
                        <li><a href="topology?name=${clusterName}&id=${topologyId}">Topology</a></li>
                        <c:if test="${compName != null}">
                            <c:choose>
                                <c:when test="${page eq 'component'}">
                                    <li class="active">Component</li>
                                </c:when>
                                <c:otherwise>
                                    <li><a href="component?name=${clusterName}&topology=${topologyId}&component=${compName}">
                                        Component</a></li>
                                    <li class="active">Task</li>
                                </c:otherwise>
                            </c:choose>
                        </c:if>
                    </c:otherwise>
                </c:choose>
            </c:otherwise>
        </c:choose>
    </c:if>
</ol>