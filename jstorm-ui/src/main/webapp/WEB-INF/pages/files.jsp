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
    <h1>Current Path:
        <span class="path">
            <c:set var="url" value="files?cluster=${clusterName}&host=${host}&port=${port}&dir=."/>
            <c:forEach items="${path}" var="p" varStatus="index">
                <c:if test="${!index.first}">
                    <c:set var="url" value="${url}/${p}"/>
                    /
                </c:if>
                <a href="${url}" class="label label-primary">${p}</a>
            </c:forEach>
        </span>
        <span class="path pull-right">
            [${host}]
        </span>
    </h1>

    <hr>

    <c:choose>
        <c:when test="${summary!=null}">
            <div class="col-md-8 col-md-offset-2 alert alert-warning" role="alert">
                <strong>Ooops!</strong> ${summary}
            </div>
        </c:when>
        <c:otherwise>
            <h2>Dirs: <small>(Total: ${dirs.size()})</small></h2>
            <table class="table table-bordered table-hover table-striped sortable"
                   data-empty="No sub directories in current directory."
                   data-table="${dirs.size() > PAGE_MAX ? "full" : "sort"}">
                <thead class="center">
                <tr>
                    <th>Dir Name</th>
                    <th>Modification Time</th>
                    <th>Size</th>
                </tr>
                </thead>
                <tbody>
                <c:forEach var="dir" items="${dirs}">
                    <tr>
                        <td>
                            <a href="files?cluster=${clusterName}&host=${host}&port=${port}&dir=${parent}/${dir.fileName}">
                                    ${dir.fileName}</a></td>
                        <td><ct:pretty input="${dir.modifyTime}" type="datetime"/></td>
                        <td data-order="${dir.size}"><ct:pretty input="${dir.size}" type="filesize"/></td>
                    </tr>
                </c:forEach>
                </tbody>
            </table>


            <h2>Files: <small>(Total: ${files.size()})</small></h2>
            <table class="table table-bordered table-hover table-striped sortable"
                   data-table="${files.size() > PAGE_MAX ? "full" : "sort"}">
                <thead class="center">
                <tr>
                    <th>File Name</th>
                    <th>Modification Time</th>
                    <th>Size</th>
                </tr>
                </thead>
                <tbody>
                <c:forEach var="file" items="${files}">
                    <tr>
                        <td>
                            <a href="log?cluster=${clusterName}&host=${host}&port=${port}&file=${file.fileName}&dir=${parent}"
                               target="_blank">${file.fileName}</a></td>
                        <td><ct:pretty input="${file.modifyTime}" type="datetime"/></td>
                        <td data-order="${file.size}"><ct:pretty input="${file.size}" type="filesize"/></td>
                    </tr>
                </c:forEach>
                </tbody>
            </table>
        </c:otherwise>
    </c:choose>


</div>

<jsp:include page="layout/_footer.jsp"/>
</body>
</html>