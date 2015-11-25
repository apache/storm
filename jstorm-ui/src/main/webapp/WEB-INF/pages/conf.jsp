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

<html>
<head>
    <jsp:include page="layout/_head.jsp"/>
    <link href="assets/css/json.human.css" rel="stylesheet"/>
    <script src="assets/js/json.human.js"></script>
</head>
<body>
<jsp:include page="layout/_header.jsp"/>

<div class="container-fluid">
    <h2>
        ${pageTitle}
        <span class="small">
            [${subtitle}]
       </span>
    </h2>
    <hr>
    <div id="config">
        <div class="alert alert-info col-md-6 col-md-offset-3 text-center">Loading...</div>
    </div>
</div>

<jsp:include page="layout/_footer.jsp"/>
<script>
    $.getJSON("${uri}", function (json) {
        $('#config').html(JsonHuman.format(json, {showArrayIndex: false}));
    });
</script>
</body>
</html>