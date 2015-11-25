<%@ taglib prefix="spring" uri="http://www.springframework.org/tags" %>
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

<script>
    $(function () {
        $("table[data-table='sort']").each(function () {
            var empty = $(this).attr("data-empty") ? $(this).attr("data-empty") : "No data available in table";
            $(this).DataTable({
                "info": false,
                "paging": false,
                "searching": false,
                "order": [],
                "language": {
                    "emptyTable": empty
                }
            });
        });
        $("table[data-table='full']").each(function () {
            $(this).DataTable({
                "info": true,
                "paging": true,
                "ordering": true,
                "searching": true,
                "order": [],
                "lengthMenu": [[15, 25, 50, -1], [15, 25, 50, "ALL"]]
            });
        });
        $(".pop").popover({
            container: 'body',
            trigger: "manual",
            html: true,
            placement: "bottom",
            content: function () {
                return $(this).next('.pop-content').html();
            }
        }).on("mouseenter", function () {
            var _this = this;
            $(this).popover("show");
            $(".popover").on("mouseleave", function () {
                $(_this).popover('hide');
            });
        }).on("mouseleave", function () {
            var _this = this;
            setTimeout(function () {
                if (!$(".popover:hover").length) {
                    $(_this).popover("hide");
                }
            }, 300);
        });

    });
</script>