$(document).ready(function() {
//News Ajax Call
    $("#news-list li a, .news-internal").click(function() {
		$("#news-content").empty().append("<div id='loading' align='center'><img src='../images/loading.gif' /></div>");
		$("#news-list li a").removeClass('current');
		$(this).addClass('current');

		$.ajax({ url: this.href, success: function(html) {
				$("#news-content").empty().append(html);
			}
		});
		return false;
	});
	$("#news-content").empty().append("<div id='loading' align='center'><img src='images/loading.gif' /></div>");
	$.ajax({ url: 'news/storm0100-beta-released.html', success: function(html) { 
			$("#news-content").empty().append(html);
		} 
	});
});