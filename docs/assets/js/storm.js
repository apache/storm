$(document).ready(function() {
	//Scroll to Top
	$(".totop").hide();
	$(window).scroll(function(){
		if ($(this).scrollTop() > 300) {
			$('.totop').fadeIn();
		} else {
			$('.totop').fadeOut();
		}
	});
	$(".totop a").click(function(e) {
		e.preventDefault();
		$("html, body").animate({ scrollTop: 0 }, "slow");
		return false;
	});
	
    //Fixed Navigation
    $('.navbar').affix({
        offset: {
            top: $('header').height()
        }
    });

    //Owl Carousel For CLient List
	$("#owl-example").owlCarousel({
		items: 8
	});	

	$(".navbar li a").each(function() {
		if(document.URL.includes(this.getAttribute('href')))
		{
			$(".navbar li a").removeClass('current');
			$(this).addClass('current');
		}
		if(document.URL.includes('/documentation/'))
		{
			$(".navbar li a").removeClass('current');
			$('#documentation').addClass('current');
		}
		if(document.URL.includes('/contribute/'))
		{
			$(".navbar li a").removeClass('current');
			$('#contribute').addClass('current');
		}

	});
	$(".news li a").each(function() {
		if(document.URL.includes(this.getAttribute('href')) && !($("#news-list li a").hasClass('current'))){
			$("#news-list li a").removeClass('current');
			$(this).addClass('current');
		}
	});

});;

function isMobile() {
	    if (sessionStorage.desktop)
	        return false;
	    else if (localStorage.mobile)
	        $('#twitter_widget').hide();

	    var mobile = ['iphone','ipad','android','blackberry','nokia','opera mini','windows mobile','windows phone','iemobile']; 
	    for (var i in mobile) 
	    	if (navigator.userAgent.toLowerCase().indexOf(mobile[i].toLowerCase()) > 0) 
	    		$('#twitter_widget').hide();
	    return false;
	};