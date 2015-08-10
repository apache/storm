---
layout: page
title: Project Information
items:
    - 
      - "/about/integrates.html"
      - "Integrates"
    - 
      - "/about/simple-api.html"
      - "Simple API"
    - 
      - "/about/scalable.html"
      - "Scalable"
    - 
      - "/about/fault-tolerant.html"
      - "Fault tolerant"
    - 
      - "/about/guarantees-data-processing.html"
      - "Guarantees data processing"
    - 
      - "/about/multi-language.html"
      - "Use with any language"
    - 
      - "/about/deployment.html"
      - "Easy to deploy and operate"
    - 
      - "/about/free-and-open-source.html"
      - "Free and open source"
---
{% for post in page.items %}
<div class="download-block">
    <div class="row">
        <div class="col-md-3 remove-custom-padding">
            <h4><a class="{% if page.url == post[0] then %}current{% else %}{% endif %}" href="{{ post[0] }}">{{ post[1] }}</a></h4>
        </div>
        <div class="col-md-9 remove-custom-padding">
          	<div class="download-info">
            <h4>{{page.title}}</h4>
          		{{ post[1].meta }}
            </div>
        </div>
    </div>
</div>
{% endfor %}