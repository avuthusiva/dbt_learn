{% set temp = 24 %}

{% if (temp > 35) %}
{{ 'Too hot today' }}
{% elif (temp <= 35 and temp >=25)  %}
{{ 'It cold today' }}
{% else %}
{{ 'Too cold today' }}
{% endif %}