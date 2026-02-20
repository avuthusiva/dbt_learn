{% set cols = ['empno','ename','job','hiredate','mgr','sal','comm','deptno']  %}
select 
{% for col in cols -%}
{%- if (loop.last) -%}
{{ col -}}
{% else -%}
{{ col -}},
{% endif -%}
{% endfor %}
from emp