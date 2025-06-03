{#

{% set my_cool_string = 'wow! cool!'%}
{% set my_cool_number = 100 %}

{{ my_cool_string }} I want to write jinja for {{ my_cool_number}} years



{% set my_animals = ['lemur','wolf','panther','tardigrade']%}

{{ my_animals[0]}}

{% for i in my_animals%}

My favourite animal is the {{i}}

{% endfor %}


{% set temperature = 75%}

{% if temperature < 65%}
    Time for a cappuccino!
{% else %}
    Time for a cold brew
{% endif %}

#}

{%- set foods = ['carrot','hotdog','cucumber','bell pepper']-%}

{% for food in foods %}
    {%- if food == 'hotdog' -%}
        {%- set food_type = 'snack'-%}
    {%- else -%}
        {%- set food_type = 'vegetable'-%}
    {%- endif -%}


The humble {{ food }} is my favourite {% print(food_type) %}

{% endfor %}