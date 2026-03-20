-- #20260320_001 - PoChun Hsu - [Add]     null_ratio

-- macros/null_ratio.sql
{% macro null_ratio(model, column) %}
(
    select
        (count(*) - count({{ column }}))::float / count(*)
    from {{ model }}
)
{% endmacro %}
