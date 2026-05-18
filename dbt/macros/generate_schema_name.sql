{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {#- When custom_schema_name is specified, use it exactly as-is without prefixing -#}
        {{ custom_schema_name }}

    {%- endif -%}

{%- endmacro %}

