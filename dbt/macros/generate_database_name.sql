{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- if target.name|upper != 'PROD' -%}

        {%- set default_database = 'NDR_DEV' -%}

        {{ default_database }}

    {%- else -%}

        {%- set default_database = target.database -%}
        {%- if custom_database_name is none -%}

            {{ default_database }}

        {%- else -%}

            {{ custom_database_name | trim }}

        {%- endif -%}

    {%- endif -%}

{%- endmacro %}