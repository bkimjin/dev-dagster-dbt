{% macro copy_into_stage(stage_name, file_path, format) %}

    {%- if target.name|upper == 'PROD' -%}

        {% set row_count_query %}
            select count(*) as row_count 
            from ( {{ this }} )
            where loaded_to_s3 = 0
        {% endset %}
    
        {% set row_count = run_query(row_count_query) %}
            {% if row_count %}
                {% set num_rows = row_count.columns[0].values()[0] %}
            {% else %}
                {% set num_rows = 0 %}
            {% endif %}
        
        {% if num_rows > 0 %}

            {% set copy_into_sql %}
                copy into @{{ stage_name }}/{{ file_path }}/{{ run_started_at.strftime('%Y_%m_%d') }}/
                from (
                    select * exclude (loaded_to_s3)
                    from {{ this }} 
                    where loaded_to_s3 = 0
                )
                file_format = (TYPE = {{ format }})
            {% endset %}

            {{ log("Executing SQL: " ~ copy_into_sql, info=False) }}
            {% do run_query(copy_into_sql) %}

            {% set update_sql %}
                update {{ this }} 
                set loaded_to_s3 = 1
                where loaded_to_s3 = 0 or loaded_to_s3 is null
            {% endset %}

            {{ log("Executing SQL: " ~ update_sql, info=False) }}
            {% do run_query(update_sql) %}

        {% else %}

            {{ log("Skipped write to stage - no new data.", info=True) }}

        {%- endif -%}

    {%- else -%}

        {{ log("Skipped write to stage - not in prod environment.", info=True) }}

    {%- endif -%}

{% endmacro %}