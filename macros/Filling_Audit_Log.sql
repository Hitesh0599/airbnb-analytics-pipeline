{% macro Filling_Audit_Log(results) %}
    {% if execute %}
        {% set nodes = [] %}
        {% for res in results if res.node.resource_type in ['model', 'snapshot', 'seed']%}
            {% do nodes.append(res) %}
        {% endfor %}

        {% if nodes | length > 0 %}
            INSERT INTO {{target.database}}.AUDIT.Audit_Log values
            {% for res in nodes%}
                (
                    '{{invocation_id}}',
                    '{{res.node.name}}',
                    '{{res.node.config.materialized}}',
                    '{{res.status}}',
                    $${{(res.message | string) | replace("$$", "")}}$$,
                    {{res.execution_time or 0}},
                    current_timestamp()
                ){% if not loop.last %} , {% endif %}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}
