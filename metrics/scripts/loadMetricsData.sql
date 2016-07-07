CREATE OR REPLACE FUNCTION metrics_compute(python_filename text, metrics_schema text, start_offset integer, end_offset integer, row_view_schema text) returns integer as $$
DECLARE
   metrics_command text := 'python ' || python_filename || ' metricsCompute -metricsSchema ' || metrics_schema || ' -rowViewSchema ' || row_view_schema || ' -copyFromProgram -startOffset ' || start_offset || ' -endOffset ' || end_offset;
   
BEGIN 
execute 'copy ' || metrics_schema || '.metrics from program ' ||  quote_literal(metrics_command);
return 1;
END;
$$ LANGUAGE plpgsql;

\set python_filename '\'' :pythonFilename '\''
\set metrics_schema '\'' :metricsSchema '\''
\set row_view_schema '\'' :rowViewSchema '\''
select metrics_compute(:python_filename, :metrics_schema, :startOffset, :endOffset, :row_view_schema);

