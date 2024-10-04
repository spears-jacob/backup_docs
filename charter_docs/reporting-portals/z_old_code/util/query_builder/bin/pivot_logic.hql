
                  ) as map_column
          FROM {columnsTable}
        ) as derived_table
    LATERAL VIEW EXPLODE (map_column) exploded_table AS metric, value
;
