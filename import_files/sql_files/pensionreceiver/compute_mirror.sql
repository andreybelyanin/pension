INSERT INTO ${ods_schema_name}.pensionreceiver
                 SELECT nk,
                        dws_job,
                        deleted_flag,
                        default_flag,
                        insert_date,
                        gn,
                        "nm/f",
                        "nm/i",
                        "nm/o",
                        dc,
                        ad,
                        hash,
                        table_name,
                        hash_table,
                        hash_dc
FROM (SELECT *, row_number() over (partition by hash_dc order by insert_date desc) as row_num
      FROM (SELECT nk,
                    dws_job,
                    deleted_flag,
                    default_flag,
                    insert_date,
                    gn,
                    "nm/f",
                    "nm/i",
                    "nm/o",
                    dc,
                    ad,
                    hash,
                    table_name,
                    hash_table,
                    hash_dc
               FROM ${ods_schema_name}.pensionreceiver
               UNION ALL
               SELECT
                   nk,
                   '${dws_job}' as dws_job,
                      CASE WHEN (dws_act = 'D') THEN 'Y' ELSE 'N' END as deleted_flag,
                      'N' as default_flag,
                      '${insert_date}' as insert_date,
                      gn,
                      "nm/f",
                      "nm/i",
                      "nm/o",
                      dc,
                      ad,
                      hash,
                      table_name,
                      hash_table,
                      hash_dc
               FROM ${ods_schema_name}.pensionreceiver_delta WHERE date = '${insert_date_delta}'
           ) union_delta_mirror) mirror
            where row_num = 1;
