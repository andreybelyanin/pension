INSERT INTO ${ods_schema_name}.pensionreceiver_delta
SELECT
    nk,
    dws_job,
    dws_act,
    dws_uniact,
    insert_date,
    deleted_flag,
    default_flag,
    gn,
    "nm/f",
    "nm/i",
    "nm/o",
    dc,
    ad,
    hash,
    table_name,
    hash_table,
    hash_dc,
    '${date}' as date
FROM (SELECT *, row_number() over (partition by hash_dc order by insert_date desc) as row_num FROM
(SELECT
        nk,
        dws_job,
        dws_act,
        dws_uniact,
        insert_date,
        deleted_flag,
        default_flag,
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
                     FROM (SELECT UUID_GENERATE()                             AS nk,
                                  '${dws_job}'                                AS dws_job,
                                  '${insert_date}'                            AS insert_date,
                                  'I'                                         AS dws_uniact,
                                   mirror.default_flag                        AS default_flag,
                                   mirror.deleted_flag                        AS deleted_flag,

                         CASE
                                WHEN ((mirror.hash_dc is null) or (mirror.hash_dc = src.hash_dc and
                                    mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                WHEN (src.hash_dc is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                WHEN (mirror.deleted_flag = 'N') and
                                     ((src.gn <=> mirror.gn) = FALSE or
                                     (src."nm/f" <=> mirror."nm/f") = FALSE or
                                     (src."nm/i" <=> mirror."nm/i") = FALSE or
                                     (src."nm/o" <=> mirror."nm/o") = FALSE or
                                     (src.dc <=> mirror.dc) = FALSE or
                                     (src.ad <=> mirror.ad) = FALSE or
                                     (src.hash <=> mirror.hash) = FALSE or
                                     (src.table_name <=> mirror.table_name) = FALSE or
                                     (src.hash_table <=> mirror.hash_table) = FALSE)
                                        THEN 'U'
                                 ELSE null
                             END                                                AS dws_act,

                         CASE
                                WHEN (src.gn is not null)
                                    THEN src.gn
                                WHEN (mirror.gn is not null)
                                    THEN mirror.gn
                                ELSE null
                         END                                                         AS gn,

                         CASE
                                WHEN (src."nm/f" is not null)
                                    THEN src."nm/f"
                                WHEN (mirror."nm/f" is not null)
                                    THEN mirror."nm/f"
                                ELSE null
                         END                                                       AS "nm/f",

                         CASE
                                WHEN (src."nm/i" is not null)
                                    THEN src."nm/i"
                                WHEN (mirror."nm/i" is not null)
                                    THEN mirror."nm/i"
                                ELSE null
                         END                                                        AS "nm/i",

                         CASE
                                WHEN (src."nm/o" is not null)
                                    THEN src."nm/o"
                                WHEN (mirror."nm/o" is not null)
                                    THEN mirror."nm/o"
                                ELSE null
                         END                                                         AS "nm/o",

                         CASE
                                WHEN (src.dc is not null)
                                    THEN src.dc
                                WHEN (mirror.dc is not null)
                                    THEN mirror.dc
                                ELSE null
                         END                                                            AS dc,

                         CASE
                                WHEN (src.ad is not null)
                                    THEN src.ad
                                WHEN (mirror.ad is not null)
                                    THEN mirror.ad
                                ELSE null
                         END                                                            AS ad,

                         CASE
                                WHEN (src.hash is not null)
                                    THEN src.hash
                                WHEN (mirror.hash is not null)
                                    THEN mirror.hash
                                ELSE null
                         END                                                            AS hash,

                         CASE
                                WHEN (src.table_name is not null)
                                    THEN src.table_name
                                WHEN (mirror.table_name is not null)
                                    THEN mirror.table_name
                                ELSE null
                         END                                                            AS table_name,

                         CASE
                                WHEN (src.hash_table is not null)
                                    THEN src.hash_table
                                WHEN (mirror.hash_table is not null)
                                    THEN mirror.hash_table
                                ELSE null
                         END                                                            AS hash_table,

                         CASE
                                WHEN (src.hash_dc is not null)
                                    THEN src.hash_dc
                                WHEN (mirror.hash_dc is not null)
                                    THEN mirror.hash_dc
                                ELSE null
                         END                                                            AS hash_dc

                         FROM ${ods_schema_name}.pensionreceiver_dsrc AS src
                         FULL OUTER JOIN ${ods_schema_name}.pensionreceiver
                         AS mirror ON src.hash_dc = mirror.hash_dc) delta) delta_num) rsl WHERE row_num = 1;

INSERT INTO ${ods_schema_name}.pensionreceiver_double
                    SELECT
                            dws_job,
                            insert_date,
                            effective_flag,
                            algorithm_ncode,
                            nk,
                            deleted_flag,
                            default_flag,
                            gn,
                            "nm/f",
                            "nm/i",
                            "nm/o",
                            dc,
                            ad,
                            hash,
                            table_name,
                            hash_table,
                            hash_dc FROM
(SELECT *, row_number() over (partition by hash_dc order by insert_date desc) as row_num FROM
    (SELECT
        nk,
        dws_job,
        dws_act,
        dws_uniact,
        insert_date,
        deleted_flag,
        default_flag,
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
                     FROM (SELECT UUID_GENERATE()                             AS nk,
                                  '${dws_job}'                                AS dws_job,
                                  '${insert_date}'                            AS insert_date,
                                  'I'                                         AS dws_uniact,
                                   mirror.default_flag                        AS default_flag,
                                   mirror.deleted_flag                        AS deleted_flag,

                         CASE
                                WHEN ((mirror.hash_dc is null) or (mirror.hash_dc = src.hash_dc and
                                    mirror.deleted_flag = 'Y' and '${insert_date}' > mirror.insert_date))
                                     THEN 'I'
                                WHEN (src.hash_dc is null and mirror.deleted_flag = 'N')
                                     THEN 'D'
                                WHEN (mirror.deleted_flag = 'N') and
                                     ((src.gn <=> mirror.gn) = FALSE or
                                     (src."nm/f" <=> mirror."nm/f") = FALSE or
                                     (src."nm/i" <=> mirror."nm/i") = FALSE or
                                     (src."nm/o" <=> mirror."nm/o") = FALSE or
                                     (src.dc <=> mirror.dc) = FALSE or
                                     (src.ad <=> mirror.ad) = FALSE or
                                     (src.hash <=> mirror.hash) = FALSE or
                                     (src.table_name <=> mirror.table_name) = FALSE or
                                     (src.hash_table <=> mirror.hash_table) = FALSE)
                                        THEN 'U'
                                 ELSE null
                             END                                                AS dws_act,

                         CASE
                                WHEN (src.gn is not null)
                                    THEN src.gn
                                WHEN (mirror.gn is not null)
                                    THEN mirror.gn
                                ELSE null
                         END                                                         AS gn,

                         CASE
                                WHEN (src."nm/f" is not null)
                                    THEN src."nm/f"
                                WHEN (mirror."nm/f" is not null)
                                    THEN mirror."nm/f"
                                ELSE null
                         END                                                       AS "nm/f",

                         CASE
                                WHEN (src."nm/i" is not null)
                                    THEN src."nm/i"
                                WHEN (mirror."nm/i" is not null)
                                    THEN mirror."nm/i"
                                ELSE null
                         END                                                        AS "nm/i",

                         CASE
                                WHEN (src."nm/o" is not null)
                                    THEN src."nm/o"
                                WHEN (mirror."nm/o" is not null)
                                    THEN mirror."nm/o"
                                ELSE null
                         END                                                         AS "nm/o",

                         CASE
                                WHEN (src.dc is not null)
                                    THEN src.dc
                                WHEN (mirror.dc is not null)
                                    THEN mirror.dc
                                ELSE null
                         END                                                            AS dc,

                         CASE
                                WHEN (src.ad is not null)
                                    THEN src.ad
                                WHEN (mirror.ad is not null)
                                    THEN mirror.ad
                                ELSE null
                         END                                                            AS ad,

                         CASE
                                WHEN (src.hash is not null)
                                    THEN src.hash
                                WHEN (mirror.hash is not null)
                                    THEN mirror.hash
                                ELSE null
                         END                                                            AS hash,

                         CASE
                                WHEN (src.table_name is not null)
                                    THEN src.table_name
                                WHEN (mirror.table_name is not null)
                                    THEN mirror.table_name
                                ELSE null
                         END                                                            AS table_name,

                         CASE
                                WHEN (src.hash_table is not null)
                                    THEN src.hash_table
                                WHEN (mirror.hash_table is not null)
                                    THEN mirror.hash_table
                                ELSE null
                         END                                                            AS hash_table,

                         CASE
                                WHEN (src.hash_dc is not null)
                                    THEN src.hash_dc
                                WHEN (mirror.hash_dc is not null)
                                    THEN mirror.hash_dc
                                ELSE null
                         END                                                            AS hash_dc

                         FROM ${ods_schema_name}.pensionreceiver_dsrc AS src
                         FULL OUTER JOIN ${ods_schema_name}.pensionreceiver
                         AS mirror ON src.hash_dc = mirror.hash_dc) delta) delta_num) rsl WHERE count_num > 1;
