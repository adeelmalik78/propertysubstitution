CREATE OR REPLACE PACKAGE BODY ${schema.name}.alps_parser_pkg

/*
*****************************************************************************************
03.27.2023 Nguyen Thang Cerebro 62239 sort on column_id in query against
 all_tab_columns
01.13.2023 Nguyen Thang Cerebro 57896, 56389, 57449
10.31.2023 Nguyen Thang Cerebro 64673 Incorrect messages for LPSS Renewal Process
*****************************************************************************************
*/

AS
    PROCEDURE load_sca_to_gt (p_batchid           IN     VARCHAR2,
                              p_parsername        IN     VARCHAR2,
                              p_out_status           OUT VARCHAR2,
                              p_out_description      OUT VARCHAR2)
    IS
        --Dynamic Population of columns from SCA_OUTPUT to GT Table
        CURSOR c_column_map IS
            SELECT *
              FROM alps_parser_column_map
             WHERE     target_table = 'GT_ALPS_PARSER_SCA'
                   AND map_name = 'SCA_GT_MAP';

        vsqlinsertstring   VARCHAR2 (10000);
        vsqlselectstring   VARCHAR2 (10000);

        vsql               VARCHAR2 (4000);
        vfilename          VARCHAR2 (255);
    BEGIN
        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'LOAD_SCA_TO_GT',
            'Begin Loading SCA Output from Parsing into GT Table');

        --Get the Input file name
        BEGIN
            SELECT UPPER (input_filename)
              INTO vfilename
              FROM alps_parser_instance
             WHERE uid_batch = NVL (p_batchid, -99999);
        EXCEPTION
            WHEN OTHERS
            THEN
                raise_application_error (
                    -20001,
                       'Unaable to get the Input file name for the p_BatchId:'
                    || p_batchid);
        END;

        --************************************************************************
        --Truncate the global Temp Table, load data into GT Table from SCA_OUTPUT
        --************************************************************************
        EXECUTE IMMEDIATE 'truncate table gt_alps_parser_sca';

        BEGIN
            --Get the columns for map
            FOR rec IN c_column_map
            LOOP
                vsqlinsertstring :=
                    vsqlinsertstring || rec.target_column || ',' || CHR (10);
                vsqlselectstring :=
                       vsqlselectstring
                    || CASE
                           WHEN rec.source_expression IS NULL
                           THEN
                               rec.source_column
                           ELSE
                               rec.source_expression
                       END
                    || ','
                    || CHR (10);
            END LOOP;

            --Prepare the SQL Statement
            vsqlinsertstring :=
                SUBSTR (vsqlinsertstring, 1, LENGTH (vsqlinsertstring) - 2);
            vsqlselectstring :=
                SUBSTR (vsqlselectstring, 1, LENGTH (vsqlselectstring) - 2);


            --Handle vParser Name
            vsqlselectstring :=
                REPLACE (vsqlselectstring,
                         'vParserName',
                         '''' || p_parsername || '''');

            --Insert dat abased on input filename derived from the p_BatchId
            vsqlinsertstring :=
                   'INSERT INTO ALPS.GT_ALPS_PARSER_SCA (UID_BATCH,UID_RECORD,PARSE_COMMENTS,'
                || vsqlinsertstring
                || ')';

            vsqlselectstring :=
                   'SELECT '
                || p_batchid                                     /*UID_BATCH*/
                || ','
                || 'ROWNUM'                                     /*UID_RECORD*/
                || ','
                || 'NULL,'                                  /*PARSE_COMMENTS*/
                || vsqlselectstring
                || ' FROM ALPS.ALPS_PARSER_SCA_OUTPUT'
                || ' WHERE UPPER(inputfile) = UPPER(:vFileName)';

            vsqlinsertstring := vsqlinsertstring || vsqlselectstring;

            --DBMS_OUTPUT.put_line (vsqlinsertstring);

            --Execute the statement
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'SCA',
                'LOAD_SCA_TO_GT',
                'Before executing SQLString:' || vsqlinsertstring);

            EXECUTE IMMEDIATE vsqlinsertstring
                USING vfilename;


            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'SCA',
                'LOAD_SCA_TO_GT',
                   'After executing SQLString. Rows Effected is '
                || SQL%ROWCOUNT);


            --*******************
            IF vfilename IS NULL
            THEN
                raise_application_error (
                    -20001,
                       'Unable to get Data from alps_parser_instance for the Inputfile:'
                    || vfilename);
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                raise_application_error (
                    -20001,
                       'Unable to get Data from alps_parser_instance for the Inputfile:'
                    || vfilename);
        END;

        --Send Outputs
        DBMS_OUTPUT.put_line ('SUCCESS');
        p_out_status := 'SUCCESS';
        p_out_description := NULL;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'LOAD_SCA_TO_GT',
            'End Loading SCA Output from Parsing into GT Table');
    EXCEPTION
        --Exception Handling goes here
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ());
            p_out_status := 'ERROR';
            p_out_description :=
                SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ();
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'LOAD_SCA_TO_GT',
                   'Error Loading SCA Output from Parsing into GT Table'
                || CHR (13)
                || p_out_description);
    END load_sca_to_gt;

    /*Translates the SCA Data*/
    PROCEDURE scalar_translation (p_batchid           IN     VARCHAR2,
                                  p_parsername        IN     VARCHAR2,
                                  p_out_status           OUT VARCHAR2,
                                  p_out_description      OUT VARCHAR2)
    IS
        --Declare Local Variabled
        vblock             CLOB;

        CURSOR ctranslations (inmarket   VARCHAR2,
                              indisco    VARCHAR2,
                              intype     VARCHAR2)
        IS
              SELECT market_translation,
                     disco_translation,
                     columnvalue,
                     LISTAGG (columnname, ', ')
                         WITHIN GROUP (ORDER BY columnname)    column_list
                FROM alps_parser_sca_translation
                     UNPIVOT INCLUDE NULLS (columnvalue
                                           FOR columnname
                                           IN (inputfile,
                                              market_code,
                                              disco_code,
                                              ldc_account,
                                              address1,
                                              address2,
                                              city,
                                              state_code,
                                              zip,
                                              zipplus4,
                                              county,
                                              country,
                                              rate_class,
                                              rate_subclass,
                                              zone_code,
                                              load_profile,
                                              voltage_class,
                                              meter_cycle,
                                              start_time,
                                              stop_time,
                                              usage_total,
                                              demand_total,
                                              capacity,
                                              transmission,
                                              meter_type,
                                              mhp,
                                              bus,
                                              loss_factor,
                                              strata,
                                              stationid,
                                              attr1,
                                              attr2,
                                              attr3,
                                              attr4,
                                              attr5,
                                              attr6,
                                              attr7,
                                              attr8,
                                              attr9,
                                              attr10,
                                              weather_zone))
               WHERE     columnvalue IS NOT NULL
                     AND market_translation = inmarket
                     AND disco_translation = indisco
            --and TRIM(UPPER(columnvalue)) LIKE inType||'%'
            GROUP BY market_translation, disco_translation, columnvalue
            ORDER BY DECODE (SUBSTR (columnvalue, 1, 4), 'EXPR', 0, 1),
                     --Order By SEQ_NUM in the DYNSQL Table
                     NVL ((SELECT seq_num
                             FROM alps_parser_dynsql
                            WHERE name = columnvalue AND ROWNUM < 2),
                          99);

        vsql               VARCHAR2 (4000);
        vdsqlstatus        VARCHAR2 (100);
        vdsqldescription   VARCHAR2 (2000);
        voutstatus         VARCHAR2 (100);
        voutdescription    VARCHAR2 (2000);
        vfilename          VARCHAR2 (255);
        vinsertstatement   VARCHAR2 (2000);
        vcount             INTEGER;

        --For Date Adjustment
        CURSOR cdates IS
            SELECT uid_record,
                   inputfile,
                   ldc_account,
                   TO_DATE (start_time, 'MM/DD/YYYY HH24:MIS:SS')
                       start_time,
                   TO_DATE (stop_time, 'MM/DD/YYYY HH24:MIS:SS')
                       stop_time,
                   usage_total,
                   demand_total
              FROM gt_alps_parser_sca
             WHERE ROWNUM < 1;

        TYPE t_dates IS TABLE OF cdates%ROWTYPE;

        tbl_dates          t_dates;
        tbl_dates_empty    t_dates;
    BEGIN
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'SCALAR_TRANSLATION',
                                  'Begin Scalar Translation');

        --Get the Input file name
        BEGIN
            SELECT input_filename
              INTO vfilename
              FROM alps_parser_instance
             WHERE uid_batch = NVL (p_batchid, -99999);
        EXCEPTION
            WHEN OTHERS
            THEN
                raise_application_error (
                    -20001,
                       'Unaable to get the Input file name for the p_BatchId:'
                    || p_batchid);
        END;

        /*DELETE FROM alps_parser_acct_note
              WHERE uid_batch = NVL (p_batchid, -99999);
              */

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_LOAD_GT',
               'Before GT Table loaded with Parsed Data for File Name:'
            || vfilename);

        --Call Loading SCA Data into GT for processing
        voutstatus := NULL;
        voutdescription := NULL;
        load_sca_to_gt (p_batchid,
                        p_parsername,
                        voutstatus,
                        voutdescription);

        IF voutstatus <> 'SUCCESS'
        THEN
            raise_application_error (-20001, voutdescription);
        END IF;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_LOAD_GT',
               'After GT Table loaded with Parsed Data for File Name:'
            || vfilename);


        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'PRE_PROCESS_ACTIONS',
                                  'Before PreProcess Actions');

        --*******************************************
        -- BEGIN CALL to PRE_PROCESS_ACTION, Works on GT Tabe
        --*******************************************
        FOR rec
            IN (SELECT DISTINCT pre_process_action
                  FROM alps_parser_templates
                 WHERE     simx_parser_name = p_parsername
                       AND pre_process_action IS NOT NULL)
        LOOP
            --Output Value type, DynSQLName or Expression
            --   DBMS_OUTPUT.
            --   put_line ('rec.PRE_PROCESS_ACTION:' || rec.PRE_PROCESS_ACTION);
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                2,
                'DB',
                NULL,
                'SCA',
                'PRE_PROCESS_ACTIONS',
                'Working on Pre Preocess Action:' || rec.pre_process_action);

            --Execute the SQL Statement
            IF INSTR (TRIM (UPPER (rec.pre_process_action)), 'STATEMENT:') >
               0
            THEN
                BEGIN
                    vsql :=
                        REPLACE (TRIM (rec.pre_process_action),
                                 'STATEMENT:',
                                 '');


                    --   DBMS_OUTPUT.put_line (vsql);
                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        3,
                        'DB',
                        NULL,
                        'SCA',
                        'PRE_PROCESS_ACTIONS',
                        'Execiting SQL Statements:' || vsql);

                    EXECUTE IMMEDIATE vsql;
                -- DBMS_OUTPUT.put_line ('Statement Execution Completed');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        raise_application_error (
                            -20001,
                               'Could not fetch the Dynamic SQL for '
                            || rec.pre_process_action);
                END;
            ELSE
                --If the Pre Process Action does not start with "STATEMENT:" then see if there is any Dynamic SQL Block to execute
                vblock := NULL;

                BEGIN
                    --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                    SELECT source
                      INTO vblock
                      FROM alps_parser_dynsql
                     WHERE     active_flag = 'Y'
                           AND name = rec.pre_process_action;
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        DBMS_OUTPUT.put_line (
                               'Could not fetch the Dynamic SQL for '
                            || rec.pre_process_action);
                --Review VKOLLA, Uncomments the below line?????
                --RAISE_APPLICATION_ERROR(-20001,'Could not fetch the Dynamic SQL for '||rec.PRE_PROCESS_ACTION);
                END;

                --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));

                --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks
                IF LENGTH (vblock) > 0
                THEN
                    vdsqlstatus := NULL;
                    vdsqldescription := NULL;


                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        3,
                        'DB',
                        NULL,
                        'SCA',
                        'PRE_PROCESS_ACTIONS',
                           'Begin Executing Dynamic SQL Block for '
                        || rec.pre_process_action);

                    --dbms_output.put_line(dbms_lob.substr(vBlock,4000,1));
                    EXECUTE IMMEDIATE vblock
                        USING IN OUT vdsqlstatus, IN OUT vdsqldescription;

                    --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                    --DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);

                    IF vdsqlstatus <> 'SUCCESS'
                    THEN
                        raise_application_error (
                            -20001,
                               'Error calling Pre Process Action SQLs'
                            || CHR (13)
                            || vdsqldescription);
                    END IF;

                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        3,
                        'DB',
                        NULL,
                        'SCA',
                        'PRE_PROCESS_ACTIONS',
                           'End Executing Dynamic SQL Block for '
                        || rec.pre_process_action
                        || ';vdsqldescription:'
                        || vdsqldescription);
                END IF;
            END IF;
        END LOOP;

        --****************************
        -- END CALL to PRE_PROCESS_ACTION
        --****************************

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'PRE_PROCESS_ACTIONS',
                                  'After PreProcess Actions');

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'TRSNLATION',
            'Begin Translation of Data using Expressions and Dynamic SQLs');

        --For Each Disco, fetch the Dynamic SQL Blocks/Expressions
        FOR recdisco IN (SELECT DISTINCT market_code, disco_code
                           FROM gt_alps_parser_sca)
        LOOP
            --get each Dynamic SQL Block and Expression, execute the code to populate respective columns.
            FOR rec
                IN ctranslations (recdisco.market_code,
                                  recdisco.disco_code,
                                  NULL)
            LOOP
                --DBMS_OUTPUT.put_line ('rec.columnvalue:' || rec.columnvalue);

                --Update the records in Global Temp table, if it is a simple Expression
                IF INSTR (TRIM (UPPER (rec.columnvalue)), 'EXPR:') > 0
                THEN
                    vsql :=
                           'Update gt_alps_parser_sca Set '
                        || REPLACE (
                               rec.column_list,
                               ', ',
                                  '='
                               || REPLACE (TRIM (rec.columnvalue),
                                           'EXPR:',
                                           '')
                               || ',')
                        || '='
                        || REPLACE (TRIM (rec.columnvalue), 'EXPR:', '')
                        || ' WHERE DISCO_CODE = '''
                        || recdisco.disco_code
                        || '''';

                    --DBMS_OUTPUT.put_line (vsql);

                    EXECUTE IMMEDIATE vsql;

                    DBMS_OUTPUT.put_line ('Update Completed');
                ELSE
                    --If the Translation Mapping does not have "EXPR:" then see if there is any Dynamic SQL Block to execute
                    vblock := NULL;

                    BEGIN
                        --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                        SELECT source
                          INTO vblock
                          FROM alps_parser_dynsql
                         WHERE     active_flag = 'Y'
                               AND category = 'SCA_TRANSLATION'
                               AND name = rec.columnvalue;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            DBMS_OUTPUT.put_line (
                                   'Could not fetch the Dynamic SQL for '
                                || rec.columnvalue);
                    --Review VKOLLA, Uncomments the below line?????
                    --RAISE_APPLICATION_ERROR(-20001,'Could not fetch the Dynamic SQL for '||rec.columnvalue);
                    END;

                    --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));

                    --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks
                    FOR recmeters
                        IN (SELECT DISTINCT ldc_account
                              FROM gt_alps_parser_sca
                             WHERE disco_code = recdisco.disco_code)
                    LOOP
                        IF LENGTH (vblock) > 0
                        THEN
                            vdsqlstatus := NULL;
                            vdsqldescription := NULL;

                            --dbms_output.put_line(dbms_lob.substr(vBlock,4000,1));
                            EXECUTE IMMEDIATE vblock
                                USING recmeters.ldc_account,
                                      rec.column_list,
                                      IN OUT vdsqlstatus,
                                      IN OUT vdsqldescription;

                            alps.alps_parser_pkg.LOG (
                                USER,
                                p_batchid,
                                1,
                                'DB',
                                NULL,
                                'SCA',
                                'TRSNLATION',
                                   'Dynamic SQL:'
                                || rec.columnvalue
                                || ' is completed with Inputs :1>>'
                                || recmeters.ldc_account
                                || ' :2>>'
                                || rec.column_list);
                            alps.alps_parser_pkg.LOG (
                                USER,
                                p_batchid,
                                1,
                                'DB',
                                NULL,
                                'SCA',
                                'TRSNLATION',
                                   'Dynamic SQL:'
                                || rec.columnvalue
                                || ' is completed with Outputs :3>>'
                                || vdsqlstatus
                                || ' :4>>'
                                || vdsqldescription);

                            --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                            --DBMS_OUTPUT.
                            --  put_line ('vDSQLDescription:' || vdsqldescription);

                            IF vdsqlstatus <> 'SUCCESS'
                            THEN
                                raise_application_error (
                                    -20001,
                                       'Error calling Transformations SQLs'
                                    || CHR (13)
                                    || vdsqldescription);
                            END IF;
                        END IF;
                    END LOOP;
                END IF;
            --Output Value type, DynSQLName or Expression
            --DBMS_OUTPUT.put_line ('rec.columnvalue:' || rec.columnvalue);
            END LOOP;
        END LOOP;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'TRSNLATION',
            'End Translation of Data using Expressions and Dynamic SQLs');

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'FE_LIGHTING_ADJUSTMENT',
            'Begin Adding future months and copy existing KWH usage');

        -- Ligthing accounts - FIll up missing months for First Energy accounts
        BEGIN
            alps.fe_lighting_accounts (vfilename,
                                       p_out_status,
                                       p_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                p_out_status := 'ERROR';
                p_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'FE_LIGHTING_ADJUSTMENT',
            'End Adding future months and copy existing KWH usage');

        SELECT COUNT (1) INTO vcount FROM gt_alps_parser_sca;

        DBMS_OUTPUT.put_line (
            'AFTER FE_LIGHTING_ADJUSTMENT - GT count ' || vcount);
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'USAGE_ADJUSTMENT',
                                  'Begin Adjusting Usage when Applicable');

        --****************
        --USAGE ADJUSTMENT
        --****************
        FOR rec IN (SELECT DISTINCT disco_code, ldc_account
                      FROM gt_alps_parser_sca
                     WHERE inputfile = vfilename)
        LOOP
              --Load Meter Specific Data into PL/SQL Table
              SELECT uid_record,
                     inputfile,
                     ldc_account,
                     TO_DATE (start_time, 'MM/DD/YYYY HH24:MI:SS')
                         start_time,
                     TO_DATE (stop_time, 'MM/DD/YYYY HH24:MI:SS')
                         stop_time,
                     NVL (usage_total, 0),
                     NVL (demand_total, 0)
                BULK COLLECT INTO tbl_dates
                FROM gt_alps_parser_sca
               WHERE     ldc_account = rec.ldc_account
                     AND disco_code = rec.disco_code
            ORDER BY ldc_account, start_time, stop_time;

            IF tbl_dates.COUNT > 0
            THEN
                FOR i IN tbl_dates.FIRST .. tbl_dates.LAST
                LOOP
                    IF     tbl_dates (i).start_time IS NULL
                       AND tbl_dates (i).stop_time IS NOT NULL
                    THEN
                        IF i = 1
                        THEN
                            NULL;                                     --Ignore
                        ELSE
                            alps.alps_parser_pkg.LOG (
                                USER,
                                p_batchid,
                                4,
                                'DB',
                                NULL,
                                'SCA',
                                'USAGE_ADJUSTMENT',
                                   'Adjusting Start Date on Usage record for LDC Account:'
                                || tbl_dates (i).ldc_account
                                || ' with Previous Stop Date Time:'
                                || TO_CHAR (tbl_dates (i - 1).stop_time,
                                            'MM/DD/YYYY HH24:MI:SS'));
                            tbl_dates (i).start_time :=
                                tbl_dates (i - 1).stop_time;
                        END IF;
                    ELSIF     tbl_dates (i).start_time IS NOT NULL
                          AND tbl_dates (i).stop_time IS NULL
                    THEN
                        IF i = tbl_dates.COUNT
                        THEN
                            NULL;                         --Ignore Last Record
                        ELSE
                            tbl_dates (i).stop_time :=
                                tbl_dates (i + 1).start_time;

                            alps.alps_parser_pkg.LOG (
                                USER,
                                p_batchid,
                                4,
                                'DB',
                                NULL,
                                'SCA',
                                'USAGE_ADJUSTMENT',
                                   'Adjusting Stop Date on Usage record for LDC Account:'
                                || tbl_dates (i).ldc_account
                                || ' with Next Start Date Time:'
                                || TO_CHAR (tbl_dates (i + 1).start_time,
                                            'MM/DD/YYYY HH24:MI:SS'));
                        END IF;
                    END IF;
                END LOOP;
            END IF;

            --Add Seconds
            FOR formatrec
                IN (SELECT start_date_add_sec, end_date_add_sec
                      FROM v_alps_parser_template
                     WHERE simx_parser_name = p_parsername AND ROWNUM < 2)
            LOOP
                IF tbl_dates.COUNT > 0
                THEN
                    FOR i IN tbl_dates.FIRST .. tbl_dates.LAST
                    LOOP
                        IF tbl_dates (i).start_time IS NOT NULL
                        THEN
                            tbl_dates (i).start_time :=
                                  TRUNC (tbl_dates (i).start_time, 'DD')
                                + (formatrec.start_date_add_sec / 86400);
                            alps.alps_parser_pkg.LOG (
                                USER,
                                p_batchid,
                                5,
                                'DB',
                                NULL,
                                'SCA',
                                'USAGE_ADJUSTMENT',
                                   'Adding Seconds to START_TIME for LDC Account:'
                                || tbl_dates (i).ldc_account
                                || ' - START_DATE_ADD_SEC:'
                                || formatrec.start_date_add_sec);
                        END IF;



                        IF tbl_dates (i).stop_time IS NOT NULL
                        THEN
                            tbl_dates (i).stop_time :=
                                  TRUNC (tbl_dates (i).stop_time, 'DD')
                                + (formatrec.end_date_add_sec / 86400);
                            alps.alps_parser_pkg.LOG (
                                USER,
                                p_batchid,
                                5,
                                'DB',
                                NULL,
                                'SCA',
                                'USAGE_ADJUSTMENT',
                                   'Adding Seconds to STOP_TIME for LDC Account:'
                                || tbl_dates (i).ldc_account
                                || ' - END_DATE_ADD_SEC:'
                                || formatrec.end_date_add_sec);
                        END IF;
                    END LOOP;
                END IF;
            END LOOP;

            --BULK  UPDATE
            FORALL i IN tbl_dates.FIRST .. tbl_dates.LAST
                UPDATE gt_alps_parser_sca
                   SET start_time =
                           TO_CHAR (tbl_dates (i).start_time,
                                    'MM/DD/YYYY HH24:MI:SS'),
                       stop_time =
                           TO_CHAR (tbl_dates (i).stop_time,
                                    'MM/DD/YYYY HH24:MI:SS'),
                    --   attr8 = DECODE (attr8, NULL, 'H', attr8), /*Usage Type*/
                       attr9 = DECODE (attr9, NULL, 'KWH', attr9), /*Usage UOM*/
                       attr10 = DECODE (attr10, NULL, 'KW', attr10) /*Demand UOM*/
                 WHERE uid_record = tbl_dates (i).uid_record;


            --REMOVE Records with No Staop or No Start Times
            DELETE FROM
                gt_alps_parser_sca
                  WHERE     (start_time IS NULL OR stop_time IS NULL)
                        AND disco_code = rec.disco_code
                        AND ldc_account = rec.ldc_account;
        END LOOP;

        SELECT COUNT (1) INTO vcount FROM gt_alps_parser_sca;

        DBMS_OUTPUT.put_line ('AFTER USAGE_ADJUSTMENT - GT count ' || vcount);

        --****************
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'USAGE_ADJUSTMENT',
                                  'End Adjusting Usage Data');



        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'USAGE_ADJUSTMENT',
                                  'Begin pushing Data into SCA_FINAL table');



        --Dump Data into ALPS_PARSER_SCA_FINAL table from GT Table
        FOR rec
            IN (SELECT column_name
                  FROM all_tab_columns
                 WHERE     owner = 'ALPS'
                       AND table_name = 'ALPS_PARSER_SCA_FINAL'
                                              order by column_id)
        LOOP
            vinsertstatement :=
                vinsertstatement || CHR (13) || rec.column_name || ',';
        END LOOP;

        --Remove last character
        vinsertstatement :=
            CASE
                WHEN LENGTH (vinsertstatement) > 0
                THEN
                    SUBSTR (vinsertstatement,
                            1,
                            LENGTH (vinsertstatement) - 1)
                ELSE
                    vinsertstatement
            END;

        --Prepare final Insert Statement
        IF vinsertstatement IS NOT NULL
        THEN
            vinsertstatement :=
                   'INSERT INTO ALPS_PARSER_SCA_FINAL Select '
                || vinsertstatement
                || ' from GT_ALPS_PARSER_SCA WHERE UID_BATCH = NVL(:1,-99999)';



            DBMS_OUTPUT.put_line (
                   'Issue Insert into SCA_FINAL vInsertStatement:'
                || vinsertstatement);

            DELETE FROM alps_parser_sca_final
                  WHERE uid_batch = p_batchid;

            EXECUTE IMMEDIATE vinsertstatement
                USING p_batchid;
        END IF;



        --

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'USAGE_ADJUSTMENT',
                                  'End pushing Data into SCA_FINAL table');

        -- Collect any missing/invalid mappings for Rate/Profile/voltage and Zone
        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'UPSERT_MISSING_ATTR_MAPS',
            'Begin Collecting missing/invalid mappings for Rate/Profile/voltage and Zone');

        -- Ligthing accounts - FIll up missing months for First Energy accounts
        BEGIN
            alps.upsert_missing_attr_maps (p_batchid,
                                           p_out_status,
                                           p_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                p_out_status := 'ERROR';
                p_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'UPSERT_MISSING_ATTR_MAPS',
            'End Collecting missing/invalid mappings for Rate/Profile/voltage and Zone');
        --
        --
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'SYNC_INVALID_ACCOUNTS',
                                  'Begin Sync Invalid AccountS');

        BEGIN
            alps.sync_invalid_accounts (p_batchid,
                                        vfilename,
                                        p_out_status,
                                        p_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                p_out_status := 'ERROR';
                p_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;


        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'SYNC_INVALID_ACCOUNTS',
                                  'End Sync Invalid AccountS');



        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CHECK_FUTURE_TAGS',
                                  'Begin checking for Future Tags');

        DECLARE
            uidbatch         VARCHAR2 (32767);
            outstatus        VARCHAR2 (32767);
            outdescription   VARCHAR2 (32767);
        BEGIN
            uidbatch := p_batchid;
            outstatus := NULL;
            outdescription := NULL;

            FOR rec
                IN (SELECT 1
                      FROM alps_mml_lookup
                     WHERE     lookup_group = 'EW_FLAGS'
                           AND lookup_code = 'TAG_PREPARE_ENABLED'
                           AND lookup_str_value1 = 'Y')
            LOOP
                alps.prepare_tag_request (uidbatch,
                                          outstatus,
                                          outdescription);
            END LOOP;

            p_out_status := outstatus;



            p_out_description := outdescription;
        EXCEPTION
            WHEN OTHERS
            THEN
                p_out_status := 'ERROR';
                p_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;



        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CHECK_FUTURE_TAGS',
               'End Check for Future Tags with p_out_status:'
            || p_out_status
            || ' and with p_out_description:'
            || p_out_description);


        --Send Outputs
        DBMS_OUTPUT.put_line ('SUCCESS');


        p_out_status := 'SUCCESS';


        p_out_description := NULL;



        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'SCALAR_TRANSLATION',
                                  'End Scalar Translation');
        COMMIT;
    EXCEPTION
        --Exception Handling goes here
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ());
            p_out_status := 'ERROR';
            p_out_description :=
                SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ();



            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'SCALAR_TRANSLATION',
                   'Error in Scalar Translation'
                || CHR (13)
                || p_out_description);
    END scalar_translation;

    PROCEDURE idrrect_translation (p_batchid           IN     VARCHAR2,
                                   p_parsername        IN     VARCHAR2,
                                   p_out_status           OUT VARCHAR2,
                                   p_out_description      OUT VARCHAR2)
    IS
        vfilename           VARCHAR2 (255);

        vblock              CLOB;
        vdsqlstatus         VARCHAR2 (1000);
        vdsqldescription    VARCHAR2 (2000);



        v_out_status        VARCHAR2 (100);
        v_out_description   VARCHAR2 (2000);
    BEGIN
        p_out_description := NULL;
        vcomment_description := NULL;
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDRRECT_TRANSLATION',
                                  'Begin IDR Rectangular Process');

        --Get the Input file name
        BEGIN
            SELECT input_filename
              INTO vfilename
              FROM alps_parser_instance
             WHERE uid_batch = NVL (p_batchid, -99999);
        EXCEPTION
            WHEN OTHERS
            THEN
                raise_application_error (
                    -20001,
                       'Unaable to get the Input file name for the p_BatchId:'
                    || p_batchid);
        END;


        --For ROI, its handled with in GET_ROI_DATA
        IF p_parsername != 'ROI Excel'
        THEN
            DELETE FROM alps_parser_acct_note
                  WHERE uid_batch = NVL (p_batchid, -99999);
        END IF;



        --*************
        --Validate IDR RECT
        --*************

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'IDR_RECT_REQUIRED_COLVALIDATION',
            'Before IDR Rect Required Column Validation');

        --Initial Validation
        BEGIN
            initial_validation (p_batchid,
                                p_parsername,
                                v_out_status,
                                v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        --If error, then exit the Coutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;



        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'IDR_RECT_REQUIRED_COLVALIDATION',
            'After IDR Rect Required Column Validation');



        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDR_RECT_VALIDATION',
                                  'Begin IDR Rectangular Validation');

        BEGIN
            --Get the Dynamic SQL Block to execute for Validation
            vblock := NULL;

            BEGIN
                --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                SELECT source
                  INTO vblock
                  FROM alps_parser_dynsql
                 WHERE     active_flag = 'Y'
                       AND category = 'VALIDATION'
                       AND name = 'IDR_RECT_VALIDATION';
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                        'Could not fetch the Dynamic SQL for IDR_RECT_VALIDATION');
            --Review VKOLLA, Uncomments the below line?????
            --RAISE_APPLICATION_ERROR(-20001,'Could not fetch the Dynamic SQL for '||rec.columnvalue);
            END;

            --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));

            --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks



            IF LENGTH (vblock) > 0
            THEN
                vdsqlstatus := NULL;
                vdsqldescription := NULL;

                /*Inputs = FileName; Outputs = STATUS/DESCRIPTION */
                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_RECT',
                    'IDR_RECT_VALIDATION',
                    'Begin calling Dynamic SQL with Inputs :1>>' || vfilename);

                EXECUTE IMMEDIATE vblock
                    USING vfilename,
                          p_batchid,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;



                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_RECT',
                    'IDR_RECT_VALIDATION',
                       'End calling Dynamic SQL with Outputs :2>>'
                    || vdsqlstatus
                    || ' :3>>'
                    || vdsqldescription);

                --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                -- DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);



                IF vdsqlstatus <> 'SUCCESS'
                THEN
                    raise_application_error (
                        -20001,
                           'Error calling IDR Rectagular Validation Dynamic SQL'
                        || CHR (13)
                        || vdsqldescription);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDR_RECT_VALIDATION',
                                  'End IDR Rectangular Validation');

        /*************/
        /* TRANSLATE  */
        /*************/
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDR_RECT_TRANSLATION',
                                  'Begin IDR Rectangular Translation');



        /*Proceed to Translation, by this time GT_ALPS_PARSER_IDR_RECT table is populated and ready for Translation*/
        BEGIN
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'IDR_RECT',
                'IDR_RECT_TRANSLATION',
                'Update each HOUR Value to remove Comma and replace with Blank');


            --Replace commas
            UPDATE gt_alps_parser_idr_rect
               SET hour0 = REPLACE (hour0, ',', ''),
                   hour1 = REPLACE (hour1, ',', ''),
                   hour2 = REPLACE (hour2, ',', ''),
                   hour3 = REPLACE (hour3, ',', ''),
                   hour4 = REPLACE (hour4, ',', ''),
                   hour5 = REPLACE (hour5, ',', ''),
                   hour6 = REPLACE (hour6, ',', ''),
                   hour7 = REPLACE (hour7, ',', ''),
                   hour8 = REPLACE (hour8, ',', ''),
                   hour9 = REPLACE (hour9, ',', ''),
                   hour10 = REPLACE (hour10, ',', ''),
                   hour11 = REPLACE (hour11, ',', ''),
                   hour12 = REPLACE (hour12, ',', ''),
                   hour13 = REPLACE (hour13, ',', ''),
                   hour14 = REPLACE (hour14, ',', ''),
                   hour15 = REPLACE (hour15, ',', ''),
                   hour16 = REPLACE (hour16, ',', ''),
                   hour17 = REPLACE (hour17, ',', ''),
                   hour18 = REPLACE (hour18, ',', ''),
                   hour19 = REPLACE (hour19, ',', ''),
                   hour20 = REPLACE (hour20, ',', ''),
                   hour21 = REPLACE (hour21, ',', ''),
                   hour22 = REPLACE (hour22, ',', ''),
                   hour23 = REPLACE (hour23, ',', '');

            --Single and isolated blank values
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'IDR_RECT',
                'IDR_RECT_TRANSLATION',
                'Update Single Blank values in each Record');

            UPDATE gt_alps_parser_idr_rect
               SET hour0 =
                       CASE
                           WHEN hour0 IS NULL AND hour1 IS NOT NULL
                           THEN
                               hour1
                           ELSE
                               hour0
                       END,
                   hour1 =
                       CASE
                           WHEN     hour1 IS NULL
                                AND hour0 IS NOT NULL
                                AND hour2 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour0) + TO_NUMBER (hour2))
                                   / 2)
                           ELSE
                               hour1
                       END,
                   hour2 =
                       CASE
                           WHEN     hour2 IS NULL
                                AND hour1 IS NOT NULL
                                AND hour3 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour1) + TO_NUMBER (hour3))
                                   / 2)
                           ELSE
                               hour2
                       END,
                   hour3 =
                       CASE
                           WHEN     hour3 IS NULL
                                AND hour2 IS NOT NULL
                                AND hour4 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour2) + TO_NUMBER (hour4))
                                   / 2)
                           ELSE
                               hour3
                       END,
                   hour4 =
                       CASE
                           WHEN     hour4 IS NULL
                                AND hour3 IS NOT NULL
                                AND hour5 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour3) + TO_NUMBER (hour5))
                                   / 2)
                           ELSE
                               hour4
                       END,
                   hour5 =
                       CASE
                           WHEN     hour5 IS NULL
                                AND hour4 IS NOT NULL
                                AND hour6 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour4) + TO_NUMBER (hour6))
                                   / 2)
                           ELSE
                               hour5
                       END,
                   hour6 =
                       CASE
                           WHEN     hour6 IS NULL
                                AND hour5 IS NOT NULL
                                AND hour7 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour5) + TO_NUMBER (hour7))
                                   / 2)
                           ELSE
                               hour6
                       END,
                   hour7 =
                       CASE
                           WHEN     hour7 IS NULL
                                AND hour6 IS NOT NULL
                                AND hour8 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour6) + TO_NUMBER (hour8))
                                   / 2)
                           ELSE
                               hour7
                       END,
                   hour8 =
                       CASE
                           WHEN     hour8 IS NULL
                                AND hour7 IS NOT NULL
                                AND hour9 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour7) + TO_NUMBER (hour9))
                                   / 2)
                           ELSE
                               hour8
                       END,
                   hour9 =
                       CASE
                           WHEN     hour9 IS NULL
                                AND hour8 IS NOT NULL
                                AND hour10 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour8) + TO_NUMBER (hour10))
                                   / 2)
                           ELSE
                               hour9
                       END,
                   hour10 =
                       CASE
                           WHEN     hour10 IS NULL
                                AND hour9 IS NOT NULL
                                AND hour11 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour9) + TO_NUMBER (hour11))
                                   / 2)
                           ELSE
                               hour10
                       END,
                   hour11 =
                       CASE
                           WHEN     hour11 IS NULL
                                AND hour10 IS NOT NULL
                                AND hour12 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour10) + TO_NUMBER (hour12))
                                   / 2)
                           ELSE
                               hour11
                       END,
                   hour12 =
                       CASE
                           WHEN     hour12 IS NULL
                                AND hour11 IS NOT NULL
                                AND hour13 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour10) + TO_NUMBER (hour13))
                                   / 2)
                           ELSE
                               hour12
                       END,
                   hour13 =
                       CASE
                           WHEN     hour13 IS NULL
                                AND hour12 IS NOT NULL
                                AND hour14 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour12) + TO_NUMBER (hour14))
                                   / 2)
                           ELSE
                               hour13
                       END,
                   hour14 =
                       CASE
                           WHEN     hour14 IS NULL
                                AND hour13 IS NOT NULL
                                AND hour15 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour13) + TO_NUMBER (hour15))
                                   / 2)
                           ELSE
                               hour14
                       END,
                   hour15 =
                       CASE
                           WHEN     hour15 IS NULL
                                AND hour14 IS NOT NULL
                                AND hour16 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour14) + TO_NUMBER (hour16))
                                   / 2)
                           ELSE
                               hour15
                       END,
                   hour16 =
                       CASE
                           WHEN     hour16 IS NULL
                                AND hour15 IS NOT NULL
                                AND hour17 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour15) + TO_NUMBER (hour17))
                                   / 2)
                           ELSE
                               hour16
                       END,
                   hour17 =
                       CASE
                           WHEN     hour17 IS NULL
                                AND hour16 IS NOT NULL
                                AND hour18 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour16) + TO_NUMBER (hour18))
                                   / 2)
                           ELSE
                               hour17
                       END,
                   hour18 =
                       CASE
                           WHEN     hour18 IS NULL
                                AND hour17 IS NOT NULL
                                AND hour19 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour17) + TO_NUMBER (hour19))
                                   / 2)
                           ELSE
                               hour18
                       END,
                   hour19 =
                       CASE
                           WHEN     hour19 IS NULL
                                AND hour18 IS NOT NULL
                                AND hour20 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour18) + TO_NUMBER (hour20))
                                   / 2)
                           ELSE
                               hour19
                       END,
                   hour20 =
                       CASE
                           WHEN     hour20 IS NULL
                                AND hour19 IS NOT NULL
                                AND hour21 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour19) + TO_NUMBER (hour21))
                                   / 2)
                           ELSE
                               hour20
                       END,
                   hour21 =
                       CASE
                           WHEN     hour21 IS NULL
                                AND hour20 IS NOT NULL
                                AND hour22 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour20) + TO_NUMBER (hour22))
                                   / 2)
                           ELSE
                               hour21
                       END,
                   hour22 =
                       CASE
                           WHEN     hour22 IS NULL
                                AND hour21 IS NOT NULL
                                AND hour23 IS NOT NULL
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour21) + TO_NUMBER (hour23))
                                   / 2)
                           ELSE
                               hour22
                       END,
                   hour23 =
                       CASE
                           WHEN hour23 IS NULL AND hour22 IS NOT NULL
                           THEN
                               hour22
                           ELSE
                               hour23
                       END;

            --Consecutive Blanks, go 7 days prior/later/average where exists
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'IDR_RECT',
                'IDR_RECT_TRANSLATION',
                'Update Consecutive blanks using 7 Day Logic');

            FOR recblanks
                IN (SELECT a.*, ROWID AS recordid
                      FROM gt_alps_parser_idr_rect a
                     WHERE INSTR (
                                  '~'
                               || hour0
                               || '~'
                               || hour1
                               || '~'
                               || hour2
                               || '~'
                               || hour3
                               || '~'
                               || hour4
                               || '~'
                               || hour5
                               || '~'
                               || hour6
                               || '~'
                               || hour7
                               || '~'
                               || hour8
                               || '~'
                               || hour9
                               || '~'
                               || hour10
                               || '~'
                               || hour11
                               || '~'
                               || hour12
                               || '~'
                               || hour13
                               || '~'
                               || hour14
                               || '~'
                               || hour15
                               || '~'
                               || hour16
                               || '~'
                               || hour17
                               || '~'
                               || hour18
                               || '~'
                               || hour19
                               || '~'
                               || hour20
                               || '~'
                               || hour21
                               || '~'
                               || hour22
                               || '~'
                               || hour23
                               || '~',
                               '~~~') >
                           0)
            LOOP
                FOR recadjusted
                    IN (SELECT a.recordid,
                               alps_parser_pkg.f_get_avg_value (a.hour0,
                                                                b.hour0,
                                                                c.hour0)
                                   hour0,
                               alps_parser_pkg.f_get_avg_value (a.hour1,
                                                                b.hour1,
                                                                c.hour1)
                                   hour1,
                               alps_parser_pkg.f_get_avg_value (a.hour2,
                                                                b.hour2,
                                                                c.hour2)
                                   hour2,
                               alps_parser_pkg.f_get_avg_value (a.hour3,
                                                                b.hour3,
                                                                c.hour3)
                                   hour3,
                               alps_parser_pkg.f_get_avg_value (a.hour4,
                                                                b.hour4,
                                                                c.hour4)
                                   hour4,
                               alps_parser_pkg.f_get_avg_value (a.hour5,
                                                                b.hour5,
                                                                c.hour5)
                                   hour5,
                               alps_parser_pkg.f_get_avg_value (a.hour6,
                                                                b.hour6,
                                                                c.hour6)
                                   hour6,
                               alps_parser_pkg.f_get_avg_value (a.hour7,
                                                                b.hour7,
                                                                c.hour7)
                                   hour7,
                               alps_parser_pkg.f_get_avg_value (a.hour8,
                                                                b.hour8,
                                                                c.hour8)
                                   hour8,
                               alps_parser_pkg.f_get_avg_value (a.hour9,
                                                                b.hour9,
                                                                c.hour9)
                                   hour9,
                               alps_parser_pkg.f_get_avg_value (a.hour10,
                                                                b.hour10,
                                                                c.hour10)
                                   hour10,
                               alps_parser_pkg.f_get_avg_value (a.hour11,
                                                                b.hour11,
                                                                c.hour11)
                                   hour11,
                               alps_parser_pkg.f_get_avg_value (a.hour12,
                                                                b.hour12,
                                                                c.hour12)
                                   hour12,
                               alps_parser_pkg.f_get_avg_value (a.hour13,
                                                                b.hour13,
                                                                c.hour13)
                                   hour13,
                               alps_parser_pkg.f_get_avg_value (a.hour14,
                                                                b.hour14,
                                                                c.hour14)
                                   hour14,
                               alps_parser_pkg.f_get_avg_value (a.hour15,
                                                                b.hour15,
                                                                c.hour15)
                                   hour15,
                               alps_parser_pkg.f_get_avg_value (a.hour16,
                                                                b.hour16,
                                                                c.hour16)
                                   hour16,
                               alps_parser_pkg.f_get_avg_value (a.hour17,
                                                                b.hour17,
                                                                c.hour17)
                                   hour17,
                               alps_parser_pkg.f_get_avg_value (a.hour18,
                                                                b.hour18,
                                                                c.hour18)
                                   hour18,
                               alps_parser_pkg.f_get_avg_value (a.hour19,
                                                                b.hour19,
                                                                c.hour19)
                                   hour19,
                               alps_parser_pkg.f_get_avg_value (a.hour20,
                                                                b.hour20,
                                                                c.hour20)
                                   hour20,
                               alps_parser_pkg.f_get_avg_value (a.hour21,
                                                                b.hour21,
                                                                c.hour21)
                                   hour21,
                               alps_parser_pkg.f_get_avg_value (a.hour22,
                                                                b.hour22,
                                                                c.hour22)
                                   hour22,
                               alps_parser_pkg.f_get_avg_value (a.hour23,
                                                                b.hour23,
                                                                c.hour23)
                                   hour23
                          FROM                              /*Current Record*/
                               (SELECT t1.*, t1.ROWID recordid
                                  FROM gt_alps_parser_idr_rect t1
                                 WHERE     ldc_account =
                                           recblanks.ldc_account
                                       AND ROWID = recblanks.recordid
                                       AND intervaldate =
                                           recblanks.intervaldate) a,
                               /*Last Week Record*/
                                (SELECT *
                                   FROM gt_alps_parser_idr_rect
                                  WHERE     ldc_account =
                                            recblanks.ldc_account
                                        AND intervaldate =
                                            recblanks.intervaldate - 7) b,
                               /*Next Week Record*/
                                (SELECT *
                                   FROM gt_alps_parser_idr_rect
                                  WHERE     ldc_account =
                                            recblanks.ldc_account
                                        AND intervaldate =
                                            recblanks.intervaldate + 7) c
                         WHERE     a.ldc_account = b.ldc_account(+)
                               AND a.ldc_account = c.ldc_account(+))
                LOOP
                    /*Update GT Table with available averages from Previous 7 th day and next 7th day*/
                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        4,
                        'DB',
                        NULL,
                        'IDR_RECT',
                        'IDR_RECT_TRANSLATION',
                           'Update Consecutive blanks using 7 Day Logic for LDC Account:'
                        || recblanks.ldc_account);

                    UPDATE gt_alps_parser_idr_rect
                       SET hour0 = recadjusted.hour0,
                           hour1 = recadjusted.hour1,
                           hour2 = recadjusted.hour2,
                           hour3 = recadjusted.hour3,
                           hour4 = recadjusted.hour4,
                           hour5 = recadjusted.hour5,
                           hour6 = recadjusted.hour6,
                           hour7 = recadjusted.hour7,
                           hour8 = recadjusted.hour8,
                           hour9 = recadjusted.hour9,
                           hour10 = recadjusted.hour10,
                           hour11 = recadjusted.hour11,
                           hour12 = recadjusted.hour12,
                           hour13 = recadjusted.hour13,
                           hour14 = recadjusted.hour14,
                           hour15 = recadjusted.hour15,
                           hour16 = recadjusted.hour16,
                           hour17 = recadjusted.hour17,
                           hour18 = recadjusted.hour18,
                           hour19 = recadjusted.hour19,
                           hour20 = recadjusted.hour20,
                           hour21 = recadjusted.hour21,
                           hour22 = recadjusted.hour22,
                           hour23 = recadjusted.hour23
                     WHERE ROWID = recadjusted.recordid;
                END LOOP;                                      /*RecAdjusted*/
            END LOOP;                                   /*recMultipleBlanks */


            --Single and isolated blank values
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'IDR_RECT',
                'IDR_RECT_TRANSLATION',
                'Update Single 0 values in each Record');

            UPDATE gt_alps_parser_idr_rect
               SET hour0 =
                       CASE
                           WHEN     TO_NUMBER (hour0) = 0
                                AND TO_NUMBER (hour1) > 0
                           THEN
                               hour1
                           ELSE
                               hour0
                       END,
                   hour1 =
                       CASE
                           WHEN     TO_NUMBER (hour1) = 0
                                AND TO_NUMBER (hour0) > 0
                                AND TO_NUMBER (hour2) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour0) + TO_NUMBER (hour2))
                                   / 2)
                           ELSE
                               hour1
                       END,
                   hour2 =
                       CASE
                           WHEN     TO_NUMBER (hour2) = 0
                                AND TO_NUMBER (hour1) > 0
                                AND TO_NUMBER (hour3) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour1) + TO_NUMBER (hour3))
                                   / 2)
                           ELSE
                               hour2
                       END,
                   hour3 =
                       CASE
                           WHEN     TO_NUMBER (hour3) = 0
                                AND TO_NUMBER (hour2) > 0
                                AND TO_NUMBER (hour4) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour2) + TO_NUMBER (hour4))
                                   / 2)
                           ELSE
                               hour3
                       END,
                   hour4 =
                       CASE
                           WHEN     TO_NUMBER (hour4) = 0
                                AND TO_NUMBER (hour3) > 0
                                AND TO_NUMBER (hour5) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour3) + TO_NUMBER (hour5))
                                   / 2)
                           ELSE
                               hour4
                       END,
                   hour5 =
                       CASE
                           WHEN     TO_NUMBER (hour5) = 0
                                AND TO_NUMBER (hour4) > 0
                                AND TO_NUMBER (hour6) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour4) + TO_NUMBER (hour6))
                                   / 2)
                           ELSE
                               hour5
                       END,
                   hour6 =
                       CASE
                           WHEN     TO_NUMBER (hour6) = 0
                                AND TO_NUMBER (hour5) > 0
                                AND TO_NUMBER (hour7) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour5) + TO_NUMBER (hour7))
                                   / 2)
                           ELSE
                               hour6
                       END,
                   hour7 =
                       CASE
                           WHEN     TO_NUMBER (hour7) = 0
                                AND TO_NUMBER (hour6) > 0
                                AND TO_NUMBER (hour8) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour6) + TO_NUMBER (hour8))
                                   / 2)
                           ELSE
                               hour7
                       END,
                   hour8 =
                       CASE
                           WHEN     TO_NUMBER (hour8) = 0
                                AND TO_NUMBER (hour7) > 0
                                AND TO_NUMBER (hour9) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour7) + TO_NUMBER (hour9))
                                   / 2)
                           ELSE
                               hour8
                       END,
                   hour9 =
                       CASE
                           WHEN     TO_NUMBER (hour9) = 0
                                AND TO_NUMBER (hour8) > 0
                                AND TO_NUMBER (hour10) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour8) + TO_NUMBER (hour10))
                                   / 2)
                           ELSE
                               hour9
                       END,
                   hour10 =
                       CASE
                           WHEN     TO_NUMBER (hour10) = 0
                                AND TO_NUMBER (hour9) > 0
                                AND TO_NUMBER (hour11) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour9) + TO_NUMBER (hour11))
                                   / 2)
                           ELSE
                               hour10
                       END,
                   hour11 =
                       CASE
                           WHEN     TO_NUMBER (hour11) = 0
                                AND TO_NUMBER (hour10) > 0
                                AND TO_NUMBER (hour12) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour10) + TO_NUMBER (hour12))
                                   / 2)
                           ELSE
                               hour11
                       END,
                   hour12 =
                       CASE
                           WHEN     TO_NUMBER (hour12) = 0
                                AND TO_NUMBER (hour11) > 0
                                AND TO_NUMBER (hour13) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour10) + TO_NUMBER (hour13))
                                   / 2)
                           ELSE
                               hour12
                       END,
                   hour13 =
                       CASE
                           WHEN     TO_NUMBER (hour13) = 0
                                AND TO_NUMBER (hour12) > 0
                                AND TO_NUMBER (hour14) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour12) + TO_NUMBER (hour14))
                                   / 2)
                           ELSE
                               hour13
                       END,
                   hour14 =
                       CASE
                           WHEN     TO_NUMBER (hour14) = 0
                                AND TO_NUMBER (hour13) > 0
                                AND TO_NUMBER (hour15) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour13) + TO_NUMBER (hour15))
                                   / 2)
                           ELSE
                               hour14
                       END,
                   hour15 =
                       CASE
                           WHEN     TO_NUMBER (hour15) = 0
                                AND TO_NUMBER (hour14) > 0
                                AND TO_NUMBER (hour16) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour14) + TO_NUMBER (hour16))
                                   / 2)
                           ELSE
                               hour15
                       END,
                   hour16 =
                       CASE
                           WHEN     TO_NUMBER (hour16) = 0
                                AND TO_NUMBER (hour15) > 0
                                AND TO_NUMBER (hour17) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour15) + TO_NUMBER (hour17))
                                   / 2)
                           ELSE
                               hour16
                       END,
                   hour17 =
                       CASE
                           WHEN     TO_NUMBER (hour17) = 0
                                AND TO_NUMBER (hour16) > 0
                                AND TO_NUMBER (hour18) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour16) + TO_NUMBER (hour18))
                                   / 2)
                           ELSE
                               hour17
                       END,
                   hour18 =
                       CASE
                           WHEN     TO_NUMBER (hour18) = 0
                                AND TO_NUMBER (hour17) > 0
                                AND TO_NUMBER (hour19) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour17) + TO_NUMBER (hour19))
                                   / 2)
                           ELSE
                               hour18
                       END,
                   hour19 =
                       CASE
                           WHEN     TO_NUMBER (hour19) = 0
                                AND TO_NUMBER (hour18) > 0
                                AND TO_NUMBER (hour20) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour18) + TO_NUMBER (hour20))
                                   / 2)
                           ELSE
                               hour19
                       END,
                   hour20 =
                       CASE
                           WHEN     TO_NUMBER (hour20) = 0
                                AND TO_NUMBER (hour19) > 0
                                AND TO_NUMBER (hour21) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour19) + TO_NUMBER (hour21))
                                   / 2)
                           ELSE
                               hour20
                       END,
                   hour21 =
                       CASE
                           WHEN     TO_NUMBER (hour21) = 0
                                AND TO_NUMBER (hour20) > 0
                                AND TO_NUMBER (hour22) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour20) + TO_NUMBER (hour22))
                                   / 2)
                           ELSE
                               hour21
                       END,
                   hour22 =
                       CASE
                           WHEN     TO_NUMBER (hour22) = 0
                                AND TO_NUMBER (hour21) > 0
                                AND TO_NUMBER (hour23) > 0
                           THEN
                               TO_CHAR (
                                     (TO_NUMBER (hour21) + TO_NUMBER (hour23))
                                   / 2)
                           ELSE
                               hour22
                       END,
                   hour23 =
                       CASE
                           WHEN     TO_NUMBER (hour23) = 0
                                AND TO_NUMBER (hour22) > 0
                           THEN
                               hour22
                           ELSE
                               hour23
                       END;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDR_RECT_TRANSLATION',
                                  'End of IDR Rectangular Translation');

        --Remove Data from ALPS_PARSER_IDR_RECT_OUTPUT and push the data from GT Table
        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'IDR_RECT',
            'IDR_RECT_TRANSLATION',
               'Delete IDR Data from alps_parser_idr_rect_output for inputfile:'
            || vfilename);

        DELETE FROM alps_parser_idr_rect_output
              WHERE UPPER (inputfile) = UPPER (vfilename);


        INSERT INTO alps_parser_idr_rect_output
            SELECT * FROM gt_alps_parser_idr_rect;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'IDR_RECT',
            'IDR_RECT_TRANSLATION',
               'Insert IDR Data from GT_ALPS_PARSER_IDR_RECT Table into ALPS_PARSER_IDR_RECT_OUTPUT for inputfile:'
            || vfilename
            || ' No of Rows Effected:'
            || SQL%ROWCOUNT);



        /****************/
        /*Tag Estimation*/
        /****************/



        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDR_BASED_TAG_ESTIMATION',
                                  'End IDR BASED TAG_ESTIMATION');

        BEGIN
            --Get the Dynamic SQL Block to execute for Validation
            vblock := NULL;



            BEGIN
                --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                SELECT source
                  INTO vblock
                  FROM alps_parser_dynsql
                 WHERE     active_flag = 'Y'
                       AND category = 'RECT_TAG_ESTIMATION'
                       AND name = 'IDR_RECT_TAG_ESTIMATION';
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                        'Could not fetch the Dynamic SQL for IDR_RECT_TAG_ESTIMATION');
            END;



            --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));



            --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks



            IF LENGTH (vblock) > 0
            THEN
                vdsqlstatus := NULL;
                vdsqldescription := NULL;

                /*Inputs = FileName; Outputs = STATUS/DESCRIPTION */
                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_RECT',
                    'IDR_BASED_TAG_ESTIMATION',
                    'Begin calling Dynamic SQL with Inputs :1>>' || vfilename);

                EXECUTE IMMEDIATE vblock
                    USING vfilename,
                          p_batchid,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;



                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_RECT',
                    'IDR_BASED_TAG_ESTIMATION',
                       'End calling Dynamic SQL with Outputs :2>>'
                    || vdsqlstatus
                    || ' :3>>'
                    || vdsqldescription);

                --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                -- DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);


                IF vdsqlstatus <> 'SUCCESS'
                THEN
                    /*raise_application_error (
                       -20001,
                          'Error calling IDR Tag Estimation Dynamic SQL'
                       || CHR (13)
                       || vdsqldescription);*/
                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'IDR_RECT',
                        'IDR_BASED_TAG_ESTIMATION',
                           'Error calling IDR Tag Estimation Dynamic SQL'
                        || CHR (13)
                        || vdsqldescription);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDR_BASED_TAG_ESTIMATION',
                                  'End IDR BASED TAG_ESTIMATION');



        --Outputs
        p_out_status := 'SUCCESS';
        p_out_description := NULL;

        --Get the distinct Warnings from the Note table for each meter and send as part of p_Out_Description
        FOR rec
            IN (SELECT DISTINCT comments
                  FROM alps_parser_acct_note
                 WHERE     note_type = 'WARNING'
                       AND uid_batch = NVL (p_batchid, -99999)
                       AND comments IS NOT NULL)
        LOOP
            vcomment_description :=
                p_out_description || rec.comments || CHR (13);
        --p_out_description := p_out_description || rec.comments || CHR (13);
        END LOOP;

        SELECT SUBSTR (vcomment_description, 1, 2000)
          INTO p_out_description
          FROM DUAL;


        --
        COMMIT;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_RECT',
                                  'IDRRECT_TRANSLATION',
                                  'End IDR Rectangular Process');
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;

            --Outputs
            p_out_status := 'ERROR';



            --Get the distinct Errors from the Note table for each meter and send as part of p_Out_Description
            FOR rec
                IN (SELECT DISTINCT comments
                      FROM alps_parser_acct_note
                     WHERE     note_type = 'ERROR'
                           AND uid_batch = NVL (p_batchid, -99999)
                           AND comments IS NOT NULL)
            LOOP
                vcomment_description :=
                    p_out_description || rec.comments || CHR (13);
            --p_out_description := p_out_description || rec.comments || CHR (13);
            END LOOP;

            SELECT SUBSTR (vcomment_description, 1, 2000)
              INTO p_out_description
              FROM DUAL;

            p_out_description :=
                   'Error encountered:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace
                || CHR (13)
                || p_out_description;



            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'IDR_RECT',
                'IDRRECT_TRANSLATION',
                   'Error in IDR Rectangular Process:'
                || CHR (13)
                || p_out_description);
    END;

PROCEDURE idrstrip_translation (p_batchid           IN     VARCHAR2,
                                    p_parsername        IN     VARCHAR2,
                                    p_out_status           OUT VARCHAR2,
                                    p_out_description      OUT VARCHAR2)
    IS
        vfilename           VARCHAR2 (255);
        vparsername         VARCHAR2 (255);

        vblock              CLOB;
        vdsqlstatus         VARCHAR2 (1000);
        vdsqldescription    VARCHAR2 (2000);

        vrowseffected       NUMBER;



        v_out_status        VARCHAR2 (100);
        v_out_description   VARCHAR2 (2000);
        v_cnt               INTEGER := 0;

    BEGIN
        vcomment_description := NULL;
        p_out_description := NULL;
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDRSTRIP_TRANSLATION',
                                  'Begin IDR STRIP Process');

        --Get the Input file name
        BEGIN
            SELECT input_filename, simx_parser_name
              INTO vfilename, vparsername
              FROM alps_parser_instance
             WHERE uid_batch = NVL (p_batchid, -99999);
        EXCEPTION
            WHEN OTHERS
            THEN
                raise_application_error (
                    -20001,
                       'Unaable to get the Input file name for the p_BatchId:'
                    || p_batchid);
        END;

        DELETE FROM alps_parser_acct_note
              WHERE uid_batch = NVL (p_batchid, -99999);

        --
        IF vparsername <> 'Renewal Excel' AND
           INSTR(vfilename,'INTERNAL_REQUEST_RENEWAL') = 0
        THEN
        --*************
        --Validate IDR RECT
        --*************


        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'IDR_STRIP_REQUIRED_COLVALIDATION',
            'Before IDR Strip Required Column Validation');

        --Initial Validation
        BEGIN
            initial_validation (p_batchid,
                                p_parsername,
                                v_out_status,
                                v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        --If error, then exit the Coutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'IDR_STRIP_REQUIRED_COLVALIDATION',
            'After IDR Strip Required Column Validation');


        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_STRIP_VALIDATION',
                                  'Begin IDR STRIP Validation');

        BEGIN
            --Get the Dynamic SQL Block to execute for Validation
            vblock := NULL;

            BEGIN
                --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                SELECT source
                  INTO vblock
                  FROM alps_parser_dynsql
                 WHERE     active_flag = 'Y'
                       AND category = 'VALIDATION'
                       AND name = 'IDR_STRIP_VALIDATION_NEW';
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                        'Could not fetch the Dynamic SQL for IDR_STRIP_VALIDATION');
            --Review VKOLLA, Uncomments the below line?????
            --RAISE_APPLICATION_ERROR(-20001,'Could not fetch the Dynamic SQL for '||rec.columnvalue);
            END;

            --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));

            --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks

            IF LENGTH (vblock) > 0
            THEN
                vdsqlstatus := NULL;
                vdsqldescription := NULL;

                /*Inputs = FileName; Outputs = STATUS/DESCRIPTION */
                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_STRIP_VALIDATION',
                    'Begin calling Dynamic SQL with Inputs :1>>' || vfilename);

                EXECUTE IMMEDIATE vblock
                    USING vfilename,
                          p_batchid,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_STRIP_VALIDATION',
                       'End calling Dynamic SQL with Outputs :2>>'
                    || vdsqlstatus
                    || ' :3>>'
                    || vdsqldescription);

                --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                --DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);

                IF vdsqlstatus <> 'SUCCESS'
                THEN
                    raise_application_error (
                        -20001,
                           'Error calling IDR Strip Validation Dynamic SQL'
                        || CHR (13)
                        || vdsqldescription);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        --
        BEGIN
            --Get the Dynamic SQL Block to execute for channel 87 Validation
            vblock := NULL;

            BEGIN
                --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                SELECT source
                  INTO vblock
                  FROM alps_parser_dynsql
                 WHERE     active_flag = 'Y'
                       AND category = 'VALIDATION'
                       AND name = 'IDR_STRIP87_VALIDATION';
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                        'Could not fetch the Dynamic SQL for IDR_STRIP87_VALIDATION');
            --Review VKOLLA, Uncomments the below line?????
            --RAISE_APPLICATION_ERROR(-20001,'Could not fetch the Dynamic SQL for '||rec.columnvalue);
            END;

            --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));

            --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks

            IF LENGTH (vblock) > 0
            THEN
                vdsqlstatus := NULL;
                vdsqldescription := NULL;

                /*Inputs = FileName; Outputs = STATUS/DESCRIPTION */
                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_STRIP87_VALIDATION',
                    'Begin calling Dynamic SQL with Inputs :1>>' || vfilename);

                EXECUTE IMMEDIATE vblock
                    USING vfilename,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;



                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_STRIP87_VALIDATION',
                       'End calling Dynamic SQL with Outputs :2>>'
                    || vdsqlstatus
                    || ' :3>>'
                    || vdsqldescription);

                --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                --DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);

                IF vdsqlstatus <> 'SUCCESS'
                THEN
                    raise_application_error (
                        -20001,
                           'Error calling IDR Strip87 Validation Dynamic SQL'
                        || CHR (13)
                        || vdsqldescription);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_STRIP_VALIDATION',
                                  'End IDR STRIP Validation');

        /*************/
        /* TRANSLATE  */
        /*************/
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_STRIP_TRANSLATION',
                                  'Begin IDR STRIP Translation');

        /*Proceed to Translation, by this time GT_ALPS_PARSER_IDR_RECT table is populated and ready for Translation*/
        BEGIN
            --Replace commas
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                4,
                'DB',
                NULL,
                'IDR_STRIP',
                'IDR_STRIP_TRANSLATION',
                'Update TOTAL_USAGE on each record by replacing Comma with a Blank');

            UPDATE gt_alps_parser_idr_strip
               SET usage_total = REPLACE (usage_total, ',', '');

            --Single and isolated blank values
            BEGIN
                FOR recldc IN (SELECT DISTINCT ldc_account
                                 FROM gt_alps_parser_idr_strip)
                LOOP
                    FOR rec
                        IN (SELECT *
                              FROM (SELECT market_code,
                                           disco_code,
                                           ldc_account,
                                           time_stamp,
                                           usage_total,
                                           --Previous Value
                                           CASE
                                               WHEN (   time_stamp =
                                                        prev_time_stamp
                                                     OR prev_time_stamp <
                                                        TRUNC (time_stamp))
                                               THEN
                                                   NULL
                                               ELSE
                                                   prev_usage_total
                                           END         prev_usage_total,
                                           prev_time_stamp,
                                           --NextValue
                                           CASE
                                               WHEN (   time_stamp =
                                                        next_time_stamp
                                                     OR TRUNC (
                                                            next_time_stamp) >
                                                        TRUNC (time_stamp))
                                               THEN
                                                   NULL
                                               ELSE
                                                   next_usage_total
                                           END         next_usage_total,
                                           next_time_stamp,
                                           alps_parser_pkg.f_get_avg_value (
                                               --CUrrent Value
                                               usage_total,
                                               --Previous Value
                                               CASE
                                                   WHEN (   time_stamp =
                                                            prev_time_stamp
                                                         OR prev_time_stamp <
                                                            TRUNC (
                                                                time_stamp))
                                                   THEN
                                                       NULL
                                                   ELSE
                                                       prev_usage_total
                                               END,
                                               --NextValue
                                               CASE
                                                   WHEN (   time_stamp =
                                                            next_time_stamp
                                                         OR TRUNC (
                                                                next_time_stamp) >
                                                            TRUNC (
                                                                time_stamp))
                                                   THEN
                                                       NULL
                                                   ELSE
                                                       next_usage_total
                                               END)    new_avg_usage_total
                                      FROM (SELECT market_code,
                                                   disco_code,
                                                   ldc_account,
                                                   time_stamp,
                                                   usage_total,
                                                   LAG (usage_total,
                                                        1,
                                                        'DUMMY')
                                                       OVER (
                                                           ORDER BY
                                                               ldc_account,
                                                               time_stamp)
                                                       AS prev_usage_total,
                                                   LAG (time_stamp,
                                                        1,
                                                        time_stamp)
                                                       OVER (
                                                           ORDER BY
                                                               ldc_account,
                                                               time_stamp)
                                                       AS prev_time_stamp,
                                                   LEAD (usage_total,
                                                         1,
                                                         'DUMMY')
                                                       OVER (
                                                           ORDER BY
                                                               ldc_account,
                                                               time_stamp)
                                                       AS next_usage_total,
                                                   LEAD (time_stamp,
                                                         1,
                                                         time_stamp)
                                                       OVER (
                                                           ORDER BY
                                                               ldc_account,
                                                               time_stamp)
                                                       AS next_time_stamp
                                              FROM gt_alps_parser_idr_strip
                                             WHERE ldc_account =
                                                   recldc.ldc_account)
                                     WHERE     usage_total IS NULL
                                           AND (CASE
                                                    WHEN (  TRUNC (
                                                                time_stamp)
                                                          - TRUNC (
                                                                prev_time_stamp)) >
                                                         0
                                                    THEN
                                                        'DUMMY'
                                                    ELSE
                                                        prev_usage_total
                                                END)
                                                   IS NOT NULL
                                           AND (CASE
                                                    WHEN   TRUNC (
                                                               next_time_stamp)
                                                         - TRUNC (time_stamp) >
                                                         0
                                                    THEN
                                                        'DUMMY'
                                                    ELSE
                                                        next_usage_total
                                                END)
                                                   IS NOT NULL))
                    LOOP
                        UPDATE gt_alps_parser_idr_strip
                           SET usage_total = rec.new_avg_usage_total
                         WHERE     market_code = rec.market_code
                               AND disco_code = rec.disco_code
                               AND ldc_account = rec.ldc_account
                               AND time_stamp = rec.time_stamp;

                        alps.alps_parser_pkg.LOG (
                            USER,
                            p_batchid,
                            4,
                            'DB',
                            NULL,
                            'IDR_STRIP',
                            'IDR_STRIP_TRANSLATION',
                               'Update Single Blank values for LDC Account:'
                            || rec.ldc_account
                            || ' TimeStamp:'
                            || TO_CHAR (rec.time_stamp,
                                        'MM/DD/YYYY HH24:MI:SS'));
                    END LOOP;
                END LOOP;
            END;

            --Consecutive Blanks, go 7 days prior/later/average where exists
            FOR rec IN (SELECT DISTINCT market_code,
                                        disco_code,
                                        ldc_account,
                                        time_stamp,
                                        usage_total,
                                        prev_week_value,
                                        next_week_value
                          FROM v_alps_parser_strip_val)
            LOOP
                UPDATE gt_alps_parser_idr_strip
                   SET usage_total =
                           alps_parser_pkg.f_get_avg_value (
                               rec.usage_total,
                               rec.prev_week_value,
                               rec.next_week_value)
                 WHERE     market_code = rec.market_code
                       AND disco_code = rec.disco_code
                       AND ldc_account = rec.ldc_account
                       AND time_stamp = rec.time_stamp;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_STRIP_TRANSLATION',
                       'Update Consecutive Blank values for LDC Account:'
                    || rec.ldc_account
                    || ' TimeStamp:'
                    || TO_CHAR (rec.time_stamp, 'MM/DD/YYYY HH24:MI:SS'));
            END LOOP;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        BEGIN
            FOR recldc IN (SELECT DISTINCT ldc_account
                             FROM gt_alps_parser_idr_strip)
            LOOP
                FOR rec
                    IN (  SELECT DISTINCT
                                 market_code,
                                 disco_code,
                                 ldc_account,
                                 time_stamp,
                                 MAX (prev_usage_total)     prev_usage_total,
                                 MAX (next_usage_total)     next_usage_total
                            FROM (  SELECT DISTINCT
                                           market_code,
                                           disco_code,
                                           ldc_account,
                                           TRUNC (time_stamp, 'HH')
                                               time_stamp,
                                           usage_total,
                                           TO_NUMBER (
                                               LAG (usage_total, 1, NULL)
                                                   OVER (ORDER BY time_stamp))
                                               AS prev_usage_total,
                                           LAG (time_stamp, 1, time_stamp)
                                               OVER (ORDER BY time_stamp)
                                               AS prev_time_stamp,
                                           TO_NUMBER (
                                               LEAD (usage_total, 1, NULL)
                                                   OVER (ORDER BY time_stamp))
                                               AS next_usage_total,
                                           LEAD (time_stamp, 1, time_stamp)
                                               OVER (ORDER BY time_stamp)
                                               AS next_time_stamp
                                      FROM gt_alps_parser_idr_strip
                                     WHERE ldc_account = recldc.ldc_account
                                  ORDER BY time_stamp)
                           WHERE NVL (usage_total, 0) = 0
                        GROUP BY market_code,
                                 disco_code,
                                 ldc_account,
                                 time_stamp
                          HAVING (    MAX (prev_usage_total) > 0
                                  AND MAX (next_usage_total) > 0))
                LOOP
                    UPDATE gt_alps_parser_idr_strip
                       SET usage_total =
                                 (rec.prev_usage_total + rec.next_usage_total)
                               / 2
                     WHERE     market_code = rec.market_code
                           AND disco_code = rec.disco_code
                           AND ldc_account = rec.ldc_account
                           AND NVL (usage_total, 0) = 0
                           AND TRUNC (time_stamp, 'HH') =
                               TRUNC (rec.time_stamp, 'HH');


                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        4,
                        'DB',
                        NULL,
                        'IDR_STRIP',
                        'IDR_STRIP_TRANSLATION',
                           'Update Single 0 values for LDC Account:'
                        || rec.ldc_account
                        || ' TimeStamp:'
                        || TO_CHAR (rec.time_stamp, 'MM/DD/YYYY HH24:MI:SS'));
                END LOOP;
            END LOOP;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_STRIP_TRANSLATION',
                                  'End IDR STRIP Translation');

        --Remove Data from ALPS_PARSER_IDR_RECT_OUTPUT and push the data from GT Table
        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'IDR_STRIP',
            'IDR_STRIP_TRANSLATION',
               'Delete IDR Data from alps_parser_idr_strip_output for inputfile:'
            || vfilename);

        DELETE FROM alps_parser_idr_strip_output
              WHERE UPPER (inputfile) = UPPER (vfilename);

        INSERT INTO alps_parser_idr_strip_output
            SELECT * FROM gt_alps_parser_idr_strip;

        vrowseffected := SQL%ROWCOUNT;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'IDR_STRIP',
            'IDR_STRIP_TRANSLATION',
               'Insert IDR Data from GT_ALPS_PARSER_IDR_STRIP Table into ALPS_PARSER_IDR_STRIP_OUTPUT for inputfile:'
            || vfilename
            || ' No of Rows Effected:'
            || vrowseffected);

        INSERT INTO alps_parser_idr_strip_output
            SELECT *
              FROM gt_alps_parser_idr_strip87
             WHERE UPPER (inputfile) = UPPER (vfilename);

        vrowseffected := SQL%ROWCOUNT;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'IDR_STRIP',
            'IDR_STRIP_TRANSLATION',
               'Insert IDR Data from GT_ALPS_PARSER_IDR_STRIP87 Table into ALPS_PARSER_IDR_STRIP_OUTPUT for inputfile:'
            || vfilename
            || ' No of Rows Effected:'
            || vrowseffected);

        /****************/
        /*Tag Estimation for NON-RENEWALS*/
        /****************/
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_BASED_TAG_ESTIMATION',
                                  'End IDR BASED TAG_ESTIMATION');

        BEGIN
            --Get the Dynamic SQL Block to execute for Validation
            vblock := NULL;



            BEGIN
                --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                SELECT source
                  INTO vblock
                  FROM alps_parser_dynsql
                 WHERE     active_flag = 'Y'
                       AND category = 'STRIP_TAG_ESTIMATION'
                       AND name = 'IDR_STRIP_TAG_ESTIMATION';
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                        'Could not fetch the Dynamic SQL for IDR_RECT_TAG_ESTIMATION');
            END;

            --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));



            --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks

            IF LENGTH (vblock) > 0
            THEN
                vdsqlstatus := NULL;
                vdsqldescription := NULL;

                /*Inputs = FileName; Outputs = STATUS/DESCRIPTION */
                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_BASED_TAG_ESTIMATION',
                    'Begin calling Dynamic SQL with Inputs :1>>' || vfilename);

                EXECUTE IMMEDIATE vblock
                    USING vfilename,
                          p_batchid,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_BASED_TAG_ESTIMATION',
                       'End calling Dynamic SQL with Outputs :2>>'
                    || vdsqlstatus
                    || ' :3>>'
                    || vdsqldescription);

                --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                -- DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);

                IF vdsqlstatus <> 'SUCCESS'
                THEN
                    /*raise_application_error (
                       -20001,
                          'Error calling IDR Tag Estimation Dynamic SQL'
                       || CHR (13)
                       || vdsqldescription);*/
                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'IDR_STRIP',
                        'IDR_BASED_TAG_ESTIMATION',
                           'Error calling IDR Tag Estimation Dynamic SQL'
                        || CHR (13)
                        || vdsqldescription);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

       END IF;
        /****************/
        /*Tag Estimation*/
        /****************/

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_BASED_TAG_ESTIMATION',
                                  'End IDR BASED TAG_ESTIMATION');

        BEGIN
            --Get the Dynamic SQL Block to execute for Validation
            vblock := NULL;



            BEGIN
                --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
                SELECT source
                  INTO vblock
                  FROM alps_parser_dynsql
                 WHERE     active_flag = 'Y'
                       AND category = 'STRIP_TAG_ESTIMATION'
                       AND name = 'IDR_STRIP_TAG_ESTIMATION_RENEWALS';
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (
                        'Could not fetch the Dynamic SQL for IDR_RECT_TAG_ESTIMATION');
            END;

            --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));



            --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks

            IF LENGTH (vblock) > 0
            THEN
                vdsqlstatus := NULL;
                vdsqldescription := NULL;

                /*Inputs = FileName; Outputs = STATUS/DESCRIPTION */
                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_BASED_TAG_ESTIMATION',
                    'Begin calling Dynamic SQL with Inputs :1>>' || vfilename);

                EXECUTE IMMEDIATE vblock
                    USING vfilename,
                          p_batchid,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    4,
                    'DB',
                    NULL,
                    'IDR_STRIP',
                    'IDR_BASED_TAG_ESTIMATION',
                       'End calling Dynamic SQL with Outputs :2>>'
                    || vdsqlstatus
                    || ' :3>>'
                    || vdsqldescription);

                --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
                -- DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);

                IF vdsqlstatus <> 'SUCCESS'
                THEN
                    /*raise_application_error (
                       -20001,
                          'Error calling IDR Tag Estimation Dynamic SQL'
                       || CHR (13)
                       || vdsqldescription);*/
                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'IDR_STRIP',
                        'IDR_BASED_TAG_ESTIMATION',
                           'Error calling IDR Tag Estimation Dynamic SQL'
                        || CHR (13)
                        || vdsqldescription);
                END IF;
            END IF;
        EXCEPTION
            WHEN OTHERS
            THEN
                RAISE;
        END;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDR_BASED_TAG_ESTIMATION',
                                  'End IDR BASED TAG_ESTIMATION');
        --Outputs
        p_out_status := 'SUCCESS';

        --p_out_description := vdsqldescription; /*V alidation Result*/
        FOR rec
            IN (SELECT DISTINCT comments
                  FROM alps_parser_acct_note
                 WHERE     note_type = 'WARNING'
                       AND uid_batch = NVL (p_batchid, -99999)
                       AND comments IS NOT NULL)
        LOOP
            vcomment_description :=
                p_out_description || rec.comments || CHR (13);
        --p_out_description := p_out_description || rec.comments || CHR (13);
        END LOOP;

        SELECT SUBSTR (vcomment_description, 1, 2000)
          INTO p_out_description
          FROM DUAL;

        --
        COMMIT;
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'IDR_STRIP',
                                  'IDRSTRIP_TRANSLATION',
                                  'End IDR STRIP Process');
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            --Outputs
            p_out_status := 'ERROR';

            --Get the distinct Errors from the Note table for each meter and send as part of p_Out_Description



            FOR rec
                IN (SELECT DISTINCT comments
                      FROM alps_parser_acct_note
                     WHERE     note_type = 'ERROR'
                           AND uid_batch = NVL (p_batchid, -99999)
                           AND comments IS NOT NULL)
            LOOP
                vcomment_description :=
                    p_out_description || rec.comments || CHR (13);
            --p_out_description := p_out_description || rec.comments || CHR (13);
            END LOOP;

            SELECT SUBSTR (vcomment_description, 1, 2000)
              INTO p_out_description
              FROM DUAL;

            p_out_description :=
                   'Error encountered:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace
                || CHR (13)
                || p_out_description;



            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'IDR_STRIP',
                'IDRSTRIP_TRANSLATION',
                'Error in IDR STRIP Process' || CHR (13) || p_out_description);
    END;


    PROCEDURE initial_validation (p_batchid           IN     VARCHAR2,
                                  p_parsername        IN     VARCHAR2,
                                  p_out_status           OUT VARCHAR2,
                                  p_out_description      OUT VARCHAR2)
    IS
        vfilename          VARCHAR2 (255);
        vmarket            VARCHAR2 (30);
        vdisco             VARCHAR2 (30);
        vtablename         VARCHAR2 (100);
        vvalidationerror   VARCHAR2 (2000);
    BEGIN
        --Check if Parser name is in the list of Parser set to ignore the validation. if not proceed with Validation
        FOR recparser
            IN (SELECT p_parsername FROM DUAL
                MINUS
                (SELECT lookup_str_value1
                   FROM alps_mml_lookup
                  WHERE     lookup_group = 'ALPS_PARSING'
                        AND lookup_code LIKE 'SKIP_REQUIRED_VALIDATION_%'))
        LOOP
            --Get the file name
            FOR rec IN (SELECT *
                          FROM alps_parser_instance
                         WHERE uid_batch = p_batchid)
            LOOP
                vfilename := rec.input_filename;
            END LOOP;

            --Get the Market_code and dico_code for the given Parser Name
            FOR rec
                IN (SELECT *
                      FROM v_alps_parser_template
                     WHERE simx_parser_name = p_parsername AND ROWNUM < 2)
            LOOP
                vmarket := rec.market_code;
                vdisco := rec.disco_code;
                vtablename := rec.stagingtable;
            END LOOP;

            --Call Validation to make sure all required columns exist in the Post Parsing Data
            vvalidationerror :=
                alps_parser_pkg.f_validate_required (vmarket,
                                                     vdisco,
                                                     vfilename,
                                                     vtablename);
        END LOOP;

        --Outputs\
        IF vvalidationerror IS NULL
        THEN
            p_out_status := 'SUCCESS';
            p_out_description := NULL;
        ELSE
            p_out_status := 'ERROR';
            p_out_description :=
                   'Initail Validation for Required fields has returned following error:'
                || vvalidationerror;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            --Outputs
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error encountered calling initial validations for Parser:'
                || p_parsername
                || CHR (13)
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;
    END;

    PROCEDURE final_validation (p_batchid           IN     VARCHAR2,
                                p_parsername        IN     VARCHAR2,
                                p_out_status           OUT VARCHAR2,
                                p_out_description      OUT VARCHAR2)
    IS
    BEGIN
        --Outputs
        p_out_status := 'SUCCESS';
        p_out_description := NULL;
    EXCEPTION
        WHEN OTHERS
        THEN
            --Outputs
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error encountered:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;
    END;



    PROCEDURE load_data (p_batchid           IN     VARCHAR2,
                         p_parsername        IN     VARCHAR2,
                         p_out_status           OUT VARCHAR2,
                         p_out_description      OUT VARCHAR2)
    IS
        vnewuidaccountraw   NUMBER;
        vstatus             VARCHAR2 (100);
        vdescription        VARCHAR2 (2000);
        vcurrentcapval      alps_parser_sca_final.capacity%TYPE;
        vfuturecapval       alps_parser_sca_final.capacity%TYPE;
        vfuturecapind       VARCHAR2 (1);
        vtagrequest         INTEGER := 0;
        vparseattribute     alps_mml_lookup.lookup_str_value1%TYPE;
        vloadall            VARCHAR2 (3);
        vzonecode           alps_parser_sca_final.zone_code%TYPE;
        vadvmeter           VARCHAR2 (1);
        vcdaaccts           VARCHAR2 (3);
        cdauidaccountraw    INTEGER;
        vodmacctcnt         INTEGER;
        vcdaacctcnt         INTEGER;
        vcdauid             alps_continuous_req.uid_rec%TYPE;
        vodmuid             alps_odm_req.uid_rec%TYPE;



        --Cursor Distinct Meters
        CURSOR cmeters (vbatchid NUMBER)
        IS
            SELECT a.*,
                      a.market_code
                   || '_'
                   || a.disco_code
                   || '_'
                   || a.ldc_account    meterid,
                   b.input_filename,
                   b.updated_dt,
                   b.simx_parser_name
              FROM alps_parser_sca_final a, alps_parser_instance b
             WHERE     a.uid_batch = b.uid_batch
                   AND a.uid_record IN
                           (  SELECT MIN (uid_record)     uid_record
                                FROM alps_parser_sca_final
                               WHERE uid_batch = NVL (vbatchid, -9999)
                            GROUP BY market_code, disco_code, ldc_account)
                   AND a.uid_batch = NVL (vbatchid, -9999);

        --Cursor Usage Records by Meter
        CURSOR cmeterusage (vbatchid NUMBER, vmeterid VARCHAR2)
        IS
            SELECT *
              FROM alps_parser_sca_final
             WHERE     market_code || '_' || disco_code || '_' || ldc_account =
                       vmeterid
                   AND uid_batch = NVL (vbatchid, -9999);

        --Cursor Attribute (Cap And Trans) Records by Meter
        CURSOR cmeterattributes (vbatchid NUMBER, vmeterid VARCHAR2)
        IS
            --Get Capacity
            SELECT market_code,
                   disco_code,
                   'CAPACITY_TAG'
                       attr_code,
                   capacity
                       val,
                   alps_parser_pkg.f_get_planning_period_date ('CAPACITY',
                                                               market_code,
                                                               'START')
                       start_time,
                   alps_parser_pkg.f_get_planning_period_date ('CAPACITY',
                                                               market_code,
                                                               'STOP')
                       stop_time,
                   'H'
                       str_val
              --DECODE(disco_code,'RECO',  'H', NULL )  str_val
              FROM alps_parser_sca_final
             WHERE     uid_record IN
                           (  SELECT MIN (uid_record)     uid_record
                                FROM alps_parser_sca_final
                               WHERE     uid_batch = NVL (vbatchid, -9999)
                                     AND    market_code
                                         || '_'
                                         || disco_code
                                         || '_'
                                         || ldc_account =
                                         vmeterid
                                     --Market is in the sllowed list for Capacity
                                     AND market_code IN
                                             (SELECT DISTINCT
                                                     (TRIM (lookup_str_value1))    mkt_string
                                                FROM alps_mml_lookup
                                               WHERE     lookup_group =
                                                         'ALPS_PARSING'
                                                     AND lookup_code LIKE
                                                             'MARKET_CAPACITY_%')
                            GROUP BY market_code, disco_code, ldc_account)
                   AND uid_batch = NVL (vbatchid, -9999)
            UNION ALL
            --Get Transmission
            SELECT market_code,
                   disco_code,
                   'TRANSMISSION_TAG'    attr_code,
                   transmission          val,
                   alps_parser_pkg.f_get_planning_period_date (
                       'TRANSMISSION',
                       market_code,
                       'START')          start_time,
                   alps_parser_pkg.f_get_planning_period_date (
                       'TRANSMISSION',
                       market_code,
                       'STOP')           stop_time,
                   'H'                   str_val
              --DECODE(disco_code,'RECO',  'H', NULL )  str_val
              --null  str_val
              FROM alps_parser_sca_final
             WHERE     uid_record IN
                           (  SELECT MIN (uid_record)     uid_record
                                FROM alps_parser_sca_final
                               WHERE     uid_batch = NVL (vbatchid, -9999)
                                     AND    market_code
                                         || '_'
                                         || disco_code
                                         || '_'
                                         || ldc_account =
                                         vmeterid
                                     --Market is in the allowed list for Transmission
                                     AND market_code IN
                                             (SELECT DISTINCT
                                                     (TRIM (lookup_str_value1))    mkt_string
                                                FROM alps_mml_lookup
                                               WHERE     lookup_group =
                                                         'ALPS_PARSING'
                                                     AND lookup_code LIKE
                                                             'MARKET_TRANSMISSION_%')
                            GROUP BY market_code, disco_code, ldc_account)
                   AND uid_batch = NVL (vbatchid, -9999)
            UNION ALL
            --Get future Capacity
            SELECT market_code,
                   disco_code,
                   'CAPACITY_TAG'    attr_code,
                   attr1             val,
                   ADD_MONTHS (
                       alps_parser_pkg.f_get_planning_period_date (
                           'CAPACITY',
                           market_code,
                           'START'),
                       12)           start_time,
                   ADD_MONTHS (
                       alps_parser_pkg.f_get_planning_period_date (
                           'CAPACITY',
                           market_code,
                           'STOP'),
                       12)           stop_time,
                   'H'               str_val
              -- null  str_val
              FROM alps_parser_sca_final
             WHERE     uid_record IN
                           (  SELECT MIN (uid_record)     uid_record
                                FROM alps_parser_sca_final
                               WHERE     uid_batch = NVL (vbatchid, -9999)
                                     AND    market_code
                                         || '_'
                                         || disco_code
                                         || '_'
                                         || ldc_account =
                                         vmeterid
                                     --Market is in the allowed list for Transmission
                                     AND market_code IN
                                             (SELECT DISTINCT
                                                     (TRIM (lookup_str_value1))    mkt_string
                                                FROM alps_mml_lookup
                                               WHERE     lookup_group =
                                                         'ALPS_PARSING'
                                                     AND lookup_code LIKE
                                                             'MARKET_FUTURE_CAP_TRANS%')
                            GROUP BY market_code, disco_code, ldc_account)
                   AND uid_batch = NVL (vbatchid, -9999)
                   AND attr1 IS NOT NULL
            UNION ALL
            --Get future TRANSMISSION
            SELECT market_code,
                   disco_code,
                   'TRANSMISSION_TAG'    attr_code,
                   attr2                 val,
                   ADD_MONTHS (
                       alps_parser_pkg.f_get_planning_period_date (
                           'TRANSMISSION',
                           market_code,
                           'START'),
                       12)               start_time,
                   ADD_MONTHS (
                       alps_parser_pkg.f_get_planning_period_date (
                           'TRANSMISSION',
                           market_code,
                           'STOP'),
                       12)               stop_time,
                   'H'                   str_val
              --NULL  str_val
              FROM alps_parser_sca_final
             WHERE     uid_record IN
                           (  SELECT MIN (uid_record)     uid_record
                                FROM alps_parser_sca_final
                               WHERE     uid_batch = NVL (vbatchid, -9999)
                                     AND    market_code
                                         || '_'
                                         || disco_code
                                         || '_'
                                         || ldc_account =
                                         vmeterid
                                     --Market is in the allowed list for Transmission
                                     AND market_code IN
                                             (SELECT DISTINCT
                                                     (TRIM (lookup_str_value1))    mkt_string
                                                FROM alps_mml_lookup
                                               WHERE     lookup_group =
                                                         'ALPS_PARSING'
                                                     AND lookup_code LIKE
                                                             'MARKET_FUTURE_CAP_TRANS%')
                            GROUP BY market_code, disco_code, ldc_account)
                   AND uid_batch = NVL (vbatchid, -9999)
                   AND attr2 IS NOT NULL;
    BEGIN
        FOR rec IN cmeters (p_batchid)
        LOOP
            DBMS_OUTPUT.put_line ('Processing meterid ==> ' || rec.meterid);

            --  ODM  load by DISCO code
            SELECT COUNT (1)
              INTO vodmacctcnt
              FROM alps_mml_lookup
             WHERE     lookup_group = 'ODM_DISCO'
                   AND lookup_code = rec.disco_code;

            /* SELECT COUNT (1), MAX (uid_rec)
               INTO vOdmAcctCnt, vOdmUId
               FROM ALPS_ODM_REQ
              WHERE meter_id = rec.meterid ;
              --AND status = 'REQUEST_COMPLETE';
              */

            -- CDA load
            SELECT COUNT (1), MAX (uid_rec)
              INTO vcdaacctcnt, vcdauid
              FROM alps_continuous_req
             WHERE meter_id = rec.meterid AND status = 'REQUEST_COMPLETE';

            DBMS_OUTPUT.put_line ('vCdaAcctCnt ==> ' || vcdaacctcnt);
            DBMS_OUTPUT.put_line ('vOdmAcctCnt ==> ' || vodmacctcnt);

            --
            IF vcdaacctcnt > 0
            THEN
                BEGIN
                    vnewuidaccountraw :=
                        alps.alps_data_pkg.f_insert_alps_account_raw (
                            rec.uid_batch,
                            rec.simx_parser_name,
                            rec.ldc_account,
                            rec.market_code,
                            rec.disco_code,
                            NULL,
                            rec.address1,
                            rec.address2,
                            rec.city,
                            rec.state_code,
                            rec.zip,
                            rec.rate_class,
                            rec.rate_subclass,
                            REPLACE (rec.strata, ',', ''),
                            rec.voltage_class,
                            rec.load_profile,
                            rec.meter_cycle,
                            rec.stationid,
                            rec.zone_code,
                            rec.meter_type,
                            rec.county,
                            rec.country,
                            vadvmeter,
                            'CDA');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        raise_application_error (
                            -20000,
                               'Unable to insert data into CDA_ACCOUNT_RAW table from ALPS Parsing'
                            || CHR (13)
                            || SQLERRM
                            || CHR (13)
                            || DBMS_UTILITY.format_error_backtrace);
                END;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    1,
                    'DB',
                    NULL,
                    'SCA',
                    'LOAD_DATA_INTO_CDA',
                       'Completed Loading Data into CDA_ACCOUNT_RAW for MeterId:'
                    || rec.meterid);

                --Usage loading
                IF vnewuidaccountraw > 0
                THEN
                    BEGIN
                        FOR recusage IN cmeterusage (p_batchid, rec.meterid)
                        LOOP
                            alps.alps_data_pkg.insert_alps_usage_raw (
                                vnewuidaccountraw,
                                TO_DATE (recusage.start_time,
                                         'MM/DD/YYYY HH24:MI:SS'),
                                TO_DATE (recusage.stop_time,
                                         'MM/DD/YYYY HH24:MI:SS'),
                                REPLACE (recusage.usage_total, ',', ''),
                                REPLACE (recusage.demand_total, ',', ''),
                                'H',
                                'CDA');
                        END LOOP;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into CDA_USAGE_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;

                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'SCA',
                        'LOAD_DATA_INTO_CDA',
                           'Completed Loading Data into CDA_USAGE_RAW for MeterId:'
                        || rec.meterid);

                    -- Tags loading
                    BEGIN
                        FOR recattrib
                            IN cmeterattributes (p_batchid, rec.meterid)
                        LOOP
                            alps.alps_data_pkg.insert_alps_acct_attr_raw (
                                vnewuidaccountraw,
                                recattrib.attr_code,
                                recattrib.start_time,
                                recattrib.stop_time,
                                recattrib.val,
                                recattrib.str_val,
                                'CDA');
                        END LOOP;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into CDA_ACCOUNT_ATTR_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;

                    -- Update to DATA_RECEIVED
                    UPDATE alps_continuous_req
                       SET status = 'DATA_RECEIVED',
                           resp_dt = rec.updated_dt,
                           resp_file = rec.input_filename
                     WHERE uid_rec = vcdauid;
                END IF;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    1,
                    'DB',
                    NULL,
                    'SCA',
                    'LOAD_DATA_INTO_RAW_CDA',
                       'Completed Loading Data into CDA_ACCOUNT_ATTR_RAW for MeterId:'
                    || rec.meterid);
            ELSIF vodmacctcnt > 0 AND rec.simx_parser_name = 'EDI_USGH_SIMX'
            THEN
                BEGIN
                    vnewuidaccountraw :=
                        alps.alps_data_pkg.f_insert_alps_account_raw (
                            rec.uid_batch,
                            rec.simx_parser_name,
                            rec.ldc_account,
                            rec.market_code,
                            rec.disco_code,
                            NULL,
                            rec.address1,
                            rec.address2,
                            rec.city,
                            rec.state_code,
                            rec.zip,
                            rec.rate_class,
                            rec.rate_subclass,
                            REPLACE (rec.strata, ',', ''),
                            rec.voltage_class,
                            rec.load_profile,
                            rec.meter_cycle,
                            rec.stationid,
                            rec.zone_code,
                            rec.meter_type,
                            rec.county,
                            rec.country,
                            vadvmeter,
                            'ODM');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        raise_application_error (
                            -20000,
                               'Unable to insert data into ODM_ACCOUNT_RAW table from ALPS Parsing'
                            || CHR (13)
                            || SQLERRM
                            || CHR (13)
                            || DBMS_UTILITY.format_error_backtrace);
                END;

                DBMS_OUTPUT.put_line (
                    'new ODM uidaccountraw ==> ' || vnewuidaccountraw);

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    1,
                    'DB',
                    NULL,
                    'SCA',
                    'LOAD_DATA_INTO_RAW_ODM',
                       'Completed Loading into ODM_ACCOUNT_RAW for MeterId:'
                    || rec.meterid);

                --Usage loading
                IF vnewuidaccountraw > 0
                THEN
                    BEGIN
                        FOR recusage IN cmeterusage (p_batchid, rec.meterid)
                        LOOP
                            alps.alps_data_pkg.insert_alps_usage_raw (
                                vnewuidaccountraw,
                                TO_DATE (recusage.start_time,
                                         'MM/DD/YYYY HH24:MI:SS'),
                                TO_DATE (recusage.stop_time,
                                         'MM/DD/YYYY HH24:MI:SS'),
                                REPLACE (recusage.usage_total, ',', ''),
                                REPLACE (recusage.demand_total, ',', ''),
                                'H',
                                'ODM');
                        END LOOP;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into ODM_USAGE_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;

                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'SCA',
                        'LOAD_DATA_INTO_ODM',
                           'Completed Loading Data into ODM_USAGE_RAW for MeterId:'
                        || rec.meterid);


                    -- Tags loading
                    BEGIN
                        FOR recattrib
                            IN cmeterattributes (p_batchid, rec.meterid)
                        LOOP
                            alps.alps_data_pkg.insert_alps_acct_attr_raw (
                                vnewuidaccountraw,
                                recattrib.attr_code,
                                recattrib.start_time,
                                recattrib.stop_time,
                                recattrib.val,
                                recattrib.str_val,
                                'ODM');
                        END LOOP;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into ODM_ACCOUNT_ATTR_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;

                    -- Update to DATA_RECEIVED
                    UPDATE alps_odm_req
                       SET status = 'DATA_RECEIVED',
                           resp_dt = rec.updated_dt,
                           resp_file = rec.input_filename
                     WHERE     market_code = rec.market_code
                           AND disco_code = rec.disco_code
                           AND ldc_account = rec.ldc_account;
                END IF;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    1,
                    'DB',
                    NULL,
                    'SCA',
                    'LOAD_DATA_INTO_ODM',
                       'Completed Loading Data into ODM_ACCOUNT_ATTR_RAW for MeterId:'
                    || rec.meterid);
            ELSE
                --Load into ALPS raw
                vnewuidaccountraw := 0;
                vtagrequest := 0;

                --
                SELECT SUBSTR (rec.attr1, 1, 1) INTO vadvmeter FROM DUAL;

                BEGIN
                    vnewuidaccountraw :=
                        alps.alps_data_pkg.f_insert_alps_account_raw (
                            rec.uid_batch,
                            rec.simx_parser_name,
                            rec.ldc_account,
                            rec.market_code,
                            rec.disco_code,
                            NULL,
                            rec.address1,
                            rec.address2,
                            rec.city,
                            rec.state_code,
                            rec.zip,
                            rec.rate_class,
                            rec.rate_subclass,
                            REPLACE (rec.strata, ',', ''),
                            rec.voltage_class,
                            rec.load_profile,
                            rec.meter_cycle,
                            rec.stationid,
                            rec.zone_code,
                            rec.meter_type,
                            rec.county,
                            rec.country,
                            vadvmeter,
                            'RAW');
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        raise_application_error (
                            -20000,
                               'Unable to insert data into ALPS_ACCOUNT_RAW table from ALPS Parsing'
                            || CHR (13)
                            || SQLERRM
                            || CHR (13)
                            || DBMS_UTILITY.format_error_backtrace);
                END;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    1,
                    'DB',
                    NULL,
                    'SCA',
                    'LOAD_DATA_INTO_RAW',
                       'Completed Loading Data into ALPS_ACCOUNT_ATTR_RAW for MeterId:'
                    || rec.meterid);

                DBMS_OUTPUT.put_line (
                    'new vnewuidaccountraw ==> ' || vnewuidaccountraw);

                --Usage loading
                IF vnewuidaccountraw > 0
                THEN
                    BEGIN
                        FOR recusage IN cmeterusage (p_batchid, rec.meterid)
                        LOOP
                            alps.alps_data_pkg.insert_alps_usage_raw (
                                vnewuidaccountraw,
                                TO_DATE (recusage.start_time,
                                         'MM/DD/YYYY HH24:MI:SS'),
                                TO_DATE (recusage.stop_time,
                                         'MM/DD/YYYY HH24:MI:SS'),
                                REPLACE (recusage.usage_total, ',', ''),
                                REPLACE (recusage.demand_total, ',', ''),
                                'H',
                                'RAW');
                        END LOOP;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into ALPS_USAGE_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;

                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'SCA',
                        'LOAD_DATA_INTO_RAW',
                           'Completed Loading Data into ALPS_USAGE_RAW for MeterId:'
                        || rec.meterid);

                    -- Tags loading
                    BEGIN
                        FOR recattrib
                            IN cmeterattributes (p_batchid, rec.meterid)
                        LOOP
                            alps.alps_data_pkg.insert_alps_acct_attr_raw (
                                vnewuidaccountraw,
                                recattrib.attr_code,
                                recattrib.start_time,
                                recattrib.stop_time,
                                recattrib.val,
                                recattrib.str_val,
                                'RAW');
                        END LOOP;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into ALPS_ACCOUNT_ATTR_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;
                    -- Load Seasonal tags
                    --User story 59395/Task 61950
                    P_Load_Seasonal_Tags(vnewuidaccountraw,
                                          p_batchid,
                                         rec.meterid );

                    --
                    alps.alps_parser_pkg.LOG (
                        USER,
                        p_batchid,
                        1,
                        'DB',
                        NULL,
                        'SCA',
                        'LOAD_DATA_INTO_RAW',
                           'Completed Loading Data into ALPS_ACCOUNT_ATTR_RAW for MeterId:'
                        || rec.meterid);

                    --
                    BEGIN
                        alps.alps_data_pkg.insert_edi_acct_attr_raw (
                            vnewuidaccountraw,
                            rec.meterid,
                            vstatus,
                            vdescription);

                        IF vstatus <> 'SUCCESS'
                        THEN
                            raise_application_error (-20001, vdescription);
                        END IF;
                    EXCEPTION
                        WHEN OTHERS
                        THEN
                            raise_application_error (
                                -20000,
                                   'Unable to insert data into ALPS_ACCOUNT_ATTR_RAW table from ALPS Parsing'
                                || CHR (13)
                                || SQLERRM
                                || CHR (13)
                                || DBMS_UTILITY.format_error_backtrace);
                    END;
                END IF;

                alps.alps_parser_pkg.LOG (
                    USER,
                    p_batchid,
                    1,
                    'DB',
                    NULL,
                    'SCA',
                    'LOAD_DATA_INTO_RAW',
                       'Completed Loading Data into ALPS RAW Tables for MeterId:'
                    || rec.meterid);
            END IF;
        END LOOP;

        -- ODM related clean up
        DELETE FROM
            alps_parser_sca_final a
              WHERE     a.uid_batch = p_batchid
                    AND EXISTS
                            (SELECT 1
                               FROM alps_parser_instance b, alps_odm_req c
                              WHERE     a.uid_batch = b.uid_batch
                                    AND b.input_filename = c.resp_file
                                    AND simx_parser_name = 'EDI_USGH_SIMX'
                                    AND c.status = 'DATA_RECEIVED');

        --Outputs
        p_out_status := 'SUCCESS';
        p_out_description := NULL;
    EXCEPTION
        --Exception Handling
        WHEN OTHERS
        THEN
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error in Procedure Load_Data:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;

            alps.alps_parser_pkg.LOG (USER,
                                      p_batchid,
                                      1,
                                      'DB',
                                      NULL,
                                      'SCA',
                                      'LOAD_DATA_INTO_RAW',
                                      p_out_description);
    END;

    PROCEDURE create_parser_instance (
        p_callersource            IN     VARCHAR2,
        p_simx_parser_name        IN     VARCHAR2,
        p_inputfilename           IN     VARCHAR2,
        p_comments                IN     VARCHAR2,
        p_templatefilename        IN     VARCHAR2,
        p_inputfilenamefullpath   IN     VARCHAR2,
        p_out_batchid                OUT VARCHAR2,
        p_out_status                 OUT VARCHAR2,
        p_out_description            OUT VARCHAR2)
    IS
        --ALPS_PARSEINSTANCE_SEQ
        voutbatchid   NUMBER;
    BEGIN
        voutbatchid := alps.alps_parseinstance_seq.NEXTVAL;

        --INSERT Record
        INSERT INTO alps.alps_parser_instance (uid_batch,
                                               caller_source,
                                               simx_parser_name,
                                               input_filename,
                                               comments,
                                               template_filename,
                                               input_file_fullpath)
             VALUES (voutbatchid,
                     p_callersource,
                     p_simx_parser_name,
                     p_inputfilename,
                     p_comments,
                     p_templatefilename,
                     p_inputfilenamefullpath);

        --Outputs
        p_out_batchid := voutbatchid;
        p_out_status := 'SUCCESS';
        p_out_description := NULL;
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            --Outputs
            p_out_batchid := NULL;
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error encountered:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;
    END;



    PROCEDURE LOG (in_user          IN VARCHAR2,
                   in_uidbatch      IN NUMBER,
                   in_loglevel      IN NUMBER,
                   in_source        IN VARCHAR2,
                   in_element       IN VARCHAR2,
                   in_processname   IN VARCHAR2,
                   in_processstep   IN VARCHAR2,
                   in_message       IN VARCHAR2)
    AS
        PRAGMA AUTONOMOUS_TRANSACTION;
        vlookuploglevel   NUMBER := 0;
    BEGIN
        --Get LOG Level
        BEGIN
            SELECT /*+RESULT_CACHE*/
                   lookup_num_value1
              INTO vlookuploglevel
              FROM alps_mml_lookup
             WHERE     lookup_group = 'ALPS_PARSING'
                   AND lookup_code = 'DB_LOG_LEVEL';
        EXCEPTION
            WHEN OTHERS
            THEN
                vlookuploglevel := 0;
        END;


        IF NVL (in_loglevel, 0) <= vlookuploglevel
        THEN
            INSERT INTO alps_parser_log (log_level,
                                         uid_batch,
                                         log_source,
                                         log_element,
                                         process_name,
                                         process_step,
                                         text)
                 VALUES (in_loglevel,
                         in_uidbatch,
                         in_source,                       /*DB or WEBSERVICE*/
                         in_element,                         /*METER ID Etc.*/
                         in_processname,      /*SCA, IDR_STRIP, IDR_RECT etc*/
                         in_processstep,     /*VALIDATION, TRANSORMATION Etc*/
                         SUBSTR (in_message, 1, 4000)); /*Message to be logged*/

            COMMIT;
        END IF;
    END LOG;



    FUNCTION f_get_planning_period_date (tag_type      IN VARCHAR2,
                                         market_code   IN VARCHAR2,
                                         date_type     IN VARCHAR2)
        RETURN DATE
    IS
        --**********************************
        -- Valid Tag Types: CAPACITY, TRANSMISSION
        -- Valid date Types: START, STOP
        --**********************************
        vdate   DATE;
    BEGIN
        FOR rec
            IN (SELECT lookup_str_value2     planning_start
                  FROM alps_mml_lookup
                 WHERE     lookup_group = 'ALPS_PARSING'
                       AND lookup_code LIKE
                               DECODE (
                                   tag_type,
                                   'CAPACITY', 'MARKET_CAPACITY%',
                                   'TRANSMISSION', 'MARKET_TRANSMISSION%',
                                   'DUMMY123')
                       AND lookup_str_value1 = market_code)
        LOOP
            IF TO_CHAR (SYSDATE, 'MMDD') >= rec.planning_start
            THEN
                vdate :=
                    TO_DATE (rec.planning_start || TO_CHAR (SYSDATE, 'YYYY'),
                             'MMDDYYYY');
            ELSE
                vdate :=
                    ADD_MONTHS (
                        TO_DATE (
                            rec.planning_start || TO_CHAR (SYSDATE, 'YYYY'),
                            'MMDDYYYY'),
                        -12);
            END IF;
        END LOOP;

        IF date_type = 'STOP'
        THEN
            vdate := ADD_MONTHS (vdate, 12) - (1 / (24 * 60 * 60));
        END IF;

        RETURN vdate;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (SQLERRM);
            RETURN NULL;
    END f_get_planning_period_date;


    PROCEDURE process_sca (p_batchid           IN     VARCHAR2,
                           p_parsername        IN     VARCHAR2,
                           p_out_status           OUT VARCHAR2,
                           p_out_description      OUT VARCHAR2)
    IS
        v_out_status         VARCHAR2 (100);
        v_out_description    VARCHAR2 (2000);
        v_inputfile          alps_parser_instance.input_filename%TYPE;
        v_simx_parser_name   alps_parser_instance.simx_parser_name%TYPE
                                 := p_parsername;
    BEGIN
        vcomment_description := NULL;
        p_out_description := NULL;

        --Load EDI Data for the incoming batch
        FOR rec
            IN (SELECT simx_parser_name, input_filename
                  FROM alps_parser_instance
                 WHERE     uid_batch = NVL (p_batchid, -9999)
                       AND INSTR (simx_parser_name, 'USGH') > 0)
        LOOP
            v_inputfile := rec.input_filename;
            v_simx_parser_name := rec.simx_parser_name;
            --
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'CALL_PROCESS_SCA',
                   'Begin load_edi_data'
                || CHR (13)
                || 'INPUTS: p_batchid/p_Parsername'
                || CHR (13)
                || p_batchid
                || '/'
                || p_parsername);



            BEGIN
                alps.load_edi_data (p_batchid,
                                    rec.input_filename,
                                    v_out_status,
                                    v_out_description);
            EXCEPTION
                WHEN OTHERS
                THEN
                    v_out_status := 'ERROR';
                    v_out_description :=
                           SQLERRM
                        || CHR (13)
                        || DBMS_UTILITY.format_error_backtrace ();
            END;

            --



            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'CALL_PROCESS_SCA',
                   'End load_edi_data'
                || CHR (13)
                || 'INPUTS: p_batchid/p_Parsername'
                || CHR (13)
                || p_batchid
                || '/'
                || p_parsername
                || CHR (13)
                || 'Status:'
                || v_out_status);

            --If error, then exit the Coutine
            IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
            THEN
                raise_application_error (-20001, v_out_description);
            END IF;

            --Check if no Data
            FOR recdata
                IN (SELECT COUNT (1)     reccount
                      FROM alps_parser_sca_output
                     WHERE UPPER (inputfile) = UPPER (rec.input_filename))
            LOOP
                IF recdata.reccount = 0
                THEN
                    raise_application_error (
                        -20001,
                           'No Data after LOAD_EDI_DATA for file:'
                        || UPPER (rec.input_filename));
                END IF;
            END LOOP;
        END LOOP;

        --Get the file name
        FOR rec IN (SELECT input_filename
                      FROM alps_parser_instance
                     WHERE uid_batch = p_batchid)
        LOOP
            v_inputfile := rec.input_filename;
        END LOOP;



        --
        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_PROCESS_SCA',
               'Begin PROCESS SCA'
            || CHR (13)
            || 'INPUTS: p_batchid/p_Parsername'
            || CHR (13)
            || p_batchid
            || '/'
            || p_parsername);

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'SCALAR_REQUIRED_COLVALIDATION',
                                  'Before Scalar Translation');

        --Initial Validation
        BEGIN
            initial_validation (p_batchid,
                                p_parsername,
                                v_out_status,
                                v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        --If error, then exit the Coutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'SCALAR_REQUIRED_COLVALIDATION',
               'After SCA Required Column Validation'
            || CHR (13)
            || 'v_out_status:'
            || v_out_status);


        IF INSTR (p_parsername, 'USGH') > 0
        THEN
            --
            alps.alps_parser_pkg.LOG (USER,
                                      p_batchid,
                                      1,
                                      'DB',
                                      NULL,
                                      'SCA',
                                      'CALL_PROCESS_SCA',
                                      'Before Zero Qty/Demand check');


            BEGIN
                alps.zero_qty_demand_chk (p_batchid,
                                          v_inputfile,
                                          v_out_status,
                                          v_out_description);
            EXCEPTION
                WHEN OTHERS
                THEN
                    v_out_status := 'ERROR';
                    v_out_description :=
                           SQLERRM
                        || CHR (13)
                        || DBMS_UTILITY.format_error_backtrace ();
            END;

            --If error, then exit the Coutine
            IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
            THEN
                raise_application_error (-20001, v_out_description);
            END IF;

            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'CALL_PROCESS_SCA',
                   'After Zero Qty/Demand Check'
                || CHR (13)
                || 'v_out_status:'
                || v_out_status);
        END IF;

        --
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_PROCESS_SCA',
                                  'Before Read Dates Overlap Check');

        --
        BEGIN
            alps.read_dates_overlap (p_batchid,
                                     v_inputfile,
                                     v_out_status,
                                     v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        --If error, then exit the Coutine



        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;



        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_PROCESS_SCA',
               'After Read Dates Overlap Check'
            || CHR (13)
            || 'v_out_status:'
            || v_out_status);

        --
        IF (   INSTR (p_parsername, 'USGH') > 0
            OR INSTR (p_parsername, 'USHI') > 0)
        THEN
            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'CALL_PROCESS_SCA',
                'Before Applying Weather Normalization factor');



            --
            BEGIN
                alps.apply_weather_factors (p_batchid,
                                            v_inputfile,
                                            v_out_status,
                                            v_out_description);
            EXCEPTION
                WHEN OTHERS
                THEN
                    v_out_status := 'ERROR';
                    v_out_description :=
                           SQLERRM
                        || CHR (13)
                        || DBMS_UTILITY.format_error_backtrace ();
            END;



            --If error, then exit the Coutine
            IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
            THEN
                raise_application_error (-20001, v_out_description);
            END IF;

            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'CALL_PROCESS_SCA',
                   'After Applying Weather Normalization factor'
                || CHR (13)
                || 'v_out_status:'
                || v_out_status);
        END IF;

        --
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_SCALAR_TRANSLATION',
                                  'Before Scalar Translation');

        --Transform Data
        BEGIN
            scalar_translation (p_batchid,
                                p_parsername,
                                v_out_status,
                                v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        --If error, then exit the Coutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'SCALAR_TRANSLATION',
               'After SCA Translation'
            || CHR (13)
            || 'v_out_status:'
            || v_out_status);

        -- Sync MRC to Siebel
        /*
           alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  Null,
                                  'SCA',
                                  'CALL_PROCESS_SCA',
                                  'Before Sync MRC to Siebel');

        --
        Begin
           alps.sync_mrc_to_siebel (p_batchid, v_out_status, v_out_description);
        Exception
           When Others
           Then
              v_out_status := 'ERROR';
              v_out_description :=
                 SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ();
        End;

        --If error, then exit the Coutine
        If NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        Then
           raise_application_error (-20001, v_out_description);
        End If;

        alps.alps_parser_pkg.
         LOG (
           USER,
           p_batchid,
           1,
           'DB',
           Null,
           'SCA',
           'CALL_PROCESS_SCA',
              'After Sync MRC to Siebel'
           || CHR (13)
           || 'v_out_status:'
           || v_out_status);

      */
        --Perform Final Validations
        v_out_status := NULL;
        v_out_description := NULL;

        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_FINAL_VALIDATIONS',
                                  'Before SCA Final Validations');

        BEGIN
            final_validation (p_batchid,
                              p_parsername,
                              v_out_status,
                              v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_FINAL_VALIDATIONS',
               'After SCA Final Validations'
            || CHR (13)
            || 'v_out_status:'
            || v_out_status);

        --If error, then exit the soutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;

        -- Load MRC



        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_LOAD_MRC',
                                  'Before Load MRC');
        --Load data into ALPS Raw Tables..
        v_out_status := NULL;
        v_out_description := NULL;

        BEGIN
            NULL;
            --v_out_status := 'SUCCESS';

            alps.load_mrc (p_batchid,
                           p_parsername,
                           v_out_status,
                           v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;



        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_LOAD_MRC',
            'After Load MRC' || CHR (13) || 'v_out_status:' || v_out_status);


        --If error, then exit the soutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;


        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_LOAD_DATA',
                                  'Before Load Data into ALPS');
        --Load data into ALPS Raw Tables..
        v_out_status := NULL;
        v_out_description := NULL;

        BEGIN
            NULL;
            --v_out_status := 'SUCCESS';

            load_data (p_batchid,
                       p_parsername,
                       v_out_status,
                       v_out_description);
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

        alps.alps_parser_pkg.LOG (
            USER,
            p_batchid,
            1,
            'DB',
            NULL,
            'SCA',
            'CALL_LOAD_DATA',
               'After Load Data into ALPS'
            || CHR (13)
            || 'v_out_status:'
            || v_out_status);

        --If error, then exit the soutine
        IF NVL (v_out_status, 'ERROR') <> 'SUCCESS'
        THEN
            raise_application_error (-20001, v_out_description);
        END IF;

  alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_RENEWAL_IDR',
                                  'Before process Renewal IDR');
        --Load data into ALPS Raw Tables..
        v_out_status := NULL;
        v_out_description := NULL;
       -- Process Renewal IDR data from ALPS_PARSER_IDR_STRIP_OUTPUT
        BEGIN
        FOR rec IN
        (
        SELECT DISTINCT a.uid_batch,a.simx_parser_name, c.inputfile
        FROM alps_parser_instance a,
             v_alps_bip_renewal_source c
        WHERE a.uid_batch = p_batchid
        AND a.simx_parser_name = 'Renewal Excel'
        AND a.input_filename = c.inputfile
        )
            LOOP
            ALPS.alps_parser_pkg.idrstrip_translation (p_batchid ,
                                                    rec.simx_parser_name,
                                                    p_out_status,
                                                    p_out_description
                                                    );
            --dbms_output.put_line(' p_out_status : '||p_out_status);
            --dbms_output.put_line(' p_out_description : '||p_out_description);

            END LOOP;
        EXCEPTION
            WHEN OTHERS
            THEN
                v_out_status := 'ERROR';
                v_out_description :=
                       SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace ();
        END;

   alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_RENEWAL_IDR',
                                  'End process Renewal IDR');


        --Create Log of all the Accounts involved in current batch of Parsing with corresponding notes
        INSERT INTO alps_parser_acct_note (uid_batch,
                                           meter_id,
                                           comments,
                                           meter_type,
                                           note_type)
            SELECT DISTINCT
                   uid_batch,
                   market_code || '_' || disco_code || '_' || ldc_account
                       meter_id,
                   parse_comments,
                   'SCALAR',
                   'WARNING'
              FROM alps_parser_sca_final
             WHERE uid_batch = p_batchid;

        --Load Data into ALPS Raw Tables
        p_out_status := 'SUCCESS';
        p_out_description := NULL;

        --Get the distinct warnings from the Note table for each meter and send as part of p_Out_Description
        FOR rec
            IN (SELECT DISTINCT comments
                  FROM alps_parser_acct_note
                 WHERE     note_type = 'WARNING'
                       AND uid_batch = NVL (p_batchid, -99999)
                       AND comments IS NOT NULL)
        LOOP
            vcomment_description :=
                p_out_description || rec.comments || CHR (13);
        --p_out_description := p_out_description || rec.comments || CHR (13);
        END LOOP;

        SELECT SUBSTR (vcomment_description, 1, 2000)
          INTO p_out_description
          FROM DUAL;

        COMMIT;
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  'SCA',
                                  'CALL_PROCESS_SCA',
                                  'End PROCESS SCA');
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            --Outputs
            p_out_status := 'ERROR';

            --Get the distinct Errors from the Note table for each meter and send as part of p_Out_Description
            FOR rec
                IN (SELECT DISTINCT comments
                      FROM alps_parser_acct_note
                     WHERE     note_type = 'ERROR'
                           AND uid_batch = NVL (p_batchid, -99999)
                           AND comments IS NOT NULL)
            LOOP
                vcomment_description :=
                    p_out_description || rec.comments || CHR (13);
            --p_out_description := p_out_description || rec.comments || CHR (13);
            END LOOP;

            SELECT SUBSTR (vcomment_description, 1, 2000)
              INTO p_out_description
              FROM DUAL;

            /*     LOOP
                    IF LENGTH (p_out_description || rec.comments || CHR (13)) <= 2000
                    THEN
                       p_out_description :=
                          p_out_description || rec.comments || CHR (13);
                    END IF;
                 END LOOP;
        */
            p_out_description :=
                SUBSTR (
                       'Error encountered:'
                    || SQLERRM
                    || CHR (13)
                    || DBMS_UTILITY.format_error_backtrace
                    || CHR (13)
                    || p_out_description,
                    1,
                    2000);

            alps.alps_parser_pkg.LOG (
                USER,
                p_batchid,
                1,
                'DB',
                NULL,
                'SCA',
                'PROCESS_SCA',
                SUBSTR (p_out_status || ': ' || p_out_description, 1, 2000));
    END process_sca;

    PROCEDURE truncate_idr_output (p_batchid           IN     VARCHAR2,
                                   p_inputfile         IN     VARCHAR2,
                                   p_type              IN     VARCHAR2,
                                   p_out_status           OUT VARCHAR2,
                                   p_out_description      OUT VARCHAR2)
    IS
        ncount          NUMBER;
        vprocessname    VARCHAR2 (100) := 'IDR_CLEANUP';
        vstepname       VARCHAR2 (100) := 'IDR_CLEANUP';
        vmessage        VARCHAR2 (2000);
        nrecseffected   NUMBER;
    BEGIN
        /* Get IN_PROGRESS parsing instances in the last 2 hours */
        SELECT COUNT (1)
          INTO ncount
          FROM alps_parser_instance a
         WHERE     status = 'IN_PROGRESS'
               AND uid_batch != NVL (p_batchid, -9999) --Created in the last 2 Hours
               AND created_dt >= SYSDATE + INTERVAL '-120' MINUTE;

        /*Handle based on the input type*/

        IF ncount = 0
        THEN
            IF p_type = 'STRIP'
            THEN
                vprocessname := 'IDR_STRIP';
                vstepname := 'IDR_STRIP_TRUNCATE';

                EXECUTE IMMEDIATE 'truncate table alps_parser_idr_strip_output';

                EXECUTE IMMEDIATE 'truncate table GT_ALPS_PARSER_IDR_STRIP87';


                vmessage :=
                    'Truncate of alps_parser_idr_strip_output completed..';
            ELSIF p_type = 'RECTANGULAR'
            THEN
                vprocessname := 'IDR_RECT';
                vstepname := 'IDR_RECT_TRUNCATE';

                EXECUTE IMMEDIATE 'truncate table alps_parser_idr_rect_output';


                vmessage :=
                    'Truncate of alps_parser_idr_rect_output completed..';
            END IF;
        ELSE
            IF p_type = 'STRIP'
            THEN
                vprocessname := 'IDR_STRIP';
                vstepname := 'IDR_STRIP_DELETE';


                DELETE FROM
                    alps_parser_idr_strip_output
                      WHERE UPPER (inputfile) =
                            UPPER (NVL (p_inputfile, 'DUMMY'));


                DELETE FROM
                    alps_parser_idr_strip87_output
                      WHERE UPPER (inputfile) =
                            UPPER (NVL (p_inputfile, 'DUMMY'));

                vmessage :=
                       'Delete on alps_parser_idr_strip_output for Inputfile-->'
                    || p_inputfile
                    || ' completed. Records effected:'
                    || TO_CHAR (SQL%ROWCOUNT);
            ELSIF p_type = 'RECTANGULAR'
            THEN
                vprocessname := 'IDR_RECT';
                vstepname := 'IDR_RECT_DELETE';


                DELETE FROM
                    alps_parser_idr_rect_output
                      WHERE UPPER (inputfile) =
                            UPPER (NVL (p_inputfile, 'DUMMY'));

                vmessage :=
                       'Delete on alps_parser_idr_rect_output for Inputfile-->'
                    || p_inputfile
                    || ' completed. Records effected:'
                    || TO_CHAR (SQL%ROWCOUNT);
            END IF;
        END IF;

        --Load Data into ALPS Raw Tables
        p_out_status := 'SUCCESS';
        p_out_description := NULL;

        COMMIT;

        /*Log the record*/
        alps.alps_parser_pkg.LOG (USER,
                                  p_batchid,
                                  1,
                                  'DB',
                                  NULL,
                                  vprocessname,
                                  vstepname,
                                  vmessage);
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            --Outputs
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error encountered:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;

            /*Log the error*/
            alps.alps_parser_pkg.LOG (USER,
                                      p_batchid,
                                      1,
                                      'DB',
                                      NULL,
                                      vprocessname,
                                      vstepname,
                                      p_out_description);
    END truncate_idr_output;

    FUNCTION f_get_avg_value (hour_current    IN VARCHAR2,
                              hour_previous   IN VARCHAR2,
                              hour_next       IN VARCHAR2)
        RETURN VARCHAR2
    IS
        vreturn   VARCHAR2 (30);
    BEGIN
        SELECT CASE
                   WHEN (    hour_current IS NULL
                         AND hour_previous IS NOT NULL
                         AND hour_next IS NOT NULL)
                   THEN
                       TO_CHAR (
                             (  TO_NUMBER (REPLACE (hour_previous, ',', ''))
                              + TO_NUMBER (REPLACE (hour_next, ',', '')))
                           / 2)
                   WHEN (    hour_current IS NULL
                         AND hour_previous IS NULL
                         AND hour_next IS NOT NULL)
                   THEN
                       REPLACE (hour_next, ',', '')
                   WHEN (    hour_current IS NULL
                         AND hour_previous IS NOT NULL
                         AND hour_next IS NULL)
                   THEN
                       REPLACE (hour_previous, ',', '')
                   ELSE
                       REPLACE (hour_current, ',', '')
               END
          INTO vreturn
          FROM DUAL;

        RETURN vreturn;
    EXCEPTION
        WHEN OTHERS
        THEN
            --Return the current HOUR Value
            RETURN REPLACE (hour_current, ',', '');
    END;
    PROCEDURE get_renewal_data (inmeterlist      IN     t_meterlist,
                                ininputfile      IN     VARCHAR2,
                                indblpss         IN     VARCHAR2,
                                outstatus           OUT VARCHAR2,
                                outdescription      OUT VARCHAR2)
    IS
        vwarningmessage    VARCHAR2 (2000);
        vmeterlist         str_array;  /*Local Array of native type VARCHAR2*/
        vblock             CLOB;
        vdsqlstatus        VARCHAR2 (100);
        vdsqldescription   VARCHAR2 (2000);
        vbatchid           NUMBER;
    BEGIN
        --Convert PL/SQL Array to Type Array
        IF     inmeterlist.COUNT > 0
           AND LENGTH (ininputfile) > 0
           AND LENGTH (indblpss) > 0
        THEN
            vmeterlist := str_array ();

            FOR i IN inmeterlist.FIRST .. inmeterlist.LAST
            LOOP
                vmeterlist.EXTEND (1);
                vmeterlist (i) := inmeterlist (i);
            END LOOP;
        ELSE
            raise_application_error (
                -20001,
                'Please check the input parameter values and try again');
        END IF;


        --Gte The batch Id for the input file
        FOR rec IN (SELECT *
                      FROM (  SELECT uid_batch
                                FROM alps_parser_instance
                               WHERE input_filename = ininputfile
                            ORDER BY uid_batch DESC)
                     WHERE ROWNUM < 2)
        LOOP
            vbatchid := rec.uid_batch;
        END LOOP;

        --Delete Data from ALPS_PARSER_SCA_OUTPUT for the given input file name
        DELETE FROM alps_parser_sca_output
              WHERE inputfile = ininputfile;

        --Insert latest data from LPSS
        --Get Dynamic SQL from Table to fetch the Renewal Data
        vblock := NULL;

        BEGIN
            --Get the Dynamic SQL CLOB from the config table ALPS_PARSER_DYNSQL
            SELECT source
              INTO vblock
              FROM alps_parser_dynsql
             WHERE     active_flag = 'Y'
                   AND category = 'GENERIC_SQL'
                   AND name = 'GET_LPSS_RENEWAL_DATA';
        EXCEPTION
            WHEN OTHERS
            THEN
                DBMS_OUTPUT.put_line (
                    'Could not fetch the Dynamic SQL for GET_LPSS_RENEWAL_DATA');
        END;

        --DBMS_OUTPUT.put_line ('vBlock --> length' || LENGTH (vblock));

        --If one or more Dynamic SQL Block are found, then execute the Dynamic PL/SQL Blocks
        IF LENGTH (vblock) > 0
        THEN
            vdsqlstatus := NULL;
            vdsqldescription := NULL;

            --Replace DB Links for LPSS
            vblock := REPLACE (vblock, '@DBLPSS', '@' || indblpss);

            --dbms_output.put_line(dbms_lob.substr(vBlock,4000,1));
            BEGIN
                EXECUTE IMMEDIATE vblock
                    USING ininputfile,
                          vmeterlist,
                          IN OUT vdsqlstatus,
                          IN OUT vdsqldescription;
            EXCEPTION
                WHEN OTHERS
                THEN
                    vdsqlstatus := 'ERROR';

                    vdsqldescription :=
                        'Error Calling Dynamic SQL:' || SQLERRM;
            END;
        --DBMS_OUTPUT.put_line ('vDSQLStatus:' || vdsqlstatus);
        --DBMS_OUTPUT.put_line ('vDSQLDescription:' || vdsqldescription);
        END IF;

        IF vdsqlstatus <> 'SUCCESS'
        THEN
            raise_application_error (
                -20001,
                   'Error executing Renewal Data SQL:'
                || CHR (13)
                || vdsqldescription);
        END IF;
--64673
/*
        FOR rec IN (SELECT DISTINCT COLUMN_VALUE     ldcaccount
                      FROM TABLE (vmeterlist)
                    MINUS
                    SELECT DISTINCT ldc_account     ldcaccount
                      FROM alps_parser_sca_output
                     WHERE inputfile = ininputfile)
        LOOP
            vwarningmessage := vwarningmessage || rec.ldcaccount || ',';
            alps_parser_pkg.insert_note (
                vbatchid,
                rec.ldcaccount,
                   'Service Point:'
                || rec.ldcaccount
                || ' was not found in the LPSS database',
                'SCALAR',
                'WARNING');
        END LOOP;

        IF LENGTH (vwarningmessage) > 0
        THEN
            vwarningmessage :=
                   'Unable to fetch data from LPSS for the following Service Points.'
                || CHR (13)
                || SUBSTR (vwarningmessage, 1, LENGTH (vwarningmessage) - 1);
        END IF;
*/
        outstatus := 'SUCCESS';
        outdescription := vwarningmessage;

        --Release the Memory
        vmeterlist := str_array ();
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                   'Error in GET_RENEWAL_DATA:'
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace);
            outstatus := 'ERROR';
            outdescription :=
                (   'Error in GET_RENEWAL_DATA:'
                 || SQLERRM
                 || CHR (13)
                 || DBMS_UTILITY.format_error_backtrace);
            --Release the Memory
            vmeterlist := str_array ();
    END get_renewal_data;


    FUNCTION f_validate_required (inmarket      VARCHAR2,
                                  indisco       VARCHAR2,
                                  ininputfile   VARCHAR2,
                                  intablename   VARCHAR2)
        RETURN VARCHAR2
    IS
        vcount     NUMBER := 0;
        vresult    VARCHAR2 (2000);
        sql_stmt   VARCHAR2 (2000);



        TYPE refcurtype IS REF CURSOR;



        rc_val1    refcurtype;
        vdisco     VARCHAR2 (100);
    --Declare variables

    BEGIN
        --Validate Input Variables
        IF    NVL (LENGTH (inmarket), 0) <= 0
           OR NVL (LENGTH (indisco), 0) <= 0
           OR NVL (LENGTH (ininputfile), 0) <= 0
           OR NVL (LENGTH (intablename), 0) <= 0
        THEN
            raise_application_error (
                -20001,
                'Invalid Inputs to function F_VALIDATE_REQUIRED [Market/Disco/InputFile/TableName] - Required column validation');
        END IF;

        --Get the Required columns for the Market/Disco combo
        FOR col
            IN (SELECT *
                  FROM (SELECT a.*, 'ALPS_PARSER_SCA_OUTPUT' tablename
                          FROM (SELECT *
                                  FROM alps_parser_rcols_sca
                                       UNPIVOT INCLUDE NULLS (columnvalue
                                                             FOR columnname
                                                             IN (inputfile,
                                                                market_code,
                                                                disco_code,
                                                                ldc_account,
                                                                address1,
                                                                address2,
                                                                city,
                                                                state_code,
                                                                zip,
                                                                zipplus4,
                                                                county,
                                                                country,
                                                                rate_class,
                                                                rate_subclass,
                                                                zone_code,
                                                                load_profile,
                                                                voltage_class,
                                                                meter_cycle,
                                                                start_time,
                                                                stop_time,
                                                                usage_total,
                                                                demand_total,
                                                                capacity,
                                                                transmission,
                                                                meter_type,
                                                                mhp,
                                                                bus,
                                                                loss_factor,
                                                                strata,
                                                                stationid,
                                                                attr1,
                                                                attr2,
                                                                attr3,
                                                                attr4,
                                                                attr5,
                                                                attr6,
                                                                attr7,
                                                                attr8,
                                                                attr9,
                                                                attr10,
                                                                weather_zone)))
                               a
                         WHERE     columnvalue > 0
                               AND market_validation = inmarket
                               AND disco_validation = indisco
                        UNION
                        SELECT a.*, 'ALPS_PARSER_IDR_RECT_OUTPUT' tablename
                          FROM (SELECT *
                                  FROM alps_parser_rcols_rect
                                       UNPIVOT INCLUDE NULLS (columnvalue
                                                             FOR columnname
                                                             IN (inputfile,
                                                                market_code,
                                                                disco_code,
                                                                ldc_account,
                                                                intervaldate,
                                                                hour0,
                                                                hour1,
                                                                hour2,
                                                                hour3,
                                                                hour4,
                                                                hour5,
                                                                hour6,
                                                                hour7,
                                                                hour8,
                                                                hour9,
                                                                hour10,
                                                                hour11,
                                                                hour12,
                                                                hour13,
                                                                hour14,
                                                                hour15,
                                                                hour16,
                                                                hour17,
                                                                hour18,
                                                                hour19,
                                                                hour20,
                                                                hour21,
                                                                hour22,
                                                                hour23,
                                                                attr1,
                                                                attr2,
                                                                attr3,
                                                                attr4,
                                                                attr5))) a
                         WHERE     columnvalue > 0
                               AND market_validation = inmarket
                               AND disco_validation = indisco
                        UNION
                        SELECT a.*, 'ALPS_PARSER_IDR_STRIP_OUTPUT' tablename
                          FROM (SELECT *
                                  FROM alps_parser_rcols_strip
                                       UNPIVOT INCLUDE NULLS (columnvalue
                                                             FOR columnname
                                                             IN (inputfile,
                                                                market_code,
                                                                disco_code,
                                                                ldc_account,
                                                                time_stamp,
                                                                usage_total,
                                                                attr1,
                                                                attr2,
                                                                attr3,
                                                                attr4,
                                                                attr5))) a
                         WHERE     columnvalue > 0
                               AND market_validation = inmarket
                               AND disco_validation = indisco)
                 WHERE UPPER (tablename) = UPPER (intablename))
        LOOP
            OPEN rc_val1 FOR
                   'select count(*) from ALPS.'
                || col.tablename
                || ' where '
                || col.columnname
                || ' is null and INPUTFILE  = '''
                || ininputfile
                || '''';

            FETCH rc_val1 INTO vcount;



            IF vcount > 0
            THEN
                --Prepare string with all the missing columns that are required before Translation can begin
                vresult :=
                       vresult
                    || '['
                    || intablename
                    || '.'
                    || col.columnname
                    || '] column contains  null value'
                    || CHR (13);
            END IF;

            CLOSE rc_val1;
        END LOOP;



        --Run Market Market/Disco Validation, IFInput market/disco does not mach with the data record's Market/Disco.
        sql_stmt :=
               'SELECT count(*) FROM '
            || intablename
            || '  WHERE INPUTFILE = :inputfile and market_code = :market and DECODE(disco_code,''AEPCPL'',''ERCOTAEP'',''AEPWTU'',''ERCOTAEP'',disco_code) <>:disco';

        IF indisco IN ('AEPCPL', 'AEPWTU') AND inmarket = 'ERCOT'
        THEN
            vdisco := 'ERCOTAEP';
        ELSE
            vdisco := indisco;
        END IF;

        EXECUTE IMMEDIATE sql_stmt
            INTO vcount
            USING ininputfile, inmarket, vdisco;



        --Show Market/Disco Validation
        IF vcount > 0
        THEN
            IF LENGTH (vresult) > 0
            THEN
                vresult :=
                       vresult
                    || 'Market/Disco does not match between input Values:'
                    || inmarket
                    || ';'
                    || indisco
                    || ' and the parsed data from file:'
                    || ininputfile
                    || CHR (13);
            ELSE
                vresult :=
                       'Market/Disco does not match between input Values:'
                    || inmarket
                    || ';'
                    || indisco
                    || ' and the parsed data from file:'
                    || ininputfile;
            END IF;
        END IF;

        --Return Value with the Intial validation result


        IF LENGTH (vresult) > 0
        THEN
            vresult :=
                   'Input Validation Failed for File:'
                || ininputfile
                || CHR (13)
                || vresult;
        END IF;



        RETURN vresult;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (SQLERRM);
            vresult :=
                SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ();
            RETURN 'ERROR:' || vresult;
    END f_validate_required;

    PROCEDURE insert_note (in_uidbatch    IN NUMBER,
                           in_meterid     IN VARCHAR2,
                           in_comments    IN VARCHAR2,
                           in_metertype   IN VARCHAR2,
                           in_notetype    IN VARCHAR2)
    IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO alps_parser_acct_note (uid_batch,
                                           meter_id,
                                           comments,
                                           meter_type,
                                           note_type)
             VALUES (in_uidbatch,
                     in_meterid,
                     in_comments,
                     in_metertype,
                     in_notetype);

        COMMIT;
    END insert_note;



    PROCEDURE update_parse_notes (p_batchid           IN     NUMBER,
                                  p_out_status           OUT VARCHAR2,
                                  p_out_description      OUT VARCHAR2)
    IS
    BEGIN
        FOR cinstance IN (SELECT *
                            FROM alps_parser_instance
                           WHERE uid_batch = p_batchid)
        LOOP
            IF cinstance.status = 'SUCCESS'
            THEN
                --Update any Comments to the ALPS Account Transaction Log as they may exist in ALPS_PARSER_ACCT_NOTE table

                FOR rec
                    IN (SELECT DISTINCT
                               b.uid_alps_account,
                               'HI'                                     meter_type,
                               NVL (a.comments, 'Parsing Completed')    description
                          FROM (SELECT *
                                  FROM alps_parser_acct_note
                                 WHERE uid_batch = cinstance.uid_batch) a,
                               alps_mml_pr_detail  b
                         WHERE     EXISTS
                                       (SELECT 1
                                          FROM alps_mml_pr_header
                                         WHERE     sbl_quote_id =
                                                   b.sbl_quote_id
                                               AND active_flg = 'Y')
                               AND b.idr_status NOT IN
                                       ('NOT_IDR', 'PESETUP_COMPLETE')
                               AND a.meter_type = 'IDR'
                               AND a.meter_id = b.meter_id
                        UNION
                        SELECT DISTINCT
                               b.uid_alps_account,
                               'HU'                                     meter_type,
                               NVL (a.comments, 'Parsing Completed')    description
                          FROM (SELECT *
                                  FROM alps_parser_acct_note
                                 WHERE uid_batch = cinstance.uid_batch) a,
                               alps_mml_pr_detail  b
                         WHERE     EXISTS
                                       (SELECT 1
                                          FROM alps_mml_pr_header
                                         WHERE     sbl_quote_id =
                                                   b.sbl_quote_id
                                               AND active_flg = 'Y')
                               AND b.sca_status NOT IN ('PESETUP_COMPLETE')
                               AND a.meter_type = 'SCALAR'
                               AND (   a.meter_id = b.meter_id
                                    OR a.meter_id =
                                       DECODE (
                                           cinstance.simx_parser_name,
                                           'Renewal Excel', b.ldc_account,
                                           b.meter_id)))
                LOOP
                    INSERT INTO alps_account_transaction (uid_alps_account,
                                                          transact_type,
                                                          status,
                                                          description,
                                                          txn_indicator)
                             VALUES (
                                        rec.uid_alps_account,
                                        'SIMX_PARSING',
                                        DECODE (
                                              INSTR (
                                                  rec.description,
                                                  'was not found in the LPSS database')
                                            + INSTR (
                                                  rec.description,
                                                  'No ROI data found in SUPPLY DB'),
                                            0, 'SUCCESS',
                                            'ERROR'),
                                        SUBSTR (rec.description, 1, 2000),
                                        rec.meter_type);

                    IF (   INSTR (rec.description,
                                  'was not found in the LPSS database') >
                           0
                        OR INSTR (rec.description,
                                  'No ROI data found in SUPPLY DB') >
                           0)
                    THEN
                        UPDATE alps_mml_pr_detail
                           SET idr_status =
                                   DECODE (rec.meter_type,
                                           'HI', 'DATA_ERROR',
                                           idr_status),
                               sca_status =
                                   DECODE (rec.meter_type,
                                           'HU', 'DATA_ERROR',
                                           sca_status)
                         WHERE uid_alps_account = rec.uid_alps_account;
                    END IF;
                END LOOP;
            ELSE
                --Update correspoinding Records as Error
                FOR rec
                    IN (SELECT DISTINCT
                               b.uid_alps_account,
                               'HI'                                         meter_type,
                               NVL (cinstance.comments, 'Error Occured')    description
                          FROM (SELECT *
                                  FROM alps_parser_acct_note
                                 WHERE uid_batch = cinstance.uid_batch) a,
                               alps_mml_pr_detail  b
                         WHERE     EXISTS
                                       (SELECT 1
                                          FROM alps_mml_pr_header
                                         WHERE     sbl_quote_id =
                                                   b.sbl_quote_id
                                               AND active_flg = 'Y')
                               AND b.idr_status NOT IN
                                       ('NOT_IDR', 'PESETUP_COMPLETE')
                               AND a.meter_type = 'IDR'
                               AND a.meter_id = b.meter_id
                        UNION
                        SELECT DISTINCT
                               b.uid_alps_account,
                               'HU'                                         meter_type,
                               NVL (cinstance.comments, 'Error Occured')    description
                          FROM (SELECT *
                                  FROM alps_parser_acct_note
                                 WHERE uid_batch = cinstance.uid_batch) a,
                               alps_mml_pr_detail  b
                         WHERE     EXISTS
                                       (SELECT 1
                                          FROM alps_mml_pr_header
                                         WHERE     sbl_quote_id =
                                                   b.sbl_quote_id
                                               AND active_flg = 'Y')
                               AND b.sca_status NOT IN ('PESETUP_COMPLETE')
                               AND a.meter_type = 'SCALAR'
                               AND a.meter_id = b.meter_id)
                LOOP
                    INSERT INTO alps_account_transaction (uid_alps_account,
                                                          transact_type,
                                                          status,
                                                          description,
                                                          txn_indicator)
                         VALUES (rec.uid_alps_account,
                                 'SIMX_PARSING',
                                 'ERROR',
                                 SUBSTR (rec.description, 1, 2000),
                                 rec.meter_type);



                    UPDATE alps_mml_pr_detail
                       SET idr_status =
                               DECODE (rec.meter_type,
                                       'HI', 'DATA_ERROR',
                                       idr_status),
                           sca_status =
                               DECODE (rec.meter_type,
                                       'HU', 'DATA_ERROR',
                                       sca_status)
                     WHERE uid_alps_account = rec.uid_alps_account;
                END LOOP;
            END IF;
        END LOOP;



        COMMIT;



        p_out_status := 'SUCCESS';
        p_out_description := 'NULL';
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                   'Error Calling UPDATE_PARSE_NOTES : '
                || CHR (13)
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace);
            ROLLBACK;
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error Calling UPDATE_PARSE_NOTES : '
                || CHR (13)
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;
    END update_parse_notes;

    PROCEDURE get_roi_data (inmeterlist      IN     t_meterlist,
                            ininputfile      IN     VARCHAR2,
                            outstatus           OUT VARCHAR2,
                            outdescription      OUT VARCHAR2)
    IS
        vwarningmessage   VARCHAR2 (2000);
        vmeterlist        str_array;   /*Local Array of native type VARCHAR2*/
        vbatchid          NUMBER;
        vstatus           VARCHAR2 (100);
        vdescription      VARCHAR2 (2000);
    BEGIN
        --Convert PL/SQL Array to Type Array
        IF inmeterlist.COUNT > 0 AND LENGTH (ininputfile) > 0
        THEN
            vmeterlist := str_array ();



            FOR i IN inmeterlist.FIRST .. inmeterlist.LAST
            LOOP
                vmeterlist.EXTEND (1);
                vmeterlist (i) := inmeterlist (i);
            END LOOP;
        ELSE
            raise_application_error (
                -20001,
                'Please check the input parameter values and try again');
        END IF;



        --Gte The batch Id for the input file
        FOR rec IN (SELECT *
                      FROM (  SELECT uid_batch
                                FROM alps_parser_instance
                               WHERE input_filename = ininputfile
                            ORDER BY uid_batch DESC)
                     WHERE ROWNUM < 2)
        LOOP
            vbatchid := rec.uid_batch;
        END LOOP;

        --Delete any Account specific WARNING/ERRORs


        DELETE FROM alps_parser_acct_note
              WHERE uid_batch = NVL (vbatchid, -99999);



        --Delete Data from ALPS_PARSER_SCA_OUTPUT for the given input file name
        DELETE FROM alps_parser_idr_rect_output
              WHERE inputfile = ininputfile;

        --Fetch Data from ROI Master in Supply DB
        INSERT INTO alps_parser_idr_rect_output
            SELECT ininputfile,
                   marketcode,
                   discocode,
                   ldcaccount,
                   day,
                   hr01,
                   hr02,
                   hr03,
                   hr04,
                   hr05,
                   hr06,
                   hr07,
                   hr08,
                   hr09,
                   hr10,
                   hr11,
                   hr12,
                   hr13,
                   hr14,
                   hr15,
                   hr16,
                   hr17,
                   hr18,
                   hr19,
                   hr20,
                   hr21,
                   hr22,
                   hr23,
                   hr24,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL
              FROM fe_ohio_idr@tpitron  a,
                   (SELECT DISTINCT COLUMN_VALUE     meter_id
                      FROM TABLE (vmeterlist)) b
             WHERE a.meterid = b.meter_id;


        INSERT INTO alps_parser_idr_rect_output
            SELECT ininputfile,
                   marketcode,
                   discocode,
                   ldcaccount,
                   day,
                   hr01,
                   hr02,
                   hr03,
                   hr04,
                   hr05,
                   hr06,
                   hr07,
                   hr08,
                   hr09,
                   hr10,
                   hr11,
                   hr12,
                   hr13,
                   hr14,
                   hr15,
                   hr16,
                   hr17,
                   hr18,
                   hr19,
                   hr20,
                   hr21,
                   hr22,
                   hr23,
                   hr24,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL
              FROM pn_me_jc_idr@tpitron a,
                   (SELECT DISTINCT COLUMN_VALUE     meter_id
                      FROM TABLE (vmeterlist)) b
             WHERE a.meterid = b.meter_id;

        COMMIT;



        FOR rec
            IN (SELECT DISTINCT COLUMN_VALUE     meter_id
                  FROM TABLE (vmeterlist)
                MINUS
                SELECT DISTINCT
                       market_code || '_' || disco_code || '_' || ldc_account    meter_id
                  FROM alps_parser_idr_rect_output
                 WHERE inputfile = ininputfile)
        LOOP
            vwarningmessage := vwarningmessage || rec.meter_id || ',';
            alps_parser_pkg.insert_note (
                vbatchid,
                rec.meter_id,
                   'Service Point:'
                || rec.meter_id
                || ': No ROI data found in SUPPLY DB',
                'IDR',
                'WARNING');
        END LOOP;

        IF LENGTH (vwarningmessage) > 0
        THEN
            vwarningmessage :=
                   'Unable to fetch data (No ROI data found in SUPPLY DB) from ROI Supply DB for the following Service Points.'
                || CHR (13)
                || SUBSTR (vwarningmessage, 1, LENGTH (vwarningmessage) - 1);
        END IF;



        --Call  IDR Rect Validation and Translation
        vstatus := NULL;
        vdescription := NULL;

        BEGIN
            idrrect_translation (vbatchid,
                                 'ROI Excel',
                                 vstatus,
                                 vdescription);
        EXCEPTION
            WHEN OTHERS
            THEN
                vstatus := 'ERROR';
                vdescription :=
                       'Unable to execute idrrect_translation:'
                    || CHR (13)
                    || SQLERRM;
        END;



        IF vstatus != 'SUCCESS'
        THEN
            raise_application_error (-20001, vdescription);
        END IF;



        --Final Output
        outstatus := 'SUCCESS';
        outdescription := vwarningmessage;



        --Release the Memory
        vmeterlist := str_array ();
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                   'Error in GET_ROI_DATA:'
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace);
            outstatus := 'ERROR';
            outdescription :=
                (   'Error in GET_ROI_DATA:'
                 || SQLERRM
                 || CHR (13)
                 || DBMS_UTILITY.format_error_backtrace);
            --Release the Memory
            vmeterlist := str_array ();
    END get_roi_data;

    PROCEDURE truncate_sca_output (p_final             IN     VARCHAR2,
                                   p_out_status           OUT VARCHAR2,
                                   p_out_description      OUT VARCHAR2)
    IS
        ncount   NUMBER;
    BEGIN
        SELECT COUNT (1)
          INTO ncount
          FROM alps_parser_instance
         WHERE     status = 'IN_PROGRESS'        --Created in the last 2 Hours
               AND created_dt >= SYSDATE + INTERVAL '-120' MINUTE;



        IF ncount = 0
        THEN
            --SCA Final Table
            /*
            DBMS_OUTPUT.
             PUT_LINE (
               'Begin Truncating final arc:'
               || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_sca_final_arc';
           */


            --DBMS_OUTPUT.
            --PUT_LINE (
            --            'Begin inserting data into final arc:'
            --            || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            INSERT INTO alps_parser_sca_final_arc
                SELECT * FROM alps_parser_sca_final;



            --DBMS_OUTPUT.
            --  PUT_LINE (
            --  'Begin truncating sca final:'
            --    || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_sca_final';



            --SCA Output Table
            /*
            DBMS_OUTPUT.
             PUT_LINE (
               'Begin truncating output arc:'
               || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_sca_output_arc';
           */



            --    DBMS_OUTPUT.
            --  PUT_LINE (
            --    'Begin inserting data into output arc:'
            --    || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            INSERT INTO alps_parser_sca_output_arc
                SELECT * FROM alps_parser_sca_output;



            --    DBMS_OUTPUT.
            --     PUT_LINE (
            --     'Begin truncating sca output:'
            --    || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_sca_output';



            --Account Note Tbale
            /*
            DBMS_OUTPUT.
             PUT_LINE (
               'Begin truncating note arc:'
               || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_acct_note_arc';
           */



            --    DBMS_OUTPUT.
            --    PUT_LINE (
            --     'Begin insert into note arc:'
            --    || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            INSERT INTO alps_parser_acct_note_arc
                SELECT * FROM alps_parser_acct_note;



            --    DBMS_OUTPUT.
            --     PUT_LINE (
            --      'Begin truncating note:'
            --     || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_acct_note';



            --Parser Instance
            /*
            DBMS_OUTPUT.
             PUT_LINE (
               'Begin truncating Instance arc:'
               || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_instance_arc';
           */



            --      DBMS_OUTPUT.
            --          PUT_LINE (
            --            'Begin insert into instance arc:'
            --            || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            INSERT INTO alps_parser_instance_arc
                SELECT * FROM alps_parser_instance;



            /*  DBMS_OUTPUT.
               put_line (
                 'Begin truncating instance:'
                 || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
                 */

            EXECUTE IMMEDIATE 'truncate table alps_parser_instance';



            --Parser Log
            /*
            DBMS_OUTPUT.
             PUT_LINE (
               'Begin truncating log arc:'
               || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));

            EXECUTE IMMEDIATE 'truncate table alps_parser_log_arc';
           */



            /*DBMS_OUTPUT.
             put_line (
               'Begin insert into log arc:'
               || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
            */
            INSERT INTO alps_parser_log_arc
                SELECT * FROM alps_parser_log;

            /*
             DBMS_OUTPUT.
              put_line (
                'Begin truncating log:'
                || TO_CHAR (SYSDATE, 'YYYY-MM-DD HH24:MI:SS'));
            */



            EXECUTE IMMEDIATE 'truncate table alps_parser_log';



            EXECUTE IMMEDIATE 'truncate table alps_parser_edi_output';
        ELSE
            /*Raise Error*/
            raise_application_error (
                -20001,
                'One or more records found with IN_PROGRESS STATUS');
        END IF;



        p_out_status := 'SUCCESS';
        p_out_description := NULL;
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            --Outputs
            p_out_status := 'ERROR';
            p_out_description :=
                   'Error encountered in truncate_sca_output:'
                || SQLERRM
                || CHR (13)
                || DBMS_UTILITY.format_error_backtrace;
    END truncate_sca_output;
END alps_parser_pkg;
////
