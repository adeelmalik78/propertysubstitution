CREATE OR REPLACE PACKAGE BODY ${schema.name}.alps_data_pkg
AS
/*
*********************************************************************************************************
Rev 09.29.2023      Cerebro 69960 Move MISO trans tag generation from PR to meter level
Rev 09.15.2023    Cerebro xxxxx Handle Multipe row erros in refresh_idrstatus_from_pe
Rev 09.12.2023    Cerebro 69505 IDR Status not flipping to PE_SETUP_COMPLETE once IDR data received 
Rev 10.12.2023    cerebro 70455 LCE NAME IS BLANK CAUSING AN ERROR
*********************************************************************************************************
*/

    FUNCTION f_update_file_attach (p_orig_file_name   IN     VARCHAR2,
                                   p_file_uid         IN     NUMBER,
                                   p_status           IN     VARCHAR2,
                                   p_description      IN     VARCHAR2,
                                   p_att_file_path    IN     VARCHAR2,
                                   p_out_status          OUT VARCHAR2,
                                   p_out_desc            OUT VARCHAR2,
                                   p_out_file_uid        OUT NUMBER)
        RETURN NUMBER
    AS
        /* 
            Purspose : to manage new file entry into ALPS_FILE_ATTACHMENT and maintain status of existing response files

        1.    If File Name is NOT NULL:
            a.    If FILE not existed , Insert into ALPS_FILE_ATTACHMENT and return associated FILE_UID to calling process.
            b.    If FILE existed:
                i.    File STATUS = PARSE_COMPLETE , the function return FILE_UID= -1  to calling process
                ii.    File STATUS = DETACH_COMPLETE , the function return FILE_UID= -1  to calling process
                iii.    File STATUS = READY_FOR_PARSING , the function return associated FILE_UID   to calling process
                iv.    File STATUS <> (PARSE_COMPLETE, DETACH_COMPLETE, READY_FOR_PARSING ) , Update file STATUS to  READY_FOR_PARSING , return associated  FILE_UID to calling process

        2.    If FILE UID IS NOT NULL :
            a.    If File existed with STATUS = READY_FOR_PARSING , then Update file STATUS , DESCRIPTION.  Return FILE_UID   to calling process
            b.    If File existed with STATUS <> READY_FOR_PARSING , No UPDATE and  return FILE_UID= -1  to calling process
            c.    If FILE_UID is invalid no matching FILE_UID,  function returns FILE_UID = -1  to calling process

            Parameters:
                                p_orig_file_name - Incoming file Name
                                p_file_uid  - incoming file id
                                p_status  - incoming file status
                                p_description  - incoming status description
                                p_att_file_path  - incoming file full path
                                p_out_status - status of the function call returned to calling program(process). Values are SUCCESS, ERROR
                                p_out_description - Description of the function call returned to calling program(process).

            Execution : Called from other program/process
        */
        lv_uid             alps_file_attachment.uid_attachment%TYPE;
        lv_status          alps_file_attachment.status%TYPE;
        lv_file_exist      VARCHAR2 (3);
        lv_cnt             INTEGER := 0;
        invalid_file_uid   EXCEPTION;
    BEGIN
        gv_code_proc := 'F_UPDATE_FILE_ATTACH';

        --
        IF     p_orig_file_name IS NOT NULL
           AND (p_file_uid < 0 OR p_file_uid IS NULL)
        THEN
            --  Since only file name was provided, confirm :
            -- 1.  If file existed then return file_uid to calling process
            --2.  FIle not existed then Insert, set status ; return file_uid to calling process


            gv_code_ln := $$plsql_line;

            BEGIN
                  SELECT uid_attachment, status
                    INTO lv_uid, lv_status
                    FROM (SELECT uid_attachment, status, active_flg
                            FROM alps_file_attachment
                           WHERE (   UPPER (original_file_name) =
                                     UPPER (p_orig_file_name)
                                  OR UPPER (att_file_path) =
                                     UPPER (p_orig_file_name)))
                   WHERE active_flg = 'Y' AND ROWNUM < 2
                ORDER BY uid_attachment DESC;
            EXCEPTION
                WHEN OTHERS
                THEN
                    lv_uid := NULL;
                    lv_status := NULL;
            END;

            DBMS_OUTPUT.put_line ('file lv_uid  ==> ' || lv_uid);

            --
            IF lv_uid IS NULL
            THEN
                --
                IF alps.f_getleapparsertype (NULL,
                                             UPPER (p_orig_file_name),
                                             NULL) <>
                   'NOTHING'
                THEN
                    --
                    DBMS_OUTPUT.put_line ('inserting file');
                    gv_code_ln := $$plsql_line;

                    INSERT INTO alps_file_attachment (att_file_path,
                                                      att_file_type,
                                                      active_flg,
                                                      status,
                                                      original_file_name)
                         VALUES (p_orig_file_name,
                                 'RESPONSE_OTHER',
                                 'Y',
                                 'READY_FOR_PARSING',
                                 p_orig_file_name)
                      RETURNING uid_attachment
                           INTO lv_uid;

                    --
                    p_out_status := 'READY_FOR_PARSING';
                    p_out_desc := NULL;
                    p_out_file_uid := lv_uid;

                    COMMIT;

                    RETURN (lv_uid);
                ELSE
                    p_out_status := 'ERROR';
                    p_out_file_uid := -1;
                    RETURN (-1);
                END IF;
            ELSE
                DBMS_OUTPUT.put_line (' file existed ');
                gv_code_ln := $$plsql_line;
                DBMS_OUTPUT.put_line (
                       ' file existed, file handle ==>  '
                    || lv_uid
                    || ' file status is ==> '
                    || lv_status);

                -- File existed then :
                -- 1. Update file status to "READY_FOR_PARSING" if file not alrady been Parsed (having status = "PARSING_COMPLETE")
                -- 2. Return file_uid to calling process
                IF lv_status = 'READY_FOR_PARSING'
                THEN                 -- file is alreay handled by other thread
                    p_out_status := 'READY_FOR_PARSING';
                    p_out_desc := 'File is handled by another thread.';
                    p_out_file_uid := lv_uid;
                    RETURN (lv_uid);
                END IF;

                IF lv_status <> 'PARSE_COMPLETE'
                THEN
                    IF lv_status <> 'DETACH_COMPLETE'
                    THEN
                        UPDATE alps_file_attachment
                           SET status = 'READY_FOR_PARSING',
                               description = p_description
                         WHERE uid_attachment = lv_uid;

                        DBMS_OUTPUT.put_line (
                               ' file existed, file handle ==>  '
                            || lv_uid
                            || ' set status to ==> READY_FOR_PARSING');
                        --
                        p_out_status := 'READY_FOR_PARSING';
                        p_out_desc := NULL;
                        p_out_file_uid := lv_uid;

                        COMMIT;

                        RETURN (lv_uid);
                    ELSIF lv_status = 'DETACH_COMPLETE'
                    THEN
                        DBMS_OUTPUT.put_line (
                               ' file existed, file handle ==>  '
                            || lv_uid
                            || ' sET status to ==> ERROR');
                        p_out_status := 'ERROR';
                        p_out_desc :=
                               'File is not ready for Parsing. The current status is '
                            || ''''
                            || lv_status
                            || '''';
                        p_out_file_uid := -1;
                        RETURN (-1);
                    END IF;
                ELSIF lv_status = 'PARSE_COMPLETE'
                THEN
                    DBMS_OUTPUT.put_line (
                           ' file existed, file handle ==>  '
                        || lv_uid
                        || ' sET status to ==> READY_FOR_PARSING');
                    p_out_status := 'READY_FOR_PARSING';
                    p_out_desc := NULL;
                    p_out_file_uid := lv_uid;
                    RETURN (lv_uid);
                END IF;
            END IF;
        ELSIF p_file_uid > 0
        THEN
            -- If File_Uid was provided then:
            -- 1. confirm the validity of file_uid
            -- 2. If file id is valid and status is READY_FOR_PARSING then update  its status, description
            BEGIN
                gv_code_ln := $$plsql_line;

                SELECT status                                          --'YES'
                  INTO lv_status
                  FROM alps_file_attachment
                 WHERE uid_attachment = p_file_uid;

                --AND status = 'READY_FOR_PARSING';
                --
                IF lv_status = 'READY_FOR_PARSING'
                THEN
                    gv_code_ln := $$plsql_line;

                    UPDATE alps_file_attachment
                       SET status = p_status, description = p_description
                     WHERE uid_attachment = p_file_uid;

                    --
                    p_out_status := 'SUCCESS';
                    p_out_desc := NULL;
                    p_out_file_uid := p_file_uid;

                    COMMIT;

                    RETURN (p_file_uid);
                ELSE
                    p_out_status := 'ERROR';
                    p_out_desc :=
                           'File is not ready for Parsing. The current status is '
                        || ''''
                        || lv_status
                        || '''';
                    p_out_file_uid := -1;
                    RETURN (-1);
                END IF;
            EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                    RAISE invalid_file_uid;
            END;
        END IF;
    EXCEPTION
        WHEN invalid_file_uid
        THEN
            p_out_status := 'ERROR';
            p_out_desc :=
                   'No such file. File_uid '
                || ''''
                || p_file_uid
                || ''''
                || ' is invalid';
            p_out_file_uid := -1;
            RETURN (-1);
            ROLLBACK;
        WHEN OTHERS
        THEN
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            alps_mml_pkg.process_logging (
                NULL,
                'Procedure: ' || gv_code_proc || ', line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                NULL,
                NULL);
            ROLLBACK;
    END f_update_file_attach;

    FUNCTION get_response_file_uid (p_file_name    IN     VARCHAR2,
                                    p_out_status      OUT VARCHAR2,
                                    p_out_desc        OUT VARCHAR2)
        RETURN VARCHAR2
    AS
        /*
            Purspose : Get the UID for the response file which is ready for parsing. The UID wil be use to link to meters that were parsed.
            Parameters:
                                p_file_name - Name of the response file
                                p_out_status - status of the function call returned to calling program(process). Values are SUCCESS, ERROR
                                p_out_description - Description of the function call returned to calling program(process).

            Execution : Called from other program/process
        */

        lv_return   VARCHAR2 (3);
    BEGIN
        gv_code_proc := 'GET_RESPONSE_FILE_UID';
        gv_source_id := p_file_name;
        gv_source_type := 'UID_ATTACHMENT';
        gv_code_ln := $$plsql_line;

        SELECT MAX (uid_attachment)            --DECODE(COUNT(1),0,'NO','YES')
          INTO lv_return
          FROM alps_file_attachment
         WHERE     UPPER (
                       SUBSTR (att_file_path,
                               INSTR (att_file_path, '\', -1) + 1)) =
                   UPPER (p_file_name)
               AND status = 'READY_FOR_PARSING'
               AND active_flg = 'Y';

        --
        p_out_status := 'SUCCESS';
        p_out_desc := NULL;
        RETURN (lv_return);
    EXCEPTION
        WHEN OTHERS
        THEN
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                NULL,
                gv_source_type);
            RAISE;
    END;

    PROCEDURE insert_excp_log (p_meter_id         IN VARCHAR2,
                               p_status           IN VARCHAR2,
                               p_description      IN VARCHAR2,
                               p_assigned         IN VARCHAR2,
                               p_exception_type   IN VARCHAR2,
                               p_exception_code   IN VARCHAR2)
    AS
        /*
            Purspose : This procedure log all exceptions encountered throughout each stage of ALPS processing. It will log the Exception Type/Codes and link account(meters) to these exceptions.
            Parameters:
                                p_meter_id - Meter Id of an account(meter). Combination of market_disco_ldc
                                p_status - status of the meter
                                p_description - Detailed description of the exception
                                p_assigned -  LA anayst assigned to work on the exception
                                p_exception_type -  Predefined exception type
                                p_exception_code - Predefined exception code

            Execution : Called from other program/process
        */

        lv_uid_exception   NUMBER;
        lv_stage           alps_mml_pr_detail.stage%TYPE;
        lv_market_code     alps_mml_pr_detail.market_code%TYPE;
        lv_disco_code      alps_mml_pr_detail.disco_code%TYPE;
        lv_ldc_account     alps_mml_pr_detail.ldc_account%TYPE;
    BEGIN
        gv_code_proc := 'INSERT_EXCEPTION_LOG';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        --
        lv_market_code :=
            SUBSTR (p_meter_id,
                    1,
                      INSTR (p_meter_id,
                             '_',
                             1,
                             1)
                    - 1);
        lv_disco_code :=
            SUBSTR (p_meter_id,
                      INSTR (p_meter_id,
                             '_',
                             1,
                             1)
                    + 1,
                    (  (  INSTR (p_meter_id,
                                 '_',
                                 1,
                                 2)
                        - 1)
                     - INSTR (p_meter_id,
                              '_',
                              1,
                              1)));
        lv_ldc_account :=
            SUBSTR (p_meter_id,
                      INSTR (p_meter_id,
                             '_',
                             1,
                             2)
                    + 1);

        -- Get current stage of the account
        BEGIN
            gv_code_ln := $$plsql_line;

            SELECT DISTINCT dtl.stage
              INTO lv_stage
              FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
             WHERE     dtl.market_code = lv_market_code
                   AND dtl.disco_code = lv_disco_code
                   AND dtl.ldc_account = lv_ldc_account
                   AND dtl.sbl_quote_id = hdr.sbl_quote_id
                   AND active_flg = 'Y'
                   AND dtl.updated_dt =
                       (SELECT MAX (dtl1.updated_dt)
                          FROM alps_mml_pr_detail dtl1
                         WHERE     dtl.market_code = dtl1.market_code
                               AND dtl.disco_code = dtl1.disco_code
                               AND dtl.ldc_account = dtl1.ldc_account
                               AND dtl.sbl_quote_id = dtl1.sbl_quote_id);
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                lv_stage := NULL;
        END;

        -- Check to see if the combination of exception type/code already existed
        BEGIN
            gv_code_ln := $$plsql_line;

            SELECT uid_exception
              INTO lv_uid_exception
              FROM alps_exception_log_header
             WHERE     exception_type = p_exception_type
                   AND exception_code = p_exception_code
                   AND active_flg = 'Y';
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                -- combination of exception type/code not founded, Insert the NEW exception
                BEGIN
                    gv_code_ln := $$plsql_line;

                    INSERT INTO alps_exception_log_header (exception_type,
                                                           exception_code,
                                                           active_flg,
                                                           description,
                                                           assigned)
                         VALUES (p_exception_type,
                                 p_exception_code,
                                 'Y',
                                 p_description,
                                 p_assigned)
                      RETURNING uid_exception
                           INTO lv_uid_exception;
                END;
        END;

        -- link accounts to the exception
        IF lv_uid_exception IS NOT NULL
        THEN
            gv_code_ln := $$plsql_line;

            INSERT INTO alps_exception_log_detail (uid_exception,
                                                   market_code,
                                                   disco_code,
                                                   ldc_account,
                                                   status,
                                                   description,
                                                   stage)
                 VALUES (lv_uid_exception,
                         lv_market_code,
                         lv_disco_code,
                         lv_ldc_account,
                         p_status,
                         p_description,
                         lv_stage);
        END IF;
    --
    --p_out_status := 'SUCCESS';
    --COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- p_out_status := 'ERROR';
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                NULL,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END insert_excp_log;

    PROCEDURE insert_exception_log (p_meter_id         IN     VARCHAR2,
                                    p_status           IN     VARCHAR2,
                                    p_description      IN     VARCHAR2,
                                    p_assigned         IN     VARCHAR2,
                                    p_exception_type   IN     VARCHAR2,
                                    p_exception_code   IN     VARCHAR2,
                                    p_out_status          OUT VARCHAR2)
    AS
        /*
            Purspose : This procedure log all exceptions encountered throughout each stage of ALPS processing. It will log the Exception Type/Codes and link account(meters) to these exceptions.
            Parameters:
                                p_meter_id - Meter Id of an account(meter). Combination of market_disco_ldc
                                p_status - status of the meter
                                p_description - Detailed description of the exception
                                p_assigned -  LA anayst assigned to work on the exception
                                p_exception_type -  Predefined exception type
                                p_exception_code - Predefined exception code
                                p_out_status  - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR

            Execution : Called from other program/process
        */
        lv_uid_exception   NUMBER;
        lv_stage           alps_mml_pr_detail.stage%TYPE;
        lv_market_code     alps_mml_pr_detail.market_code%TYPE;
        lv_disco_code      alps_mml_pr_detail.disco_code%TYPE;
        lv_ldc_account     alps_mml_pr_detail.ldc_account%TYPE;
    BEGIN
        gv_code_proc := 'INSERT_EXCEPTION_LOG';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        --
        lv_market_code :=
            SUBSTR (p_meter_id,
                    1,
                      INSTR (p_meter_id,
                             '_',
                             1,
                             1)
                    - 1);
        lv_disco_code :=
            SUBSTR (p_meter_id,
                      INSTR (p_meter_id,
                             '_',
                             1,
                             1)
                    + 1,
                    (  (  INSTR (p_meter_id,
                                 '_',
                                 1,
                                 2)
                        - 1)
                     - INSTR (p_meter_id,
                              '_',
                              1,
                              1)));
        lv_ldc_account :=
            SUBSTR (p_meter_id,
                      INSTR (p_meter_id,
                             '_',
                             1,
                             2)
                    + 1);

        -- Get current stage of the account
        BEGIN
            gv_code_ln := $$plsql_line;

            SELECT DISTINCT dtl.stage
              INTO lv_stage
              FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
             WHERE     dtl.market_code = lv_market_code
                   AND dtl.disco_code = lv_disco_code
                   AND dtl.ldc_account = lv_ldc_account
                   AND dtl.sbl_quote_id = hdr.sbl_quote_id
                   AND active_flg = 'Y'
                   AND dtl.updated_dt =
                       (SELECT MAX (dtl1.updated_dt)
                          FROM alps_mml_pr_detail dtl1
                         WHERE     dtl.market_code = dtl1.market_code
                               AND dtl.disco_code = dtl1.disco_code
                               AND dtl.ldc_account = dtl1.ldc_account
                               AND dtl.sbl_quote_id = dtl1.sbl_quote_id);
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                lv_stage := NULL;
        END;

        -- Check to see if the combination of exception type/code already existed
        BEGIN
            gv_code_ln := $$plsql_line;

            SELECT uid_exception
              INTO lv_uid_exception
              FROM alps_exception_log_header
             WHERE     exception_type = p_exception_type
                   AND exception_code = p_exception_code
                   AND active_flg = 'Y';
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                -- combination of exception type/code not founded, Insert the NEW exception
                BEGIN
                    gv_code_ln := $$plsql_line;

                    INSERT INTO alps_exception_log_header (exception_type,
                                                           exception_code,
                                                           active_flg,
                                                           description,
                                                           assigned)
                         VALUES (p_exception_type,
                                 p_exception_code,
                                 'Y',
                                 p_description,
                                 p_assigned)
                      RETURNING uid_exception
                           INTO lv_uid_exception;
                END;
        END;

        -- link account to the exception
        IF lv_uid_exception IS NOT NULL
        THEN
            gv_code_ln := $$plsql_line;

            INSERT INTO alps_exception_log_detail (uid_exception,
                                                   market_code,
                                                   disco_code,
                                                   ldc_account,
                                                   status,
                                                   description,
                                                   stage)
                 VALUES (lv_uid_exception,
                         lv_market_code,
                         lv_disco_code,
                         lv_ldc_account,
                         p_status,
                         p_description,
                         lv_stage);
        END IF;

        --
        p_out_status := 'SUCCESS';
    --COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            p_out_status := 'ERROR';
            --ALPS_MML_PKG.PROCESS_LOGGING(gv_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),NULL,gv_source_type);
            ROLLBACK;
            RAISE;
    END insert_exception_log;

    PROCEDURE update_acct_attr (p_uid_account   IN NUMBER,
                                p_address1      IN VARCHAR2,
                                p_address2      IN VARCHAR2,
                                p_city          IN VARCHAR2,
                                p_state         IN VARCHAR2,
                                p_zip           IN VARCHAR2,
                                p_zipplus4      IN VARCHAR2,
                                p_county        IN VARCHAR2,
                                p_country       IN VARCHAR2)
    AS
    /*
        Purspose : This procedure track history of changes to an account Premise attributes.
                        Every change to any premises attributes are logged into ALPS_ACCOUNT_ADDRESS
        Parameters:
                            p_uid_account - unique id of an account
                            p_address1 - Main address
                            p_address2 - Main address
                            p_city -  city name
                            p_state -  state name
                            p_zip - zip code
                            p_zipplus4  - zip code +
                            p_county -  county name
                            p_country - country name

        Execution : This procedure executed  via table (ALPS_ACCOUNT) trigger(ALPS_ACCOUNT_BIU) .
    */

    BEGIN
        gv_code_proc := 'UPDATE_ACCT_ATTR';
        gv_source_id := p_uid_account;
        gv_source_type := 'UID_ACCOUNT';
        --
        gv_code_ln := $$plsql_line;

        INSERT INTO alps_account_address (uid_account,
                                          address1,
                                          address2,
                                          city,
                                          state,
                                          zip,
                                          county,
                                          country,
                                          zipplus4)
             VALUES (p_uid_account,
                     p_address1,
                     p_address2,
                     p_city,
                     p_state,
                     p_zip,
                     p_county,
                     p_country,
                     p_zipplus4);
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                NULL,
                gv_source_type);
            RAISE;
    END update_acct_attr;

    PROCEDURE sync_pe_acct_ovrd (p_uid_account   IN NUMBER,
                                 p_ovrd_code     IN VARCHAR2,
                                 p_val           IN FLOAT,
                                 p_strval        IN VARCHAR2,
                                 p_start_tm      IN DATE,
                                 p_stop_tm       IN DATE,
                                 p_operation     IN VARCHAR2)
    AS
    /*
           Purpose : This procedure maintain relationship of account Overide Codes in PE and ALPS

           Parameters:
                               p_uid_account - unique id of an account in PE
                               p_ovrd_code - Valid Override Codes
                               p_val -  Overirde numeric values
                               p_strval -  Overirde string values
                               p_start_tm -  Starting date
                               p_stop_tm - Stop date
                               p_operation  -  Mode of operation on specific override code. Values are D(DELETE), U(UPDATE), I(INSERT)

           Execution : This procedure executed  via table (ALPS_ACCOUNT_OVERRIDES) trigger(ALPS_ACCT_OVRD_BIUD) .
       */
    BEGIN
        gv_code_proc := 'SYNC_PE_ACCT_OVRD';
        gv_source_id := p_uid_account;
        gv_source_type := 'UID_ACCOUNT';
        gv_stage := 'PE_SETUP';

        -- perform the setup based on the record operation(I - INSERT, D - DELETE, U - UPDATE)
        IF p_operation = 'D'
        THEN
            gv_code_ln := $$plsql_line;

            DELETE FROM
                acctoverridehist@TPpe
                  WHERE     uidaccount =
                            (SELECT uidaccount
                               FROM account@TPpe a, alps_account b
                              WHERE     b.uid_account = p_uid_account
                                    AND a.accountid =
                                        NVL (
                                            b.meter_id,
                                               b.market_code
                                            || '_'
                                            || b.disco_code
                                            || '_'
                                            || b.ldc_account))
                        AND overridecode = p_ovrd_code;
        ELSE
            gv_code_ln := $$plsql_line;

            UPDATE acctoverridehist@TPpe
               SET starttime = TRUNC (p_start_tm),
                   stoptime = p_stop_tm,
                   val = p_val,
                   strval = p_strval
             WHERE     uidaccount =
                       (SELECT uidaccount
                          FROM account@TPpe a, alps_account b
                         WHERE     b.uid_account = p_uid_account
                               AND a.accountid =
                                   NVL (
                                       b.meter_id,
                                          b.market_code
                                       || '_'
                                       || b.disco_code
                                       || '_'
                                       || b.ldc_account))
                   AND overridecode = p_ovrd_code;

            --
            IF SQL%ROWCOUNT = 0
            THEN
                gv_code_ln := $$plsql_line;

                INSERT INTO acctoverridehist@TPpe (uidaccount,
                                                   overridecode,
                                                   starttime,
                                                   stoptime,
                                                   val,
                                                   strval)
                         VALUES (
                                    (SELECT uidaccount
                                       FROM account@TPpe a, alps_account b
                                      WHERE     b.uid_account = p_uid_account
                                            AND a.accountid =
                                                NVL (
                                                    b.meter_id,
                                                       b.market_code
                                                    || '_'
                                                    || b.disco_code
                                                    || '_'
                                                    || b.ldc_account)),
                                    p_ovrd_code,
                                    TRUNC (p_start_tm),
                                    p_stop_tm,
                                    p_val,
                                    p_strval);
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            RAISE;
    END sync_pe_acct_ovrd;

   PROCEDURE update_pr_status (p_sbl_quote_id   IN VARCHAR2,
                                p_stage          IN VARCHAR2)
    AS
        /*
               Purpose : This procedure update overall PR status based on the current overall status of its underlying accounts(meters).
                              Once this procedure verified that the underlying accounts have a same overall status, then it will update the PR status to indicate the accounts are ready for next stage in ALPS processing.

               Parameters:
                                   p_sbl_quote_id - Siebel Quote_id of PR
                                   p_stage - Current ALPS stage. Values are VALIDATION, GATHERING, PE_SETUP, SIEBEL_SETUP, OFFER_SETUP

               Execution : Called from other program/process
           */
        lv_status             alps_mml_pr_detail.status%TYPE;
        lv_status_cnt         INTEGER := 0;
        lv_total_cnt          INTEGER := 0;   
        lv_vee_cnt            INTEGER := 0;   
        lv_ch3_cnt            INTEGER := 0;
        lv_transtag_cnt       INTEGER := 0;
        lv_veeovrd_cnt        INTEGER := 0;                      
        lv_market_code        alps_mml_pr_detail.market_code%TYPE;
        lv_disco_code         alps_mml_pr_detail.disco_code%TYPE;
        lv_ldc_account        alps_mml_pr_detail.ldc_account%TYPE;
        --
        lv_ba_cnt             INTEGER := 0;
        lv_ba_account         alps_mml_pr_detail.ldc_account%TYPE;
        lv_uid_ba_account     alps_mml_pr_detail.uid_alps_account%TYPE;
        lv_ba_sa_cnt          INTEGER := 0;
        lv_basa_acct          VARCHAR2 (3);
        lv_basa_acct_status   alps_mml_pr_detail.status%TYPE;
        lv_ba_cnt_status      alps_mml_pr_detail.status%TYPE;
        vOutStatus            VARCHAR2 (10);
        vOutDesc              VARCHAR2 (1000);
        lv_market             alps_mml_pr_detail.market_code%TYPE;
    BEGIN
        gv_code_proc := 'UPDATE_PR_STATUS';
            DBMS_OUTPUT.put_line ('processing Quote id  : ' || p_sbl_quote_id);
        
        --get the market
        SELECT DISTINCT market_code
        INTO lv_market        
        FROM alps_mml_pr_detail basa, alps_mml_pr_header hdr
        WHERE     basa.sbl_quote_id = hdr.sbl_quote_id
        AND hdr.sbl_quote_id = p_sbl_quote_id
        AND active_flg = 'Y'
        AND hdr.status = 'PENDING';
        gv_code_proc := 'UPDATE_PR_STATUS';
            DBMS_OUTPUT.put_line ('Market: ' || lv_market);        
        
        --BA_SA processing -  remove the BA where underlying BASA has been received.
        BEGIN
            FOR rec
                IN (SELECT ldc_account, hdr.sbl_quote_id
                      FROM alps_mml_pr_detail basa, alps_mml_pr_header hdr
                     WHERE     basa.sbl_quote_id = hdr.sbl_quote_id
                           AND hdr.sbl_quote_id = p_sbl_quote_id
                           AND active_flg = 'Y'
                           AND hdr.status = 'PENDING'
                           AND basa.disco_code IN ('WMECO',
                                                   'CLP',
                                                   'PSNH',
                                                   'AMERENCIPS',
                                                   'AMERENCILCO',
                                                   'AMERENIP')
                           AND INSTR (ldc_account, '_') > 0)
            LOOP
                -- Remove Original BA from ALPS tables
                DELETE FROM
                    alps_mml_pr_detail ba
                      WHERE     ba.sbl_quote_id = rec.sbl_quote_id
                            AND INSTR (ba.ldc_account, '_') = 0
                            AND ba.ldc_account =
                                SUBSTR (rec.ldc_account,
                                        1,
                                        INSTR (rec.ldc_account, '_') - 1);

                -- Remove Original BA from EXPRESS tables
                DELETE FROM
                    EXP_QUOTE_ACCT ba
                      WHERE     ba.EXP_quote_id = rec.sbl_quote_id
                            AND INSTR (ba.ldc_account, '_') = 0
                            AND ba.ldc_account =
                                SUBSTR (rec.ldc_account,
                                        1,
                                        INSTR (rec.ldc_account, '_') - 1);
            END LOOP;

            DBMS_OUTPUT.put_line ('BA rows deleted : ' || SQL%ROWCOUNT);
        EXCEPTION
            WHEN OTHERS
            THEN
                NULL;
                DBMS_OUTPUT.put_line ('Error removing BA :' || SQLERRM);
        END;

        -- For a given PR and stage, check to see if all underlying accounts having a same status
        gv_code_ln := $$plsql_line;

        SELECT COUNT (DISTINCT dtl.status), 
               COUNT(1) 
          INTO lv_status_cnt, 
               lv_total_cnt
          FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
         WHERE     hdr.sbl_quote_id = p_sbl_quote_id
               AND hdr.sbl_quote_id = dtl.sbl_quote_id
               AND dtl.status NOT IN
                       (SELECT DISTINCT sca_status
                          FROM alps_gaah_reject_lookup
                         WHERE     sca_status IS NOT NULL
                               AND sca_status <> 'ACCEPT')   -- SR-1-352875356
               AND hdr.active_flg = 'Y'
               AND hdr.status = 'PENDING';

        DBMS_OUTPUT.PUT_LINE (
            'Distinct STATUS count: ' || lv_status_cnt|| ', Total Count :'||lv_total_cnt);

        -- If underlying accounts having same overall status, then update PR status        
        IF lv_status_cnt = 1
        THEN          
          --Confirm overall STATUS is in VEE_COMPLETE
          SELECT COUNT(DISTINCT dtl.status)
          INTO lv_vee_cnt
          FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
          WHERE hdr.sbl_quote_id = p_sbl_quote_id
               AND hdr.sbl_quote_id = dtl.sbl_quote_id
               AND dtl.status = 'VEE_COMPLETE'
               AND hdr.active_flg = 'Y'
               AND hdr.status = 'PENDING';    
                                           
        DBMS_OUTPUT.PUT_LINE (
            'Distinct VEE_COMPLETE count: ' || lv_vee_cnt);       
                  
          -- Confirm VEE completed tag.
          SELECT count(1)
          INTO lv_veeovrd_cnt   
          FROM
          (                                   
            select b.accountid,
                   max(a.lstime) maxlstime
            from acctoverridehist@TPpe a, 
                 account@TPpe b
            where a.uidaccount = b.uidaccount
            and a.overridecode  = 'VEE_COMPLETE_OVRD'
            and b.accountid  in (select meter_id 
                               FROM alps_mml_pr_detail a, 
                                    alps_mml_pr_header b
                               WHERE a.sbl_quote_id = p_sbl_quote_id
                               AND a.sbl_quote_id =  b.sbl_quote_id 
                               AND active_flg = 'Y'
                               AND b.status = 'PENDING'
                             )   
            GROUP BY b.accountid                                        
            ) a
            where maxlstime >= (select last_refresh_dt - 
                                       NVL((SELECT lookup_num_value1 
                                            FROM ALPS_MML_LOOKUP 
                                            WHERE lookup_group = 'CUSTOM_VAR' 
                                            AND lookup_code = 'NUM_DAYS'),7)
                                FROM alps_account
                                WHERE meter_id  = a.accountid);
        DBMS_OUTPUT.PUT_LINE (
            'VEE OVRD  count: ' || lv_veeovrd_cnt);                               

          -- Confirm MISO tran tag.
          IF lv_market = 'MISO' 
          THEN
              SELECT count(1)
              INTO lv_transtag_cnt   
              FROM
              (                                   
                select b.accountid,
                       max(a.lstime) maxlstime
                from acctoverridehist@TPpe a, 
                     account@TPpe b
                where a.uidaccount = b.uidaccount
                and a.overridecode  = 'TRANSMISSION_TAG_OVRD'
                and b.accountid  in (select meter_id 
                                   FROM alps_mml_pr_detail a, 
                                        alps_mml_pr_header b
                                   WHERE a.sbl_quote_id = p_sbl_quote_id
                                   AND a.sbl_quote_id =  b.sbl_quote_id 
                                   AND active_flg = 'Y'
                                   AND b.status = 'PENDING'
                                 )  
                GROUP BY b.accountid                                         
                ) a
                where maxlstime >= (select last_refresh_dt - 
                                           NVL((SELECT lookup_num_value1 
                                                FROM ALPS_MML_LOOKUP 
                                                WHERE lookup_group = 'CUSTOM_VAR' 
                                                AND lookup_code = 'NUM_DAYS'),7)
                                    FROM alps_account
                                    WHERE meter_id  = a.accountid);           
               
            DBMS_OUTPUT.PUT_LINE (
                'TRANS tag count: ' || lv_transtag_cnt);    
            
           END IF;
                      
           -- Set to SETUP_READY if VEE/TRANS/CH3 count equal PR counts
           IF (lv_transtag_cnt = lv_total_cnt AND 
              lv_market = 'MISO' AND 
              lv_veeovrd_cnt = lv_total_cnt AND
              lv_vee_cnt = 1) 
              OR 
              (lv_market <> 'MISO' AND 
              lv_veeovrd_cnt = lv_total_cnt AND
              lv_vee_cnt = 1) 
                            
           THEN
            gv_code_ln := $$plsql_line;
                        DBMS_OUTPUT.PUT_LINE (
                'Ready to update to SETUP_READY'); 
            UPDATE alps_mml_pr_header hdr
               SET status = 'SETUP_READY',
                   last_siebel_setup_dt = NULL,
                   last_offer_summary_dt = NULL,
                   description = NULL
             WHERE hdr.sbl_quote_id = p_sbl_quote_id;
           END IF;
           
        ELSE
            gv_code_ln := $$plsql_line;
                                    DBMS_OUTPUT.PUT_LINE (
                'Stay at PENDING status'); 
            UPDATE alps_mml_pr_header hdr
               SET status = 'PENDING',
                   last_siebel_setup_dt = NULL,
                   last_offer_summary_dt = NULL,
                   description = NULL
             WHERE hdr.sbl_quote_id = p_sbl_quote_id;
        
        END IF;
        
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            RAISE;
    END update_pr_status;
    
    PROCEDURE cap_trans_adjustment (p_market_code       IN VARCHAR2,
                                    p_disco_code        IN VARCHAR2,
                                    p_uid_account_raw   IN NUMBER,
                                    p_uid_account       IN NUMBER)
    --p_arr_attr_startstop     IN        tab_attr_startstop        )
    AS
        /*
           Purpose :   Adjust existing CAP-TRANS START/STOP time to accomodate the incoming CAP-TRANS for a particular meter_id

           Parameter :
                              p_new_uid_acct_raw -  Primary key(uid_account_raw ) from ALPS_ACCOUNT_RAW
                             p_meter_id - Meter id
                             p_out_status  - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                             p_out_description - Description of the function call returned to calling program(process).

           Execution :  This procedure executed  via table ALPS_ACCOUNT_RAW trigger (ALPS_ACCOUNT_RAW_AIU) .
       */
        lv_new_start_time   alps_acct_cap_trans_raw.start_time%TYPE;
        lv_new_stop_time    alps_acct_cap_trans_raw.stop_time%TYPE;
        lv_old_start_time   alps_account_attr_raw.start_time%TYPE;
        lv_old_stop_time    alps_account_attr_raw.stop_time%TYPE;
        lv_uid_acct_raw     alps_account_raw.uid_account_raw%TYPE;
        lv_total_qty        alps_acct_cap_trans_raw.qty%TYPE;
        lv_value1           alps_account_attr_raw.value1%TYPE;
        lv_value2           alps_account_attr_raw.value2%TYPE;
        lv_uid_attr         alps_account_attr_raw.uid_attr%TYPE;
    BEGIN
        gv_code_proc := 'CAP_TRANS_ADJUSTMENT';
        DBMS_OUTPUT.put_line ('p_market_code ==> ' || p_market_code);
        DBMS_OUTPUT.put_line ('p_uid_account_raw ==> ' || p_uid_account_raw);
        DBMS_OUTPUT.put_line ('p_uid_account ==> ' || p_uid_account);

        FOR c_data IN (SELECT DISTINCT attr_code
                         FROM alps_account_attr_raw
                        WHERE uid_account_raw = p_uid_account_raw)
        LOOP
            DBMS_OUTPUT.put_line (
                   'c_data.attr_code ==> '
                || c_data.attr_code
                || ' , p_market_code ==> '
                || p_market_code);

            IF    (    c_data.attr_code = 'TRANSMISSION_TAG'
                   AND p_market_code = 'PJM')
               OR ((    c_data.attr_code = 'CAPACITY_TAG'
                    AND p_market_code IN ('PJM', 'NEPOOL', 'NYISO')))
            THEN
                -- For incoming or new CAP-TRANS tag, get MAX MIN start/stop date
                SELECT MIN (start_time),
                       MAX (
                           NVL (stop_time,
                                TO_DATE ('1/1/2099', 'MM/DD/YYYY')))
                  INTO lv_new_start_time, lv_new_stop_time
                  FROM alps_account_attr_raw
                 WHERE     uid_account_raw = p_uid_account_raw
                       AND attr_code = c_data.attr_code;

                DBMS_OUTPUT.put_line (
                       'NEW:  lv_new_start_time ==> '
                    || lv_new_start_time
                    || ',  lv_new_stop_time ==> '
                    || lv_new_stop_time);

                -- Get the existing book end CAP-TRANS tags for the same meter
                SELECT TRUNC (MIN (a.start_time)), TRUNC (MAX (a.stop_time))
                  INTO lv_old_start_time, lv_old_stop_time
                  FROM alps_account_attributes a
                 WHERE     a.uid_account = p_uid_account
                       AND a.attr_code = c_data.attr_code;

                DBMS_OUTPUT.put_line (
                       'EXISTING:  lv_old_start_time ==> '
                    || lv_old_start_time
                    || ',  lv_old_stop_time ==> '
                    || lv_old_stop_time);

                IF (    lv_new_start_time < lv_old_start_time
                    AND lv_new_stop_time <= lv_old_stop_time)
                THEN
                    DBMS_OUTPUT.put_line (
                        '    lv_new_stop_time > lv_old_start_time and   lv_new_stop_time < lv_old_stop_time  ');

                    -- Adjust the current tag set start_time = new  stop_time-1
                    UPDATE alps_account_attributes
                       SET start_time = lv_new_stop_time + 1
                     WHERE     uid_account = p_uid_account
                           AND attr_code = c_data.attr_code;

                    --Insert new tags

                    INSERT INTO alps_account_attributes (uid_account,
                                                         attr_code,
                                                         attr_type,
                                                         start_time,
                                                         stop_time,
                                                         value1,
                                                         value2,
                                                         str_value1,
                                                         str_value2)
                        SELECT p_uid_account,
                               attr_code,
                               'Planning Period',
                               start_time,
                               stop_time,
                               value1,
                               value2,
                               str_value1,
                               str_value2
                          FROM alps_account_attr_raw
                         WHERE     uid_account_raw = p_uid_account_raw
                               AND attr_code = c_data.attr_code;
                ELSIF (    lv_new_start_time > lv_old_start_time
                       AND lv_new_stop_time > lv_old_stop_time)
                THEN
                    DBMS_OUTPUT.put_line (
                        '    lv_new_stop_time > lv_old_start_time and   lv_new_stop_time < lv_old_stop_time  ');

                    -- Adjust the current tag set start_time = new  stop_time-1
                    UPDATE alps_account_attributes
                       SET stop_time = lv_new_start_time - 1
                     WHERE     uid_account = p_uid_account
                           AND attr_code = c_data.attr_code;

                    --Insert new tags
                    INSERT INTO alps_account_attributes (uid_account,
                                                         attr_code,
                                                         attr_type,
                                                         start_time,
                                                         stop_time,
                                                         value1,
                                                         value2,
                                                         str_value1,
                                                         str_value2)
                        SELECT p_uid_account,
                               attr_code,
                               'Planning Period',
                               start_time,
                               stop_time,
                               value1,
                               value2,
                               str_value1,
                               str_value2
                          FROM alps_account_attr_raw
                         WHERE     uid_account_raw = p_uid_account_raw
                               AND attr_code = c_data.attr_code;
                ELSIF    (    lv_new_start_time <= lv_old_start_time
                          AND lv_new_stop_time >= lv_old_stop_time)
                      OR (    lv_new_start_time > lv_old_start_time
                          AND lv_new_stop_time < lv_old_stop_time)
                THEN
                    DBMS_OUTPUT.put_line (
                        'new start_time <= old start_time and  new stop_time >= old stop_time');

                    -- Remove existing
                    DELETE FROM
                        alps_account_attributes
                          WHERE     uid_account = p_uid_account
                                AND attr_code = c_data.attr_code;

                    -- Insert new CAP-TRANS attributes
                    INSERT INTO alps_account_attributes (uid_account,
                                                         attr_code,
                                                         attr_type,
                                                         start_time,
                                                         stop_time,
                                                         value1,
                                                         value2,
                                                         str_value1,
                                                         str_value2)
                        SELECT p_uid_account,
                               attr_code,
                               'Planning Period',
                               start_time,
                               stop_time,
                               value1,
                               value2,
                               str_value1,
                               str_value2
                          FROM alps_account_attr_raw
                         WHERE     uid_account_raw = p_uid_account_raw
                               AND attr_code = c_data.attr_code;
                ELSE
                    DBMS_OUTPUT.put_line ('OTHER ');

                    -- Insert new CAP-TRANS attributes
                    INSERT INTO alps_account_attributes (uid_account,
                                                         attr_code,
                                                         attr_type,
                                                         start_time,
                                                         stop_time,
                                                         value1,
                                                         value2,
                                                         str_value1,
                                                         str_value2)
                        SELECT p_uid_account,
                               attr_code,
                               'Planning Period',
                               start_time,
                               stop_time,
                               value1,
                               value2,
                               str_value1,
                               str_value2
                          FROM alps_account_attr_raw
                         WHERE     uid_account_raw = p_uid_account_raw
                               AND attr_code = c_data.attr_code;
                END IF;
            END IF;
        END LOOP;
    EXCEPTION
        WHEN OTHERS
        THEN
            RAISE;
    END cap_trans_adjustment;

    /*PROCEDURE  CAP_TRANS_ADJUSTMENT ( p_market_code IN VARCHAR2,
                                                                  p_disco_code  IN VARCHAR2,
                                                                p_uid_account_raw  IN  NUMBER,
                                                                p_uid_account  IN NUMBER,
                                                                p_arr_attr_startstop     IN        tab_attr_startstop        )
     AS */
    /*
       Purpose :   Adjust existing CAP-TRANS START/STOP time to accomodate the incoming CAP-TRANS for a particular meter_id

       Parameter :
                          p_new_uid_acct_raw -  Primary key(uid_account_raw ) from ALPS_ACCOUNT_RAW
                         p_meter_id - Meter id
                         p_out_status  - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                         p_out_description - Description of the function call returned to calling program(process).

       Execution :  This procedure executed  via table ALPS_ACCOUNT_RAW trigger (ALPS_ACCOUNT_RAW_AIU) .
   */
    /*
        lv_new_start_time    ALPS_ACCT_CAP_TRANS_RAW.START_TIME%TYPE;
       lv_new_stop_time    ALPS_ACCT_CAP_TRANS_RAW.STOP_TIME%TYPE;
       lv_start_time           ALPS_ACCOUNT_ATTR_RAW.START_TIME%TYPE;
       lv_stop_time           ALPS_ACCOUNT_ATTR_RAW.STOP_TIME%TYPE;
       lv_uid_acct_raw      ALPS_ACCOUNT_RAW.UID_ACCOUNT_RAW%TYPE;
       lv_total_qty             ALPS_ACCT_CAP_TRANS_RAW.QTY%TYPE;
       lv_value1                ALPS_ACCOUNT_ATTR_RAW.VALUE1%TYPE;
       lv_value2                ALPS_ACCOUNT_ATTR_RAW.VALUE2%TYPE;
       lv_uid_attr              ALPS_ACCOUNT_ATTR_RAW.UID_ATTR%TYPE;
    BEGIN
        gv_code_proc          := 'CAP_TRANS_ADJUSTMENT';
        IF p_arr_attr_startstop.COUNT> 0 THEN
            FOR jj IN p_arr_attr_startstop.FIRST..p_arr_attr_startstop.LAST
            LOOP
          IF ( p_arr_attr_startstop(jj).attr_code = 'TRANSMISSION_TAG'  AND p_market_code = 'PJM' )
                OR
           (  ( p_arr_attr_startstop(jj).attr_code = 'CAPACITY_TAG' AND
                        p_market_code IN ('PJM','NEPOOL', 'NYISO')  )  )
                 THEN
            -- For incoming or new CAP-TRANS tag, get MAX MIN start/stop date
            SELECT MIN(start_time), MAX(NVL(stop_time,TO_DATE('1/1/2099','MM/DD/YYYY')))
            INTO lv_new_start_time, lv_new_stop_time
            FROM ALPS_ACCOUNT_ATTR_RAW
            WHERE uid_attr = p_arr_attr_startstop(jj).uid_attr;

           dbms_Output.put_line('NEW:  lv_new_start_time ==> '||lv_new_start_time ||',  lv_new_stop_time ==> '||lv_new_stop_time);

               -- Get the book end CAP-TRANS tags for the same meter
                SELECT MIN(a.start_time), MAX(a.stop_time)
                INTO lv_start_time, lv_stop_time
                FROM ALPS_ACCOUNT_ATTRIBUTES  a
                WHERE a.uid_account = p_uid_account
                AND a.attr_code =  p_arr_attr_startstop(jj).attr_code;

                   dbms_Output.put_line('EXISTING:  lv_start_time ==> '||lv_start_time ||',  lv_stop_time ==> '||lv_stop_time);

               IF  lv_new_stop_time < lv_start_time THEN
                                  dbms_Output.put_line('Adusting new tag end time to current start time-1');
                   -- Adusting new tag end time to current start time-1
                     INSERT INTO alps_account_attributes
                    (
                      uid_account,
                      attr_code,
                      attr_type ,
                      start_time,
                      stop_time ,
                      value1 ,
                      value2 ,
                      str_value1 ,
                      str_value2
                    )
                    VALUES
                    (
                     p_uid_account,
                    p_arr_attr_startstop(jj).attr_code,
                     'Planning Period' ,
                      arr_attr_startstop(jj).START_TIME,
                      lv_start_time - 1,  --arr_attr_startstop(jj).STOP_TIME ,
                      p_arr_attr_startstop(jj).VALUE1 ,
                      p_arr_attr_startstop(jj).VALUE2 ,
                      p_arr_attr_startstop(jj).STR_VALUE1 ,
                      p_arr_attr_startstop(jj).STR_VALUE2
                    );

               ELSIF ( lv_new_start_time <= lv_start_time  AND
                        lv_new_stop_time  >=  lv_stop_time )
                        OR
                        (lv_new_start_time >= lv_stop_time   )
                        THEN
                                  dbms_Output.put_line('MATCHED - remove and insert');
                   -- Remove existing
                   DELETE FROM ALPS_ACCOUNT_ATTRIBUTES
                    WHERE UID_ACCOUNT = p_uid_account
                    AND attr_code =  p_arr_attr_startstop(jj).attr_code;

                     -- Insert new CAP-TRANS attributes
                     INSERT INTO alps_account_attributes
                    (
                      uid_account,
                      attr_code,
                      attr_type ,
                      start_time,
                      stop_time ,
                      value1 ,
                      value2 ,
                      str_value1 ,
                      str_value2
                    )
                    VALUES
                    (
                     p_uid_account,
                     p_arr_attr_startstop(jj).attr_code,
                     'Planning Period' ,
                      p_arr_attr_startstop(jj).START_TIME,
                      p_arr_attr_startstop(jj).STOP_TIME ,
                      p_arr_attr_startstop(jj).VALUE1 ,
                      p_arr_attr_startstop(jj).VALUE2 ,
                      p_arr_attr_startstop(jj).STR_VALUE1 ,
                      p_arr_attr_startstop(jj).STR_VALUE2
                    );

               ELSIF lv_new_start_time >  lv_stop_time  THEN
                                  dbms_Output.put_line('Adjusting new tag start time to curent stop time+1');
                    -- Insert new CAP-TRANS attributes
                     INSERT INTO alps_account_attributes
                    (
                      uid_account,
                      attr_code,
                      attr_type ,
                      start_time,
                      stop_time ,
                      value1 ,
                      value2 ,
                      str_value1 ,
                      str_value2
                    )
                    VALUES
                    (
                     p_uid_account,
                     p_arr_attr_startstop(jj).attr_code,
                     'Planning Period' ,
                      lv_stop_time +1 , --arr_attr_startstop(jj).START_TIME,
                      p_arr_attr_startstop(jj).STOP_TIME ,
                      p_arr_attr_startstop(jj).VALUE1 ,
                      p_arr_attr_startstop(jj).VALUE2 ,
                      p_arr_attr_startstop(jj).STR_VALUE1 ,
                      p_arr_attr_startstop(jj).STR_VALUE2
                    );

               ELSIF lv_new_start_time <=  lv_start_time
                   DELETE FROM ALPS_ACCOUNT_ATTRIBUTES
                    WHERE UID_ACCOUNT = p_uid_account
                    AND attr_code =  p_arr_attr_startstop(jj).attr_code;

                     -- Insert new CAP-TRANS attributes
                     INSERT INTO alps_account_attributes
                    (
                      uid_account,
                      attr_code,
                      attr_type ,
                      start_time,
                      stop_time ,
                      value1 ,
                      value2 ,
                      str_value1 ,
                      str_value2
                    )
                    VALUES
                    (
                     p_uid_account,
                     p_arr_attr_startstop(jj).attr_code,
                     'Planning Period' ,
                      p_arr_attr_startstop(jj).START_TIME,
                      p_arr_attr_startstop(jj).STOP_TIME ,
                      p_arr_attr_startstop(jj).VALUE1 ,
                      p_arr_attr_startstop(jj).VALUE2 ,
                      p_arr_attr_startstop(jj).STR_VALUE1 ,
                      p_arr_attr_startstop(jj).STR_VALUE2
                    );

               END IF;

          END IF;
        END LOOP;
      END IF;
    EXCEPTION WHEN OTHERS THEN
       RAISE;
    END CAP_TRANS_ADJUSTMENT;      */
    PROCEDURE load_pe_tables (p_acct_array IN tab_pe_setup)
    AS
        /*
               Purpose : This procedure take in an array containing account SCALAR data and loaded into PE staging tables.

               Parameters:
                                   p_acct_array -  the array containg the account Scalar data to be set up in PE

               Execution : Called by Procedure INSERT_PE_STAGING
           */
        lv_uid_account          alps_account.uid_account%TYPE;
        lv_refreshed_acct_cnt   INTEGER := 0;
        lv_acct_cnt             INTEGER := 0;
        lv_resp_cnt             INTEGER := 0;
        lv_cnt                  INTEGER := 0;
        vedireqtagdt            DATE;
        vwebreqdt               DATE;
        vmaxuid                 alps_mml_pr_detail.uid_alps_account%TYPE;
        vDiscoReqTagOnly        VARCHAR2 (3);
        vEdiTxn                 VARCHAR2 (3);
    BEGIN
        gv_code_proc := 'LOAD_PE_TABLES';
        DBMS_OUTPUT.put_line ('IN LOAD_PE_TABLES');
        DBMS_OUTPUT.put_line ('parser name ' || p_acct_array (1).parser_name);
        DBMS_OUTPUT.put_line ('Market code ' || p_acct_array (1).market_code);        
        DBMS_OUTPUT.put_line ('Disco  Code ' || p_acct_array (1).disco_code);
        DBMS_OUTPUT.put_line ('Ldc Account ' || p_acct_array (1).ldc_account);        
        -- Retrieve the CAP/TRAN Tags data
        gv_code_ln := $$plsql_line;
        
        -- Annual tags
        SELECT b.uid_attr,
               b.attr_code,
               b.start_time,
               b.stop_time,
               TRUNC (a.value1, 4),
               TRUNC (a.value2, 4),
               a.str_value1,
               a.str_value2
          BULK COLLECT INTO arr_attr_startstop
          FROM alps_account_attr_raw  a,
               (  SELECT MAX (uid_attr)     uid_attr,
                         attr_code,
                         start_time,
                         stop_time
                    FROM alps_account_attr_raw
                   WHERE uid_account_raw = p_acct_array (1).uid_account_raw
                GROUP BY attr_code, start_time, stop_time) b
         WHERE     a.uid_attr = b.uid_attr
               AND (  (    a.attr_code IN ('TRANSMISSION_TAG')
                        AND ROUND (
                                MONTHS_BETWEEN (a.stop_time, a.start_time)) =
                            12)
                    OR (    a.attr_code IN ('CAPACITY_TAG')
                        AND ROUND (
                                MONTHS_BETWEEN (a.stop_time, a.start_time)) =
                            12))
               AND a.uid_account_raw = p_acct_array (1).uid_account_raw;
               
        -- Seasonal Tags
        -- User story 59395/Task 61950        
          SELECT b.uid_attr,
               b.attr_code,
               b.start_time,
               b.stop_time,
               TRUNC (a.value1, 4),
               TRUNC (a.value2, 4),
               a.str_value1,
               a.str_value2
          BULK COLLECT INTO arr_attr_seasonal_startstop
          FROM alps_account_attr_raw  a, 
               (  SELECT MAX (uid_attr)     uid_attr,
                         attr_code,
                         start_time,
                         stop_time
                    FROM alps_account_attr_raw
                   WHERE uid_account_raw = p_acct_array (1).uid_account_raw
                GROUP BY attr_code, start_time, stop_time) b
         WHERE     a.uid_attr = b.uid_attr
         AND a.uid_account_raw = p_acct_array (1).uid_account_raw
         AND (SELECT COUNT(1)
                    FROM alps_mml_lookup 
                    WHERE lookup_group = 'MARKET_SEASONAL_TAG' 
                    AND lookup_code = p_acct_array (1).market_code ) > 0 ;
        
       
        -- For each account, retrieve the usage data
        gv_code_ln := $$plsql_line;

          SELECT *
            BULK COLLECT INTO arr_usg_raw
            FROM alps_usage_raw
           WHERE uid_account_raw = p_acct_array (1).uid_account_raw
        ORDER BY start_time;

        --
        -- Chek to see if this account(meter) already setup in PE staging table.
        -- If existed already , then Update the account in PE Staging with incoming data
        -- If not existed, then Insert into PE Staging
        gv_code_ln := $$plsql_line;

        SELECT COUNT (1)
          INTO lv_acct_cnt
          FROM alps_account
         WHERE     market_code = p_acct_array (1).market_code
               AND disco_code = p_acct_array (1).disco_code
               AND ldc_account = p_acct_array (1).ldc_account;

        DBMS_OUTPUT.put_line (' ALPS ACCOUN  COUNT ==> ' || lv_acct_cnt);

        --
        IF lv_acct_cnt = 0
        THEN
            DBMS_OUTPUT.put_line ('INSERTING attribute');

            -- The account  does not existed in ALPS, INSERT  account
            IF p_acct_array.COUNT > 0
            THEN
                FOR ix IN p_acct_array.FIRST .. p_acct_array.LAST
                LOOP
                    gv_source_id := p_acct_array (ix).meter_id;
                    gv_code_ln := $$plsql_line;
                    DBMS_OUTPUT.put_line (
                           ' BEFORE INSERT INTO ALPS_ACCOUNT FOR METER:  ==> '
                        || p_acct_array (ix).meter_id);

                    IF    INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (ix).parser_name,
                                  p_acct_array (ix).disco_code),
                              'ATTRIBUTE') >
                          0
                       OR INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (ix).parser_name,
                                  p_acct_array (ix).disco_code),
                              'ALL') >
                          0
                    THEN
                        DBMS_OUTPUT.put_line (
                            ' BEFORE INSERT ALPS_ACCOUNT WITH ATTRIBUTE ');

                        INSERT INTO alps_account (market_code,
                                                  disco_code,
                                                  ldc_account,
                                                  meter_type,
                                                  customer_id,
                                                  meter_id,
                                                  lwsp_flag,
                                                  last_refresh_dt,
                                                  address1,
                                                  address2,
                                                  city,
                                                  state,
                                                  zip,
                                                  zipplus4,
                                                  county,
                                                  country,
                                                  rate_class,
                                                  rate_subclass,
                                                  loss_factor,
                                                  strata,
                                                  voltage,
                                                  load_profile,
                                                  meter_cycle,
                                                  mhp,
                                                  bus,
                                                  station_id,
                                                  zone,
                                                  status,
                                                  renewal_flag,
                                                  heating_fuel,
                                                  bus_type,
                                                  bus_subtype,
                                                  adv_meter_flag)
                             VALUES (
                                        p_acct_array (ix).market_code,
                                        p_acct_array (ix).disco_code,
                                        p_acct_array (ix).ldc_account,
                                        p_acct_array (ix).meter_type,
                                        p_acct_array (ix).customer_id,
                                        p_acct_array (ix).meter_id,
                                        p_acct_array (ix).lwsp_flag,
                                        p_acct_array (ix).last_refresh_dt,
                                        REPLACE (p_acct_array (ix).address1,
                                                 '"',
                                                 ''''),
                                        REPLACE (p_acct_array (ix).address2,
                                                 '"',
                                                 ''''),
                                        REPLACE (p_acct_array (ix).city,
                                                 '"',
                                                 ''''),
                                        p_acct_array (ix).state,
                                        p_acct_array (ix).zip,
                                        p_acct_array (ix).zipplus4,
                                        p_acct_array (ix).county,
                                        p_acct_array (ix).country,
                                        p_acct_array (ix).rate_class,
                                        p_acct_array (ix).rate_subclass,
                                        p_acct_array (ix).loss_factor,
                                        p_acct_array (ix).strata,
                                        p_acct_array (ix).voltage,
                                        p_acct_array (ix).load_profile,
                                        p_acct_array (ix).meter_cycle,
                                        p_acct_array (ix).mhp,
                                        p_acct_array (ix).bus,
                                        p_acct_array (ix).station_id,
                                        p_acct_array (ix).zone,
                                        p_acct_array (ix).status,
                                        p_acct_array (ix).renewal_flag,
                                        p_acct_array (ix).heating_fuel,
                                        p_acct_array (ix).bus_type,
                                        p_acct_array (ix).bus_subtype,
                                        p_acct_array (ix).adv_meter_flag)
                          RETURNING uid_account
                               INTO lv_uid_account;
                    ELSE
                        DBMS_OUTPUT.put_line (
                            ' BEFORE INSERT ALPS_ACCOUNT NO  ATTRIBUTE ');

                        INSERT INTO alps_account (market_code,
                                                  disco_code,
                                                  ldc_account,
                                                  meter_type,
                                                  customer_id,
                                                  meter_id,
                                                  lwsp_flag,
                                                  last_refresh_dt,
                                                  address1,
                                                  address2,
                                                  city,
                                                  state,
                                                  zip,
                                                  zipplus4,
                                                  county,
                                                  country)
                             VALUES (
                                        p_acct_array (ix).market_code,
                                        p_acct_array (ix).disco_code,
                                        p_acct_array (ix).ldc_account,
                                        p_acct_array (ix).meter_type,
                                        p_acct_array (ix).customer_id,
                                        p_acct_array (ix).meter_id,
                                        p_acct_array (ix).lwsp_flag,
                                        p_acct_array (ix).last_refresh_dt,
                                        REPLACE (p_acct_array (ix).address1,
                                                 '"',
                                                 ''''),
                                        REPLACE (p_acct_array (ix).address2,
                                                 '"',
                                                 ''''),
                                        REPLACE (p_acct_array (ix).city,
                                                 '"',
                                                 ''''),
                                        p_acct_array (ix).state,
                                        p_acct_array (ix).zip,
                                        p_acct_array (ix).zipplus4,
                                        p_acct_array (ix).county,
                                        p_acct_array (ix).country)
                          RETURNING uid_account
                               INTO lv_uid_account;
                    END IF;

                    DBMS_OUTPUT.put_line (
                           ' AFTER  INSERT INTO ALPS_ACCOUNT RETURNING UID:  ==> '
                        || lv_uid_account);
                    -- Insert into ALPS_ACCOUNT_REPOSITORY
                    gv_code_ln := $$plsql_line;

                    SELECT COUNT (1)
                      INTO lv_resp_cnt
                      FROM alps_account_repository
                     WHERE     market_code = p_acct_array (ix).market_code --p_market_code
                           AND disco_code = p_acct_array (ix).disco_code --p_disco_code
                           AND ldc_account = p_acct_array (ix).ldc_account; --p_ldc_account;

                    --
                    IF lv_resp_cnt = 0
                    THEN
                        gv_code_ln := $$plsql_line;

                        INSERT INTO alps_account_repository (
                                        market_code,
                                        disco_code,
                                        ldc_account,
                                        idr_meter,
                                        rate_code,
                                        profile_code,
                                        loss_factor_code,
                                        voltage,
                                        net_metetring)
                                 VALUES (
                                            p_acct_array (ix).market_code,
                                            p_acct_array (ix).disco_code,
                                            p_acct_array (ix).ldc_account,
                                            DECODE (
                                                p_acct_array (ix).meter_type,
                                                'IDR', 'Y',
                                                'N'),
                                            p_acct_array (ix).rate_class,
                                            p_acct_array (ix).load_profile,
                                            p_acct_array (ix).loss_factor,
                                            p_acct_array (ix).voltage,
                                            NULL);
                    END IF;

                    --
                    IF    INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (1).parser_name,
                                  p_acct_array (1).disco_code),
                              'CAPACITY') >
                          0
                       OR INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (1).parser_name,
                                  p_acct_array (1).disco_code),
                              'TRANSMISSION') >
                          0
                       OR INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (1).parser_name,
                                  p_acct_array (1).disco_code),
                              'ALL') >
                          0
                    THEN
                        DBMS_OUTPUT.put_line (
                            ' INSEERTING CAP/TRANS TAG  ==> ');

                        -- Insert  Account CAP/TRANS TAG
                        IF arr_attr_startstop.COUNT > 0
                        THEN
                            FOR ii IN arr_attr_startstop.FIRST ..
                                      arr_attr_startstop.LAST
                            LOOP
                                IF    (    arr_attr_startstop (ii).attr_code =
                                           'CAPACITY_TAG'
                                       AND p_acct_array (ix).market_code IN
                                               ('PJM',
                                                'NEPOOL',
                                                'NYISO'
                                                --'MISO'
                                                )
                                       )
                                   --OR   (  arr_attr_startstop(ii).ATTR_CODE = 'CAPACITY_TAG'  AND ( p_acct_array(ix).market_code = 'NYISO'  AND  p_acct_array(ix).disco_code <> 'NIMO') )
                                   OR (    arr_attr_startstop (ii).attr_code =
                                           'TRANSMISSION_TAG'
                                       AND p_acct_array (ix).market_code =
                                           'PJM')
                                THEN
                                    gv_code_ln := $$plsql_line;

                                    INSERT INTO alps_account_attributes (
                                                    uid_account,
                                                    attr_code,
                                                    attr_type,
                                                    start_time,
                                                    stop_time,
                                                    value1,
                                                    value2,
                                                    str_value1,
                                                    str_value2)
                                             VALUES (
                                                        lv_uid_account,
                                                        arr_attr_startstop (
                                                            ii).attr_code,
                                                        'Planning Period',
                                                        arr_attr_startstop (
                                                            ii).start_time,
                                                        arr_attr_startstop (
                                                            ii).stop_time,
                                                        arr_attr_startstop (
                                                            ii).value1,
                                                        arr_attr_startstop (
                                                            ii).value2,
                                                        arr_attr_startstop (
                                                            ii).str_value1,
                                                        arr_attr_startstop (
                                                            ii).str_value2);
                                END IF;
                            END LOOP;
                        END IF;

                       -- Insert Seasonal tags
                       -- User story 59395/Task 61950                       
                         IF arr_attr_Seasonal_startstop.COUNT > 0
                        THEN
                            FOR ii IN arr_attr_Seasonal_startstop.FIRST ..
                                      arr_attr_Seasonal_startstop.LAST
                            LOOP

                               gv_code_ln := $$plsql_line;

                               INSERT INTO alps_account_attributes 
                               (
                                uid_account,
                                attr_code,
                                attr_type,
                                start_time,
                                stop_time,
                                value1,
                                value2,
                                str_value1,
                                str_value2
                               )
                               VALUES 
                               (
                                lv_uid_account,
                                arr_attr_Seasonal_startstop (
                                    ii).attr_code,
                                'Planning Period',
                                arr_attr_Seasonal_startstop (
                                    ii).start_time,
                                arr_attr_Seasonal_startstop (
                                    ii).stop_time,
                                arr_attr_Seasonal_startstop (
                                    ii).value1,
                                arr_attr_Seasonal_startstop (
                                    ii).value2,
                                arr_attr_Seasonal_startstop (
                                    ii).str_value1,
                                arr_attr_Seasonal_startstop (
                                    ii).str_value2
                                );

                            END LOOP;
                        END IF;
                        
                    END IF;

                    -- Insert  Account Usages
                    gv_code_ln := $$plsql_line;

                    IF    INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (1).parser_name,
                                  p_acct_array (1).disco_code),
                              'USAGE') >
                          0
                       OR INSTR (
                              F_CUSTOMPARSINGATTR (
                                  p_acct_array (1).parser_name,
                                  p_acct_array (1).disco_code),
                              'ALL') >
                          0
                    THEN
                        DBMS_OUTPUT.put_line (' INSEERTING USAGE  ==> ');

                        IF arr_usg_raw.COUNT > 0
                        THEN
                            FOR jj IN arr_usg_raw.FIRST .. arr_usg_raw.LAST
                            LOOP
                                gv_code_ln := $$plsql_line;

                                INSERT INTO alps_usage (uid_account,
                                                        start_time,
                                                        stop_time,
                                                        usage,
                                                        demand,
                                                        usage_type,
                                                        avg_daily_usage,
                                                        load_factor)
                                         VALUES (
                                                    lv_uid_account,
                                                    arr_usg_raw (jj).start_time,
                                                    arr_usg_raw (jj).stop_time,
                                                    arr_usg_raw (jj).usage,
                                                    arr_usg_raw (jj).demand,
                                                    arr_usg_raw (jj).usage_type,
                                                    TRUNC (
                                                          arr_usg_raw (jj).usage
                                                        / (  arr_usg_raw (jj).stop_time
                                                           - arr_usg_raw (jj).start_time),
                                                        5),
                                                    TRUNC (
                                                        DECODE (
                                                            NVL (
                                                                arr_usg_raw (
                                                                    jj).demand,
                                                                0),
                                                            0, 0,
                                                              (  arr_usg_raw (
                                                                     jj).usage
                                                               / (  arr_usg_raw (
                                                                        jj).stop_time
                                                                  - arr_usg_raw (
                                                                        jj).start_time))
                                                            / (  24
                                                               * arr_usg_raw (
                                                                     jj).demand)),
                                                        5));
                            END LOOP;
                        END IF;
                    END IF;

                    -- Cap Tags special update- NYISO for now
                    ALPS.CAPTAG_UPDATE_SPECIAL (p_acct_array (ix).meter_id,
                                                lv_uid_account);
                END LOOP;
            END IF;
        ELSE             -- Update Accounts  attribute, CAP-TRANS Tags, Usages
            DBMS_OUTPUT.put_line ('UPDATING attribute');
            gv_code_ln := $$plsql_line;

            -- Check Refresh Date
            SELECT COUNT (1), MAX (uid_account)
              INTO lv_refreshed_acct_cnt, lv_uid_account
              FROM alps_account
             WHERE     market_code = p_acct_array (1).market_code
                   AND disco_code = p_acct_array (1).disco_code
                   AND ldc_account = p_acct_array (1).ldc_account
                   AND last_refresh_dt < p_acct_array (1).last_refresh_dt;

            --
            IF lv_refreshed_acct_cnt > 0
            THEN
                IF p_acct_array.COUNT > 0
                THEN
                    DBMS_OUTPUT.put_line ('IN UPDATE attribute');

                    --
                    FOR ix IN p_acct_array.FIRST .. p_acct_array.LAST
                    LOOP
                        gv_code_ln := $$plsql_line;

                        IF    INSTR (
                                  F_CUSTOMPARSINGATTR (
                                      p_acct_array (1).parser_name,
                                      p_acct_array (1).disco_code),
                                  'ATTRIBUTE') >
                              0
                           OR INSTR (
                                  F_CUSTOMPARSINGATTR (
                                      p_acct_array (1).parser_name,
                                      p_acct_array (1).disco_code),
                                  'ALL') >
                              0
                        THEN
                            DBMS_OUTPUT.PUT_LINE ('IN UPDATE attribute');

                            UPDATE alps_account
                               SET meter_type = p_acct_array (ix).meter_type,
                                   customer_id =
                                       p_acct_array (ix).customer_id,
                                   meter_id = p_acct_array (ix).meter_id,
                                   lwsp_flag = p_acct_array (ix).lwsp_flag,
                                   last_refresh_dt =
                                       p_acct_array (ix).last_refresh_dt,
                                   address1 =
                                       REPLACE (p_acct_array (ix).address1,
                                                '"',
                                                ''''),
                                   address2 =
                                       REPLACE (p_acct_array (ix).address2,
                                                '"',
                                                ''''),
                                   city =
                                       REPLACE (p_acct_array (ix).city,
                                                '"',
                                                ''''),
                                   state = p_acct_array (ix).state,
                                   zip = p_acct_array (ix).zip,
                                   zipplus4 = p_acct_array (ix).zipplus4,
                                   county = p_acct_array (ix).county,
                                   country = p_acct_array (ix).country,
                                   rate_class = p_acct_array (ix).rate_class,
                                   rate_subclass =
                                       p_acct_array (ix).rate_subclass,
                                   loss_factor =
                                       p_acct_array (ix).loss_factor,
                                   strata = p_acct_array (ix).strata,
                                   voltage = p_acct_array (ix).voltage,
                                   load_profile =
                                       p_acct_array (ix).load_profile,
                                   meter_cycle =
                                       p_acct_array (ix).meter_cycle,
                                   mhp = p_acct_array (ix).mhp,
                                   bus = p_acct_array (ix).bus,
                                   station_id = p_acct_array (ix).station_id,
                                   zone = p_acct_array (ix).zone,
                                   status = p_acct_array (ix).status,
                                   renewal_flag =
                                       p_acct_array (ix).renewal_flag,
                                   heating_fuel =
                                       p_acct_array (ix).heating_fuel,
                                   bus_type = p_acct_array (ix).bus_type,
                                   bus_subtype =
                                       p_acct_array (ix).bus_subtype
                             WHERE uid_account = lv_uid_account;
                        END IF;

                        --  Update ALPS_ACCOUNT_REPOSITORY
                        gv_code_ln := $$plsql_line;

                        SELECT COUNT (1)
                          INTO lv_resp_cnt
                          FROM alps_account_repository
                         WHERE     market_code =
                                   p_acct_array (ix).market_code --p_market_code
                               AND disco_code = p_acct_array (ix).disco_code --p_disco_code
                               AND ldc_account =
                                   p_acct_array (ix).ldc_account; --p_ldc_account;

                        --
                        IF lv_resp_cnt = 0
                        THEN
                            gv_code_ln := $$plsql_line;

                            INSERT INTO alps_account_repository (
                                            market_code,
                                            disco_code,
                                            ldc_account,
                                            idr_meter,
                                            rate_code,
                                            profile_code,
                                            loss_factor_code,
                                            voltage,
                                            net_metetring)
                                     VALUES (
                                                p_acct_array (ix).market_code,
                                                p_acct_array (ix).disco_code,
                                                p_acct_array (ix).ldc_account,
                                                DECODE (
                                                    p_acct_array (ix).meter_type,
                                                    'IDR', 'Y',
                                                    'N'),
                                                p_acct_array (ix).rate_class,
                                                p_acct_array (ix).load_profile,
                                                p_acct_array (ix).loss_factor,
                                                p_acct_array (ix).voltage,
                                                NULL);
                        ELSE
                            gv_code_ln := $$plsql_line;

                            UPDATE alps_account_repository
                               SET idr_meter =
                                       DECODE (p_acct_array (ix).meter_type,
                                               'IDR', 'Y',
                                               'N'),
                                   rate_code = p_acct_array (ix).rate_class,
                                   profile_code =
                                       p_acct_array (ix).load_profile,
                                   loss_factor_code =
                                       p_acct_array (ix).loss_factor,
                                   voltage = p_acct_array (ix).voltage
                             WHERE     market_code =
                                       p_acct_array (ix).market_code
                                   AND disco_code =
                                       p_acct_array (ix).disco_code
                                   AND ldc_account =
                                       p_acct_array (ix).ldc_account;
                        END IF;

                        -- Replace  Account Usage data
                        IF arr_usg_raw.COUNT > 0
                        THEN
                            gv_code_ln := $$plsql_line;

                            IF    INSTR (
                                      F_CUSTOMPARSINGATTR (
                                          p_acct_array (1).parser_name,
                                          p_acct_array (1).disco_code),
                                      'USAGE') >
                                  0
                               OR INSTR (
                                      F_CUSTOMPARSINGATTR (
                                          p_acct_array (1).parser_name,
                                          p_acct_array (1).disco_code),
                                      'ALL') >
                                  0
                            THEN
                                -- Remove duplicate/overlap month then insert Raw months

                                DBMS_OUTPUT.PUT_LINE ('IN updating usage');
                                -- Insert annual strip 
                             --   SELECT MONTHS_BETWEEN(arr_usg_raw (1).stop_time, arr_usg_raw (1).start_time) 
                              --  INTO vmonth_count 
                              --  FROM dual;
                                -- It is annual strip if month counts between STOP/START date is greater than 1 month
      
                                dbms_output.put_line(' months between==> '||MONTHS_BETWEEN(arr_usg_raw (1).stop_time, arr_usg_raw (1).start_time)) ;
                            dbms_output.put_line(' months count ==> '||arr_usg_raw.COUNT) ;
                                
                                IF MONTHS_BETWEEN(arr_usg_raw (1).stop_time, arr_usg_raw (1).start_time) > 1 AND 
                                   arr_usg_raw.COUNT = 1
                                THEN
                                    DELETE 
                                    FROM  alps_usage
                                    WHERE uid_account = lv_uid_account;                                      
                                END IF;
               
                                --
                                
                                -- Append 
                                DELETE FROM
                                    alps_usage
                                      WHERE     uid_account = lv_uid_account
                                            AND stop_time >=
                                                arr_usg_raw (1).start_time;

                                --
                                FOR jj IN arr_usg_raw.FIRST ..
                                          arr_usg_raw.LAST
                                LOOP
                                    BEGIN
                                        gv_code_ln := $$plsql_line;

                                        INSERT INTO alps_usage (
                                                        uid_account,
                                                        start_time,
                                                        stop_time,
                                                        usage,
                                                        demand,
                                                        usage_type,
                                                        avg_daily_usage,
                                                        load_factor)
                                                 VALUES (
                                                            lv_uid_account,
                                                            arr_usg_raw (jj).start_time,
                                                            arr_usg_raw (jj).stop_time,
                                                            arr_usg_raw (jj).usage,
                                                            arr_usg_raw (jj).demand,
                                                            arr_usg_raw (jj).usage_type,
                                                            TRUNC (
                                                                  arr_usg_raw (
                                                                      jj).usage
                                                                / (  arr_usg_raw (
                                                                         jj).stop_time
                                                                   - arr_usg_raw (
                                                                         jj).start_time),
                                                                5),
                                                            TRUNC (
                                                                DECODE (
                                                                    NVL (
                                                                        arr_usg_raw (
                                                                            jj).demand,
                                                                        0),
                                                                    0, 0,
                                                                      (  arr_usg_raw (
                                                                             jj).usage
                                                                       / (  arr_usg_raw (
                                                                                jj).stop_time
                                                                          - arr_usg_raw (
                                                                                jj).start_time))
                                                                    / (  24
                                                                       * arr_usg_raw (
                                                                             jj).demand)),
                                                                5));
                                    EXCEPTION
                                        WHEN DUP_VAL_ON_INDEX
                                        THEN
                                            NULL;
                                    END;
                                END LOOP;
                            END IF;
                        END IF;

                        -- Update TRANS/CAP tag
                        IF arr_attr_startstop.COUNT > 0
                        THEN
                            IF    INSTR (
                                      F_CUSTOMPARSINGATTR (
                                          p_acct_array (1).parser_name,
                                          p_acct_array (1).disco_code),
                                      'CAPACITY') >
                                  0
                               OR INSTR (
                                      F_CUSTOMPARSINGATTR (
                                          p_acct_array (1).parser_name,
                                          p_acct_array (1).disco_code),
                                      'TRANSMISSION') >
                                  0
                               OR INSTR (
                                      F_CUSTOMPARSINGATTR (
                                          p_acct_array (1).parser_name,
                                          p_acct_array (1).disco_code),
                                      'ALL') >
                                  0
                            THEN
                                --
                                FOR ii IN arr_attr_startstop.FIRST ..
                                          arr_attr_startstop.LAST
                                LOOP
                                    IF    (    arr_attr_startstop (ii).attr_code =
                                               'CAPACITY_TAG'
                                           AND p_acct_array (ix).market_code IN
                                                   ('PJM',
                                                    'NEPOOL',
                                                    'NYISO'--,
                                                   -- 'MISO'
                                                    )
                                          )
                                       --OR   (  arr_attr_startstop(ii).ATTR_CODE = 'CAPACITY_TAG'  AND ( p_acct_array(ix).market_code = 'NYISO'  AND  p_acct_array(ix).disco_code <> 'NIMO') )
                                       OR (    arr_attr_startstop (ii).attr_code =
                                               'TRANSMISSION_TAG'
                                           AND p_acct_array (ix).market_code =
                                               'PJM')
                                    THEN
                                        --  Remove existing ATTRIBUTES
                                        gv_code_ln := $$plsql_line;

                                        DELETE FROM
                                            alps_account_attributes
                                              WHERE     uid_account =
                                                        lv_uid_account
                                                    AND attr_code =
                                                        arr_attr_startstop (
                                                            ii).attr_code
                                                    AND TRUNC (start_time) BETWEEN TRUNC (
                                                                                       arr_attr_startstop (
                                                                                           ii).start_time)
                                                                               AND TRUNC (
                                                                                       arr_attr_startstop (
                                                                                           ii).stop_time);

                                        --
                                        gv_code_ln := $$plsql_line;

                                        INSERT INTO alps_account_attributes (
                                                        uid_account,
                                                        attr_code,
                                                        attr_type,
                                                        start_time,
                                                        stop_time,
                                                        value1,
                                                        value2,
                                                        str_value1,
                                                        str_value2)
                                                 VALUES (
                                                            lv_uid_account,
                                                            arr_attr_startstop (
                                                                ii).attr_code,
                                                            'Planning Period',
                                                            arr_attr_startstop (
                                                                ii).start_time,
                                                            arr_attr_startstop (
                                                                ii).stop_time,
                                                            arr_attr_startstop (
                                                                ii).value1,
                                                            arr_attr_startstop (
                                                                ii).value2,
                                                            arr_attr_startstop (
                                                                ii).str_value1,
                                                            arr_attr_startstop (
                                                                ii).str_value2);
                                    END IF;
                                END LOOP;
                            END IF;
                        END IF;

                        -- Cap Tags special update- NYISO for now
                        ALPS.CAPTAG_UPDATE_SPECIAL (
                            p_acct_array (ix).meter_id,
                            lv_uid_account);
                        
                        -- Seasonal tags update
                        -- Update TRANS/CAP tag
                        -- User story 59395/Task 61950                        
                        IF arr_attr_seasonal_startstop.COUNT > 0
                        THEN                  
                             --                                                                    
                            FOR ii IN arr_attr_seasonal_startstop.FIRST ..
                                      arr_attr_seasonal_startstop.LAST
                            LOOP                               
                                --  Remove existing ATTRIBUTES
                                gv_code_ln := $$plsql_line;

                                DELETE FROM  alps_account_attributes
                                WHERE uid_account = lv_uid_account
                                AND attr_code = arr_attr_seasonal_startstop (ii).attr_code
                                AND TRUNC (start_time) BETWEEN TRUNC (
                                                               arr_attr_seasonal_startstop (
                                                                   ii).start_time)
                                                       AND TRUNC (
                                                               arr_attr_seasonal_startstop (
                                                                   ii).stop_time);   
                              
                                --

                                INSERT INTO alps_account_attributes (
                                                uid_account,
                                                attr_code,
                                                attr_type,
                                                start_time,
                                                stop_time,
                                                value1,
                                                value2,
                                                str_value1,
                                                str_value2)
                                 VALUES (lv_uid_account,
                                         arr_attr_seasonal_startstop (ii).attr_code,
                                        'Planning Period',
                                        arr_attr_seasonal_startstop (ii).start_time,
                                        arr_attr_seasonal_startstop (ii).stop_time,
                                        arr_attr_seasonal_startstop (ii).value1,
                                        arr_attr_seasonal_startstop (ii).value2,
                                        arr_attr_seasonal_startstop (ii).str_value1,
                                        arr_attr_seasonal_startstop (ii).str_value2);                                                    

                            END LOOP;
                           
                        END IF;                        
                    END LOOP;
                END IF;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                   ' AFTER  INSERT INTO ALPS_ACCOUNT RETURNING UID:  ==> '
                || lv_uid_account);
            RAISE;
    END load_pe_tables;

     PROCEDURE insert_pe_staging (p_market_code   IN     VARCHAR2,
                                 p_disco_code    IN     VARCHAR2,
                                 p_ldc_account   IN     VARCHAR2,
                                 p_out_status       OUT VARCHAR2,
                                 p_out_desc         OUT VARCHAR2)
    AS
    /*
           Purpose : This procedure prepare account SCALAR data to setup in PE.
                           Once the account received and parsed successfully, loaded all the account attributes (premise, cap-tag,trans-tag and usages) into an array to be in sert into PE staging

           Parameters:
                               p_market_code -  Market of an account
                               p_disco_code -  DIsco of an account
                               p_ldc_account - account
                               p_out_status - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                               p_out_description - Description of the function call returned to calling program(process).

           Execution : Called by Procedure UPDATE_PARSED_SCALAR_ACCTS
       */
    BEGIN
        DBMS_OUTPUT.put_line ('INSERT_PE_STAGING : p_market_code ==> ' || p_market_code);
        DBMS_OUTPUT.put_line ('INSERT_PE_STAGING : p_disco_code ==> ' || p_disco_code);
        DBMS_OUTPUT.put_line ('INSERT_PE_STAGING : p_ldc_account ==> ' || p_ldc_account);

        gv_code_proc := 'INSERT_PE_STAGING';
        --
        gv_code_ln := $$plsql_line;
      
        --Matched by MARKET_DISCO_LDC
        SELECT DISTINCT
               e.uid_alps_account,
               a.uid_account_raw,
               a.market_code,
               a.disco_code,
               a.ldc_account,
               NVL (
                   d.meter_type,
                   (SELECT DECODE (meter_type,
                                   'SCA', 'SCALAR',
                                   'IDR', 'IDR',
                                   meter_type)
                      FROM alps_mml_pr_detail
                     WHERE uid_alps_account = e.uid_alps_account))
                   meter_type,
               --c.customer_id,
              NVL( (SELECT a.customer_id 
                  FROM alps_mml_pr_header a
                 WHERE uid_mml_master IN
                           (SELECT MAX (a.uid_mml_master)
                              FROM alps_mml_pr_header a, alps_mml_pr_detail b
                             WHERE     a.active_flg = 'Y'
                                   AND a.uid_mml_master = b.uid_mml_master
                                   AND b.ldc_account = p_ldc_account
                                   AND B.MARKET_CODE = p_market_code
                                   AND b.disco_code = p_disco_code)), (SELECT MAX(customer_id)
                                                                  FROM alps_mml_stg 
                                                                  WHERE market_code = p_market_code
                                                                    and disco_code = p_disco_code
                                                                    and ldc_account = p_ldc_account ) )
                   customer_id, --70455
               p_market_code || '_' || p_disco_code || '_' || p_ldc_account
                   meter_id,
               c.lwsp_flag,
               a.refresh_dt,
               DECODE (
                   (SELECT COUNT (1)
                      FROM alps_account_transaction
                     WHERE     uid_alps_account = e.uid_alps_account
                           AND transact_type = 'REQUEST_LPSS'),
                   0, DECODE (
                          (SELECT lookup_str_value1
                             FROM alps_mml_lookup
                            WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                  AND lookup_code = e.disco_code),
                          'Y', NVL (
                                   REPLACE (d.address1, '''', '"'),
                                   NVL (
                                       (SELECT REPLACE (address1, '''', '"')
                                          FROM alps_mml_pr_detail
                                         WHERE uid_alps_account =
                                               e.uid_alps_account),
                                       'UNKNOWN')),
                          NVL (REPLACE (b.address1, '''', '"'), 'UNKNOWN')),
                   NVL (REPLACE (d.address1, '''', '"'), 'UNKNOWN'))
                   address1,
               DECODE (
                   (SELECT COUNT (1)
                      FROM alps_account_transaction
                     WHERE     uid_alps_account = e.uid_alps_account
                           AND transact_type = 'REQUEST_LPSS'),
                   0, DECODE (
                          (SELECT lookup_str_value1
                             FROM alps_mml_lookup
                            WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                  AND lookup_code = e.disco_code),
                          'Y', NVL (
                                   REPLACE (d.address2, '''', '"'),
                                   NVL (
                                       (SELECT REPLACE (address2, '''', '"')
                                          FROM alps_mml_pr_detail
                                         WHERE uid_alps_account =
                                               e.uid_alps_account),
                                       'UNKNOWN')),
                          NVL (REPLACE (b.address2, '''', '"'), 'UNKNOWN')),
                   NVL (REPLACE (d.address2, '''', '"'), 'UNKNOWN'))
                   address2,
               DECODE (
                   (SELECT COUNT (1)
                      FROM alps_account_transaction
                     WHERE     uid_alps_account = e.uid_alps_account
                           AND transact_type = 'REQUEST_LPSS'),
                   0, DECODE (
                          (SELECT lookup_str_value1
                             FROM alps_mml_lookup
                            WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                  AND lookup_code = e.disco_code),
                          'Y', NVL (
                                   REPLACE (d.city, '''', '"'),
                                   NVL (
                                       (SELECT REPLACE (city, '''', '"')
                                          FROM alps_mml_pr_detail
                                         WHERE uid_alps_account =
                                               e.uid_alps_account),
                                       'UNKNOWN')),
                          NVL (REPLACE (b.city, '''', '"'), 'UNKNOWN')),
                   NVL (REPLACE (d.city, '''', '"'), 'UNKNOWN'))
                   city,
               NVL (d.state,
                    (SELECT MAX (state)
                       FROM alps_mml_pr_detail
                      WHERE     market_code = p_market_code
                            AND disco_code = p_disco_code
                            AND ldc_account = p_ldc_account
                            AND (   sca_status IN ('REQUEST_COMPLETE',
                                                   'DATA_ERROR',
                                                   'REQUEST_FAILED',
                                                   'REQUEST_PENDING',
                                                   'WEB_SCRAPE_PENDING')
                                 OR idr_status IN ('REQUEST_COMPLETE',
                                                   'DATA_ERROR',
                                                   'REQUEST_FAILED',
                                                   'REQUEST_PENDING',
                                                   'WEB_SCRAPE_PENDING'))))
                   state,
               DECODE (
                   (SELECT COUNT (1)
                      FROM alps_account_transaction
                     WHERE     uid_alps_account = e.uid_alps_account
                           AND transact_type = 'REQUEST_LPSS'),
                   0, DECODE (
                          (SELECT lookup_str_value1
                             FROM alps_mml_lookup
                            WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                  AND lookup_code = e.disco_code),
                          'Y', NVL (
                                   SUBSTR (REPLACE (TRIM (d.zip), '-', ''),
                                           1,
                                           5),
                                   NVL (
                                       (SELECT SUBSTR (
                                                   REPLACE (TRIM (zip),
                                                            '-',
                                                            ''),
                                                   1,
                                                   5)
                                          FROM alps_mml_pr_detail
                                         WHERE uid_alps_account =
                                               e.uid_alps_account),
                                       '00000')),
                          NVL (
                              SUBSTR (REPLACE (TRIM (b.zip), '-', ''), 1, 5),
                              '00000')),
                   NVL (SUBSTR (REPLACE (TRIM (d.zip), '-', ''), 1, 5),
                        '00000'))
                   zip,
               DECODE (
                   (SELECT COUNT (1)
                      FROM alps_account_transaction
                     WHERE     uid_alps_account = e.uid_alps_account
                           AND transact_type = 'REQUEST_LPSS'),
                   0, DECODE (
                          (SELECT lookup_str_value1
                             FROM alps_mml_lookup
                            WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                  AND lookup_code = e.disco_code),
                          'Y', NVL (
                                   SUBSTR (REPLACE (TRIM (d.zip), '-', ''),
                                           -4),
                                   NVL (
                                       (SELECT SUBSTR (
                                                   REPLACE (TRIM (zip),
                                                            '-',
                                                            ''),
                                                   -4)
                                          FROM alps_mml_pr_detail
                                         WHERE uid_alps_account =
                                               e.uid_alps_account),
                                       '0000')),
                          NVL (SUBSTR (REPLACE (TRIM (b.zip), '-', ''), -4),
                               '0000')),
                   NVL (SUBSTR (REPLACE (TRIM (d.zip), '-', ''), -4), '0000'))
                   zipplus4,
               NVL (d.county, 'UNKNOWN'),
               NVL (d.country, 'USA')
                   country,
               d.rate_class,
               d.rate_subclass,
               d.loss_factor,
               d.strata,
               d.voltage,
               d.load_profile,
               d.meter_cycle,
               d.mhp,
               d.bus,
               d.station_id,
               DECODE (d.disco_code, 'FGE', 'WCMASS', d.zone),
               NULL
                   status,
               DECODE (
                   c.pr_trans_type,
                   'New', 'N',
                   DECODE (
                       c.pr_trans_type,
                       'Amendment - Add', 'N',
                       DECODE (c.pr_trans_type, 'Previously Lost', 'N', 'Y')))
                   renewal_flag,
               f.heating_fuel,
               f.bus_type,
               f.bus_subtype,
               d.adv_meter_flag,
               d.exception_assigned
          BULK COLLECT INTO arr_pe_setup
          FROM (  SELECT MAX (uid_account_raw)     uid_account_raw,
                         MAX (updated_dt)          refresh_dt,
                         market_code,
                         disco_code,
                         ldc_account
                    FROM alps_account_raw
                   WHERE     market_code = p_market_code
                         AND disco_code = p_disco_code
                         AND ldc_account = p_ldc_account
                GROUP BY market_code, disco_code, ldc_account) a,
               alps_account_raw    d,
               alps_mml_pr_header  c,
               alps_mml_pr_detail  b,
               (  SELECT MAX (uid_alps_account)     uid_alps_account,
                         market_code,
                         disco_code,
                         ldc_account
                    FROM alps_mml_pr_detail dtl
                   WHERE     market_code = p_market_code
                         AND disco_code = p_disco_code
                         AND ldc_account = p_ldc_account
                         AND (   sca_status IN ('REQUEST_COMPLETE',
                                                'DATA_ERROR',
                                                'REQUEST_FAILED',
                                                'REQUEST_PENDING',
                                                'WEB_SCRAPE_PENDING')
                              OR idr_status IN ('REQUEST_COMPLETE',
                                                'DATA_ERROR',
                                                'REQUEST_FAILED',
                                                'REQUEST_PENDING',
                                                'WEB_SCRAPE_PENDING'))
                         AND EXISTS
                                 (SELECT 1
                                    FROM alps_mml_pr_header hdr
                                   WHERE     hdr.sbl_quote_id =
                                             dtl.sbl_quote_id
                                         AND hdr.active_flg = 'Y'
                                         AND hdr.status = 'PENDING')
                GROUP BY market_code, disco_code, ldc_account) e,
               (SELECT row_id,
                       attrib_37     heating_fuel,
                       attrib_35     bus_type,
                       attrib_36     bus_subtype
                  FROM siebel.s_doc_quote_x@TPSBL) f
         WHERE     a.market_code = b.market_code
               AND a.disco_code = b.disco_code
               AND a.ldc_account = b.ldc_account
               AND a.uid_account_raw = d.uid_account_raw
               AND e.uid_alps_account = b.uid_alps_account
               AND b.sbl_quote_id = c.sbl_quote_id
               AND b.sbl_quote_id = f.row_id(+)
               AND c.active_flg = 'Y'
               AND c.status = 'PENDING'
               AND (   b.sca_status IN ('REQUEST_COMPLETE',
                                        'DATA_ERROR',
                                        'REQUEST_FAILED',
                                        'REQUEST_PENDING',
                                        'WEB_SCRAPE_PENDING')
                    OR b.idr_status IN ('REQUEST_COMPLETE',
                                        'DATA_ERROR',
                                        'REQUEST_FAILED',
                                        'REQUEST_PENDING',
                                        'WEB_SCRAPE_PENDING'));

        --
        IF arr_pe_setup.COUNT > 0
        THEN
            DBMS_OUTPUT.put_line ('Matched by Market_Disco_Ldc');
            --Loaded into PE staging tables
            gv_source_id := arr_pe_setup (1).meter_id;
            load_pe_tables (arr_pe_setup);
            p_out_status := 'MARKET_DISCO_LDC';
            p_out_desc := NULL;
        ELSE
            gv_code_ln := $$plsql_line;

            -- Matched by  BA
            SELECT DISTINCT
                   e.uid_alps_account,
                   a.uid_account_raw,
                   p_market_code, --a.market_code,
                   p_disco_code, --a.disco_code,
                   p_ldc_account , --a.ldc_account,
                   NVL (
                       d.meter_type,
                       (SELECT DECODE (meter_type,
                                       'SCA', 'SCALAR',
                                       'IDR', 'IDR',
                                       meter_type)
                          FROM alps_mml_pr_detail
                         WHERE uid_alps_account = e.uid_alps_account)),
 NVL( (SELECT a.customer_id
                  FROM alps_mml_pr_header a
                 WHERE uid_mml_master IN
                           (SELECT MAX (a.uid_mml_master)
                              FROM alps_mml_pr_header a, alps_mml_pr_detail b
                             WHERE     a.active_flg = 'Y'
                                   AND a.uid_mml_master = b.uid_mml_master
                                   AND b.ldc_account = p_ldc_account
                                   AND B.MARKET_CODE = p_market_code
                                   AND b.disco_code = p_disco_code )), (SELECT MAX(customer_id)
                                                                  FROM alps_mml_stg 
                                                                  WHERE market_code = p_market_code
                                                                    and disco_code = p_disco_code
                                                                    and ldc_account = p_ldc_account ) )
                   customer_id,  --70455
                      p_market_code
                   || '_'
                   || p_disco_code
                   || '_'
                   || p_ldc_account
                       meter_id,
                   c.lwsp_flag,
                   a.refresh_dt,
                   DECODE (
                       (SELECT COUNT (1)
                          FROM alps_account_transaction
                         WHERE     uid_alps_account = e.uid_alps_account
                               AND transact_type = 'REQUEST_LPSS'),
                       0, DECODE (
                              (SELECT lookup_str_value1
                                 FROM alps_mml_lookup
                                WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                      AND lookup_code = e.disco_code),
                              'Y', NVL (
                                       REPLACE (d.address1, '''', '"'),
                                       NVL (
                                           (SELECT REPLACE (address1,
                                                            '''',
                                                            '"')
                                              FROM alps_mml_pr_detail
                                             WHERE uid_alps_account =
                                                   e.uid_alps_account),
                                           'UNKNOWN')),
                              NVL (REPLACE (b.address1, '''', '"'),
                                   'UNKNOWN')),
                       NVL (REPLACE (d.address1, '''', '"'), 'UNKNOWN'))
                       address1,
                   DECODE (
                       (SELECT COUNT (1)
                          FROM alps_account_transaction
                         WHERE     uid_alps_account = e.uid_alps_account
                               AND transact_type = 'REQUEST_LPSS'),
                       0, DECODE (
                              (SELECT lookup_str_value1
                                 FROM alps_mml_lookup
                                WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                      AND lookup_code = e.disco_code),
                              'Y', NVL (
                                       REPLACE (d.address2, '''', '"'),
                                       NVL (
                                           (SELECT REPLACE (address2,
                                                            '''',
                                                            '"')
                                              FROM alps_mml_pr_detail
                                             WHERE uid_alps_account =
                                                   e.uid_alps_account),
                                           'UNKNOWN')),
                              NVL (REPLACE (b.address2, '''', '"'),
                                   'UNKNOWN')),
                       NVL (REPLACE (d.address2, '''', '"'), 'UNKNOWN'))
                       address2,
                   DECODE (
                       (SELECT COUNT (1)
                          FROM alps_account_transaction
                         WHERE     uid_alps_account = e.uid_alps_account
                               AND transact_type = 'REQUEST_LPSS'),
                       0, DECODE (
                              (SELECT lookup_str_value1
                                 FROM alps_mml_lookup
                                WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                      AND lookup_code = e.disco_code),
                              'Y', NVL (
                                       REPLACE (d.city, '''', '"'),
                                       NVL (
                                           (SELECT REPLACE (city, '''', '"')
                                              FROM alps_mml_pr_detail
                                             WHERE uid_alps_account =
                                                   e.uid_alps_account),
                                           'UNKNOWN')),
                              NVL (REPLACE (b.city, '''', '"'), 'UNKNOWN')),
                       NVL (REPLACE (d.city, '''', '"'), 'UNKNOWN'))
                       city,
                   NVL (
                       d.state,
                       (SELECT MAX (state)
                          FROM alps_mml_pr_detail
                         WHERE     market_code = p_market_code
                               --AND disco_code = p_disco_code
                               AND ldc_account =
                                   SUBSTR (p_ldc_account,
                                           1,
                                             INSTR (p_ldc_account,
                                                    '_',
                                                    1,
                                                    1)
                                           - 1)
                               AND (   sca_status IN ('REQUEST_COMPLETE',
                                                      'DATA_ERROR',
                                                      'REQUEST_FAILED',
                                                      'REQUEST_PENDING',
                                                      'WEB_SCRAPE_PENDING')
                                    OR idr_status IN ('REQUEST_COMPLETE',
                                                      'DATA_ERROR',
                                                      'REQUEST_FAILED',
                                                      'REQUEST_PENDING',
                                                      'WEB_SCRAPE_PENDING'))))
                       state,
                   DECODE (
                       (SELECT COUNT (1)
                          FROM alps_account_transaction
                         WHERE     uid_alps_account = e.uid_alps_account
                               AND transact_type = 'REQUEST_LPSS'),
                       0, DECODE (
                              (SELECT lookup_str_value1
                                 FROM alps_mml_lookup
                                WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                      AND lookup_code = e.disco_code),
                              'Y', NVL (
                                       SUBSTR (
                                           REPLACE (TRIM (d.zip), '-', ''),
                                           1,
                                           5),
                                       NVL (
                                           (SELECT SUBSTR (
                                                       REPLACE (TRIM (zip),
                                                                '-',
                                                                ''),
                                                       1,
                                                       5)
                                              FROM alps_mml_pr_detail
                                             WHERE uid_alps_account =
                                                   e.uid_alps_account),
                                           '00000')),
                              NVL (
                                  SUBSTR (REPLACE (TRIM (b.zip), '-', ''),
                                          1,
                                          5),
                                  '00000')),
                       NVL (SUBSTR (REPLACE (TRIM (d.zip), '-', ''), 1, 5),
                            '00000'))
                       zip,
                   DECODE (
                       (SELECT COUNT (1)
                          FROM alps_account_transaction
                         WHERE     uid_alps_account = e.uid_alps_account
                               AND transact_type = 'REQUEST_LPSS'),
                       0, DECODE (
                              (SELECT lookup_str_value1
                                 FROM alps_mml_lookup
                                WHERE     lookup_group = 'ADDR_ATTR_DISCO'
                                      AND lookup_code = e.disco_code),
                              'Y', NVL (
                                       SUBSTR (
                                           REPLACE (TRIM (d.zip), '-', ''),
                                           -4),
                                       NVL (
                                           (SELECT SUBSTR (
                                                       REPLACE (TRIM (zip),
                                                                '-',
                                                                ''),
                                                       -4)
                                              FROM alps_mml_pr_detail
                                             WHERE uid_alps_account =
                                                   e.uid_alps_account),
                                           '0000')),
                              NVL (
                                  SUBSTR (REPLACE (TRIM (b.zip), '-', ''),
                                          -4),
                                  '0000')),
                       NVL (SUBSTR (REPLACE (TRIM (d.zip), '-', ''), -4),
                            '0000'))
                       zipplus4,
                   NVL (d.county, 'UNNOWN'),
                   NVL (d.country, 'USA')
                       country,
                   d.rate_class,
                   d.rate_subclass,
                   d.loss_factor,
                   d.strata,
                   d.voltage,
                   d.load_profile,
                   d.meter_cycle,
                   d.mhp,
                   d.bus,
                   d.station_id,
                   DECODE (d.disco_code, 'FGE', 'WCMASS', d.zone),
                   NULL
                       status,
                   DECODE (
                       c.pr_trans_type,
                       'New', 'N',
                       DECODE (
                           c.pr_trans_type,
                           'Amendment - Add', 'N',
                           DECODE (c.pr_trans_type,
                                   'Previously Lost', 'N',
                                   'Y')))
                       renewal_flag,
                   f.heating_fuel,
                   f.bus_type,
                   f.bus_subtype,
                   d.adv_meter_flag,
                   d.exception_assigned
              BULK COLLECT INTO arr_pe_setup
              FROM (  SELECT MAX (uid_account_raw)     uid_account_raw,
                             MAX (updated_dt)          refresh_dt,
                             market_code,
                             ldc_account
                        FROM alps_account_raw
                       WHERE     market_code = p_market_code
                          --   AND ldc_account = p_ldc_account
                         AND ldc_account =  p_ldc_account
                    GROUP BY market_code, ldc_account) a,
                   alps_account_raw    d,
                   alps_mml_pr_header  c,
                   alps_mml_pr_detail  b,
                   (  SELECT MAX (uid_alps_account)     uid_alps_account,
                             market_code,
                             disco_code,
                             ldc_account
                        FROM alps_mml_pr_detail
                       WHERE     market_code = p_market_code
                            -- AND disco_code = p_disco_code
                             AND ldc_account = SUBSTR (p_ldc_account,
                                                       1,
                                                         INSTR (p_ldc_account,
                                                                '_',
                                                                1,
                                                                1)
                                                       - 1) -- Matched by BA only
                             AND (   sca_status IN ('REQUEST_COMPLETE',
                                                    'DATA_ERROR',
                                                    'REQUEST_FAILED',
                                                    'REQUEST_PENDING',
                                                    'WEB_SCRAPE_PENDING')
                                  OR idr_status IN ('REQUEST_COMPLETE',
                                                    'DATA_ERROR',
                                                    'REQUEST_FAILED',
                                                    'REQUEST_PENDING',
                                                    'WEB_SCRAPE_PENDING'))
                    GROUP BY market_code, disco_code, ldc_account) e,
                   (SELECT row_id,
                           attrib_37     heating_fuel,
                           attrib_35     bus_type,
                           attrib_36     bus_subtype
                      FROM siebel.s_doc_quote_x@TPSBL) f
             WHERE     a.market_code = e.market_code
                  -- AND a.disco_code = e.disco_code
                   AND e.ldc_account = SUBSTR (a.ldc_account,
                                                       1,
                                                         INSTR (a.ldc_account,
                                                                '_',
                                                                1,
                                                                1)
                                                       - 1)  -- Matched by BA only
                   AND a.uid_account_raw = d.uid_account_raw
                   AND e.uid_alps_account = b.uid_alps_account
                   AND b.sbl_quote_id = c.sbl_quote_id
                   AND b.sbl_quote_id = f.row_id(+)
                   AND c.active_flg = 'Y'
                   AND c.status = 'PENDING'
                   AND (   b.sca_status IN ('REQUEST_COMPLETE',
                                            'DATA_ERROR',
                                            'REQUEST_FAILED',
                                            'REQUEST_PENDING',
                                            'WEB_SCRAPE_PENDING')
                        OR b.idr_status IN ('REQUEST_COMPLETE',
                                            'DATA_ERROR',
                                            'REQUEST_FAILED',
                                            'REQUEST_PENDING',
                                            'WEB_SCRAPE_PENDING'));

            --
            IF arr_pe_setup.COUNT > 0
            THEN
                DBMS_OUTPUT.put_line ('Matched by BA');
                --Populate PE staging tables
                gv_source_id := arr_pe_setup (1).meter_id;
                load_pe_tables (arr_pe_setup);
                p_out_status := 'BA';
                p_out_desc := NULL;
            ELSE
                -- Matched by MARKET/LDC  as the incoming accounts having DISCO diferrent than DISCO requested in MML.
                    gv_code_ln := $$plsql_line;

                    SELECT DISTINCT
                           e.uid_alps_account,
                           a.uid_account_raw,
                           p_market_code, --a.market_code,
                           p_disco_code,
                           p_ldc_account, --a.ldc_account,
                           NVL (
                               d.meter_type,
                               (SELECT DECODE (meter_type,
                                               'SCA', 'SCALAR',
                                               'IDR', 'IDR',
                                               meter_type)
                                  FROM alps_mml_pr_detail
                                 WHERE uid_alps_account = e.uid_alps_account)),
                    NVL( (SELECT a.customer_id
                         FROM alps_mml_pr_header a
                      WHERE uid_mml_master IN
                           (SELECT MAX (a.uid_mml_master)
                              FROM alps_mml_pr_header a, alps_mml_pr_detail b
                             WHERE     a.active_flg = 'Y'
                                   AND a.uid_mml_master = b.uid_mml_master
                                   AND b.ldc_account = p_ldc_account
                                   AND B.MARKET_CODE = p_market_code
                                   AND b.disco_code = p_disco_code)), (SELECT MAX(customer_id)
                                                                  FROM alps_mml_stg 
                                                                  WHERE market_code = p_market_code
                                                                    and disco_code = p_disco_code
                                                                    and ldc_account = p_ldc_account ) )
                   customer_id,  --70455
                              p_market_code
                           || '_'
                           || p_disco_code
                           || '_'
                           || p_ldc_account
                               meter_id,
                           c.lwsp_flag,
                           a.refresh_dt,
                           DECODE (
                               (SELECT COUNT (1)
                                  FROM alps_account_transaction
                                 WHERE     uid_alps_account =
                                           e.uid_alps_account
                                       AND transact_type = 'REQUEST_LPSS'),
                               0, DECODE (
                                      (SELECT lookup_str_value1
                                         FROM alps_mml_lookup
                                        WHERE     lookup_group =
                                                  'ADDR_ATTR_DISCO'
                                              AND lookup_code = e.disco_code),
                                      'Y', NVL (
                                               REPLACE (d.address1,
                                                        '''',
                                                        '"'),
                                               NVL (
                                                   (SELECT REPLACE (address1,
                                                                    '''',
                                                                    '"')
                                                      FROM alps_mml_pr_detail
                                                     WHERE uid_alps_account =
                                                           e.uid_alps_account),
                                                   'UNKNOWN')),
                                      NVL (REPLACE (b.address1, '''', '"'),
                                           'UNKNOWN')),
                               NVL (REPLACE (d.address1, '''', '"'),
                                    'UNKNOWN'))
                               address1,
                           DECODE (
                               (SELECT COUNT (1)
                                  FROM alps_account_transaction
                                 WHERE     uid_alps_account =
                                           e.uid_alps_account
                                       AND transact_type = 'REQUEST_LPSS'),
                               0, DECODE (
                                      (SELECT lookup_str_value1
                                         FROM alps_mml_lookup
                                        WHERE     lookup_group =
                                                  'ADDR_ATTR_DISCO'
                                              AND lookup_code = e.disco_code),
                                      'Y', NVL (
                                               REPLACE (d.address2,
                                                        '''',
                                                        '"'),
                                               NVL (
                                                   (SELECT REPLACE (address2,
                                                                    '''',
                                                                    '"')
                                                      FROM alps_mml_pr_detail
                                                     WHERE uid_alps_account =
                                                           e.uid_alps_account),
                                                   'UNKNOWN')),
                                      NVL (REPLACE (b.address2, '''', '"'),
                                           'UNKNOWN')),
                               NVL (REPLACE (d.address2, '''', '"'),
                                    'UNKNOWN'))
                               address2,
                           DECODE (
                               (SELECT COUNT (1)
                                  FROM alps_account_transaction
                                 WHERE     uid_alps_account =
                                           e.uid_alps_account
                                       AND transact_type = 'REQUEST_LPSS'),
                               0, DECODE (
                                      (SELECT lookup_str_value1
                                         FROM alps_mml_lookup
                                        WHERE     lookup_group =
                                                  'ADDR_ATTR_DISCO'
                                              AND lookup_code = e.disco_code),
                                      'Y', NVL (
                                               REPLACE (d.city, '''', '"'),
                                               NVL (
                                                   (SELECT REPLACE (city,
                                                                    '''',
                                                                    '"')
                                                      FROM alps_mml_pr_detail
                                                     WHERE uid_alps_account =
                                                           e.uid_alps_account),
                                                   'UNKNOWN')),
                                      NVL (REPLACE (b.city, '''', '"'),
                                           'UNKNOWN')),
                               NVL (REPLACE (d.city, '''', '"'), 'UNKNOWN'))
                               city,
                           NVL (
                               d.state,
                               (SELECT MAX (state)
                                  FROM alps_mml_pr_detail
                                 WHERE     market_code = p_market_code
                                       AND ldc_account = p_ldc_account
                                       AND (   sca_status IN
                                                   ('REQUEST_COMPLETE',
                                                    'DATA_ERROR',
                                                    'REQUEST_FAILED',
                                                    'REQUEST_PENDING',
                                                    'WEB_SCRAPE_PENDING')
                                            OR idr_status IN
                                                   ('REQUEST_COMPLETE',
                                                    'DATA_ERROR',
                                                    'REQUEST_FAILED',
                                                    'REQUEST_PENDING',
                                                    'WEB_SCRAPE_PENDING'))))
                               state,
                           DECODE (
                               (SELECT COUNT (1)
                                  FROM alps_account_transaction
                                 WHERE     uid_alps_account =
                                           e.uid_alps_account
                                       AND transact_type = 'REQUEST_LPSS'),
                               0, DECODE (
                                      (SELECT lookup_str_value1
                                         FROM alps_mml_lookup
                                        WHERE     lookup_group =
                                                  'ADDR_ATTR_DISCO'
                                              AND lookup_code = e.disco_code),
                                      'Y', NVL (
                                               SUBSTR (
                                                   REPLACE (TRIM (d.zip),
                                                            '-',
                                                            ''),
                                                   1,
                                                   5),
                                               NVL (
                                                   (SELECT SUBSTR (
                                                               REPLACE (
                                                                   TRIM (zip),
                                                                   '-',
                                                                   ''),
                                                               1,
                                                               5)
                                                      FROM alps_mml_pr_detail
                                                     WHERE uid_alps_account =
                                                           e.uid_alps_account),
                                                   '00000')),
                                      NVL (
                                          SUBSTR (
                                              REPLACE (TRIM (b.zip), '-', ''),
                                              1,
                                              5),
                                          '00000')),
                               NVL (
                                   SUBSTR (REPLACE (TRIM (d.zip), '-', ''),
                                           1,
                                           5),
                                   '00000'))
                               zip,
                           DECODE (
                               (SELECT COUNT (1)
                                  FROM alps_account_transaction
                                 WHERE     uid_alps_account =
                                           e.uid_alps_account
                                       AND transact_type = 'REQUEST_LPSS'),
                               0, DECODE (
                                      (SELECT lookup_str_value1
                                         FROM alps_mml_lookup
                                        WHERE     lookup_group =
                                                  'ADDR_ATTR_DISCO'
                                              AND lookup_code = e.disco_code),
                                      'Y', NVL (
                                               SUBSTR (
                                                   REPLACE (TRIM (d.zip),
                                                            '-',
                                                            ''),
                                                   -4),
                                               NVL (
                                                   (SELECT SUBSTR (
                                                               REPLACE (
                                                                   TRIM (zip),
                                                                   '-',
                                                                   ''),
                                                               -4)
                                                      FROM alps_mml_pr_detail
                                                     WHERE uid_alps_account =
                                                           e.uid_alps_account),
                                                   '0000')),
                                      NVL (
                                          SUBSTR (
                                              REPLACE (TRIM (b.zip), '-', ''),
                                              -4),
                                          '0000')),
                               NVL (
                                   SUBSTR (REPLACE (TRIM (d.zip), '-', ''),
                                           -4),
                                   '0000'))
                               zipplus4,
                           NVL (d.county, 'UNKNOWN'),
                           NVL (d.country, 'USA')
                               country,
                           d.rate_class,
                           d.rate_subclass,
                           d.loss_factor,
                           d.strata,
                           d.voltage,
                           d.load_profile,
                           d.meter_cycle,
                           d.mhp,
                           d.bus,
                           d.station_id,
                           DECODE (d.disco_code, 'FGE', 'WCMASS', d.zone),
                           NULL
                               status,
                           DECODE (
                               c.pr_trans_type,
                               'New', 'N',
                               DECODE (
                                   c.pr_trans_type,
                                   'Amendment - Add', 'N',
                                   DECODE (c.pr_trans_type,
                                           'Previously Lost', 'N',
                                           'Y')))
                               renewal_flag,
                           f.heating_fuel,
                           f.bus_type,
                           f.bus_subtype,
                           d.adv_meter_flag,
                           d.exception_assigned
                      BULK COLLECT INTO arr_pe_setup
                      FROM (  SELECT MAX (uid_account_raw)     uid_account_raw,
                                     MAX (updated_dt)          refresh_dt,
                                     market_code,
                                     ldc_account
                                FROM alps_account_raw
                               WHERE     market_code = p_market_code
                                     AND ldc_account = p_ldc_account
                            GROUP BY market_code, ldc_account) a,
                           alps_account_raw    d,
                           alps_mml_pr_header  c,
                           alps_mml_pr_detail  b,
                           (  SELECT MAX (uid_alps_account)
                                         uid_alps_account,
                                     market_code,
                                     disco_code,
                                     ldc_account
                                FROM alps_mml_pr_detail dtl
                               WHERE     market_code = p_market_code
                                     AND ldc_account = p_ldc_account
                                     AND (   sca_status IN
                                                 ('REQUEST_COMPLETE',
                                                  'DATA_ERROR',
                                                  'REQUEST_FAILED',
                                                  'REQUEST_PENDING',
                                                  'WEB_SCRAPE_PENDING')
                                          OR idr_status IN
                                                 ('REQUEST_COMPLETE',
                                                  'DATA_ERROR',
                                                  'REQUEST_FAILED',
                                                  'REQUEST_PENDING',
                                                  'WEB_SCRAPE_PENDING'))
                                     AND EXISTS
                                             (SELECT 1
                                                FROM alps_mml_pr_header hdr
                                               WHERE     hdr.sbl_quote_id =
                                                         dtl.sbl_quote_id
                                                     AND hdr.active_flg = 'Y'
                                                     AND hdr.status = 'PENDING')
                            GROUP BY market_code, disco_code, ldc_account) e,
                           (SELECT row_id,
                                   attrib_37     heating_fuel,
                                   attrib_35     bus_type,
                                   attrib_36     bus_subtype
                              FROM siebel.s_doc_quote_x@TPSBL) f
                     WHERE     a.market_code = b.market_code
                           AND a.ldc_account = b.ldc_account
                           AND a.uid_account_raw = d.uid_account_raw
                           AND e.disco_code <> p_disco_code
                           AND e.uid_alps_account = b.uid_alps_account
                           AND b.sbl_quote_id = c.sbl_quote_id
                           AND b.sbl_quote_id = f.row_id(+)
                           AND c.active_flg = 'Y'
                           AND c.status = 'PENDING'
                           AND (   b.sca_status IN ('REQUEST_COMPLETE',
                                                    'DATA_ERROR',
                                                    'REQUEST_FAILED',
                                                    'REQUEST_PENDING',
                                                    'WEB_SCRAPE_PENDING')
                                OR b.idr_status IN ('REQUEST_COMPLETE',
                                                    'DATA_ERROR',
                                                    'REQUEST_FAILED',
                                                    'REQUEST_PENDING',
                                                    'WEB_SCRAPE_PENDING'));

                --
                IF arr_pe_setup.COUNT > 0
                THEN
                    DBMS_OUTPUT.put_line ('Matched by MARKET_LDC');
                    --Populate PE staging tables
                    gv_source_id := arr_pe_setup (1).meter_id;
                    load_pe_tables (arr_pe_setup);

                    gv_code_ln := $$plsql_line;
                    FOR ix IN arr_pe_setup.FIRST .. arr_pe_setup.LAST
                    LOOP                                                    --
                        UPDATE alps_mml_pr_detail
                           SET meter_id = arr_pe_setup (ix).meter_id,
                               disco_code = p_disco_code
                         WHERE uid_alps_account =
                               arr_pe_setup (ix).uid_alps_account;
                    END LOOP;

                    --
                    p_out_status := 'MARKET_LDC';
                    p_out_desc := NULL;
                ELSE                                           
                    -- Brand new data to be load into ALPS for future utilization.
                    BEGIN
                        SELECT DISTINCT
                               NULL,
                               a.uid_account_raw,
                               a.market_code,
                               a.disco_code,
                               a.ldc_account,
                               NVL (
                                   d.meter_type,
                                   (SELECT DECODE (idr_meter,
                                                   'N', 'SCALAR',
                                                   'IDR')
                                      FROM alps_account_repository
                                     WHERE     market_code =
                                               a.market_code
                                           AND disco_code =
                                               a.disco_code
                                           AND ldc_account =
                                               a.ldc_account)),
                    NVL( (SELECT a.customer_id  
                         FROM alps_mml_pr_header a
                      WHERE uid_mml_master IN
                           (SELECT MAX (a.uid_mml_master)
                              FROM alps_mml_pr_header a, alps_mml_pr_detail b
                             WHERE     a.active_flg = 'Y'
                                   AND a.uid_mml_master = b.uid_mml_master
                                   AND b.ldc_account = p_ldc_account
                                   AND B.MARKET_CODE = p_market_code
                                   AND b.disco_code = p_disco_code)), (SELECT MAX(customer_id)
                                                                  FROM alps_mml_stg 
                                                                  WHERE market_code = p_market_code
                                                                    and disco_code = p_disco_code
                                                                    and ldc_account = p_ldc_account ) )
                   customer_id,   --70455
                                  p_market_code
                               || '_'
                               || p_disco_code
                               || '_'
                               || p_ldc_account
                                   meter_id,
                               NULL
                                   lwsp_flag,
                               d.updated_dt,
                               NVL (d.address1, 'UNKNOWN'),
                               d.address2,
                               NVL (d.city, 'UNKNOWN'),
                               d.state,
                               NVL (
                                   SUBSTR (
                                       REPLACE (TRIM (d.zip),
                                                '-',
                                                ''),
                                       1,
                                       5),
                                   '00000')
                                   zip,
                               NVL (
                                   SUBSTR (
                                       REPLACE (TRIM (d.zip),
                                                '-',
                                                ''),
                                       -4),
                                   '0000')
                                   zipplus4,
                               NVL (d.county, 'UNKNOWN'),
                               NVL (d.country, 'USA')
                                   country,
                               d.rate_class,
                               d.rate_subclass,
                               d.loss_factor,
                               d.strata,
                               d.voltage,
                               d.load_profile,
                               d.meter_cycle,
                               d.mhp,
                               d.bus,
                               d.station_id,
                               DECODE (d.disco_code,
                                       'FGE', 'WCMASS',
                                       d.zone),
                               NULL
                                   status,
                               NULL
                                   renewal_flag,
                               NULL
                                   heating_fuel,
                               NULL
                                   bus_type,
                               NULL
                                   bus_subtype,
                               NULL
                                   adv_meter_flg,
                               d.exception_assigned
                          BULK COLLECT INTO arr_pe_setup
                          FROM (  SELECT MAX (uid_account_raw)
                                             uid_account_raw,
                                         market_code,
                                         disco_code,
                                         ldc_account
                                    FROM alps_account_raw
                                   WHERE     market_code =
                                             p_market_code
                                         AND disco_code = p_disco_code
                                         AND ldc_account =
                                             p_ldc_account
                                         AND acct_status =
                                             'DATA_RECEIVED'
                                GROUP BY market_code,
                                         disco_code,
                                         ldc_account) a,
                               alps_account_raw  d
                         WHERE     a.uid_account_raw =
                                   d.uid_account_raw
                               AND d.acct_status = 'DATA_RECEIVED';

                        /*
                               AND NOT EXISTS(SELECT 1 FROM alps_account acct
                                                          WHERE acct.market_code =  a.market_code
                                                          AND acct.disco_code = a.disco_code
                                                          AND acct.ldc_account = a.ldc_account)
                               AND NOT EXISTS(SELECT 1 FROM alps_mml_pr_detail  dtl
                                                          WHERE dtl.market_code =  a.market_code
                                                          AND dtl.disco_code = a.disco_code
                                                          AND dtl.ldc_account = a.ldc_account);
                       */

                        --Populate PE staging tables
                        IF arr_pe_setup.COUNT > 0
                        THEN
                            DBMS_OUTPUT.put_line ( 'No match - New data');
                            gv_source_id := arr_pe_setup (1).meter_id;
                            load_pe_tables (arr_pe_setup);
                            p_out_status := NULL;
                            p_out_desc := NULL;
                        ELSE
                            p_out_status := NULL;
                            p_out_desc := NULL;
                        END IF;
                    EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                            p_out_status := NULL;
                            p_out_desc := NULL;
                            NULL;
                    END;

                END IF;
            END IF;
        END IF;

        --
        arr_attr_raw.delete;
        arr_usg_raw.delete;
        arr_pe_setup.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            arr_attr_raw.delete;
            arr_usg_raw.delete;
            arr_pe_setup.delete;
            p_out_status := 'ERROR';
            RAISE;
    END insert_pe_staging;
    
    PROCEDURE insert_vee_mgt_pr (p_prnumber     IN     VARCHAR2,
                                 p_revision     IN     VARCHAR2,
                                 p_out_status      OUT VARCHAR2,
                                 p_out_desc        OUT VARCHAR2)
    AS
        /*
                Purpose : This procedure setup accounts to be VEE(generating forecast) at the PR level

                Parameters:
                                    p_prnumber -  Market of an account
                                    p_revision -  DIsco of an account
                                    p_out_status - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                                    p_out_description - Description of the function call returned to calling program(process).

                Execution : Called from SIEBEL UI
            */
        lv_pr_cnt   INTEGER := 0;
    BEGIN
        gv_code_proc := 'INSERT_VEE_MGT_PR';
        gv_source_type := 'PR_NUMBER';
        gv_stage := 'PE_SETUP';
        gv_source_id := p_prnumber;
        --
        gv_code_ln := $$plsql_line;

        -- CHeck to see if the PR is currently being VEE or about to be VEE.
        SELECT COUNT (1)
          INTO lv_pr_cnt
          FROM alps_vee_mgt
         WHERE     pr_num = p_prnumber
               AND revision = p_revision
               AND status IN ('READY',
                              'PRIORITY',
                              'WAITING',
                              'FORECASTING IN PROGRESS');

        --
        gv_code_ln := $$plsql_line;

        -- New account, setup for VEE
        IF lv_pr_cnt = 0
        THEN
            INSERT INTO alps_vee_mgt (pr_num, revision, dbname)
                     VALUES (
                                p_prnumber,
                                p_revision,
                                (SELECT SUBSTR (db_link,
                                                1,
                                                INSTR (db_link, '.', 1) - 1)    dbname
                                   FROM all_db_links
                                  WHERE db_link LIKE '%PE%'));
        END IF;

        --
        COMMIT;
        p_out_status := 'SUCCESS';
        p_out_desc := NULL;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- Set up the error message to be return to calling App.
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            --
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
    END insert_vee_mgt_pr;

    PROCEDURE insert_vee_mgt (p_meter_id IN VARCHAR2)
    AS
        /*
                Purpose : This procedure setup accounts to be VEE(generating forecast) at the meter level

                Parameters:
                                    p_meter_id -  meter id

                Execution : Called from external application
            */
        lv_pr_cnt   INTEGER := 0;
    BEGIN
        gv_code_proc := 'INSERT_VEE_MGT';
        gv_code_ln := $$plsql_line;

        SELECT COUNT (1)
          INTO lv_pr_cnt
          FROM alps_vee_mgt
         WHERE     meterid = p_meter_id
               AND status IN ('READY',
                              'PRIORITY',
                              'WAITING',
                              'FORECASTING IN PROGRESS');

        --
        IF lv_pr_cnt = 0
        THEN
            --
            gv_code_ln := $$plsql_line;

            SELECT *
              BULK COLLECT INTO arr_vee_accts
              FROM v_alps_vee_acct_setup
             WHERE meterid = p_meter_id;

            --
            gv_code_ln := $$plsql_line;

            IF arr_vee_accts.COUNT > 0
            THEN
                FOR ix IN arr_vee_accts.FIRST .. arr_vee_accts.LAST
                LOOP
                    INSERT INTO alps_vee_mgt (pr_num,
                                              revision,
                                              meterid,
                                              dbname,
                                              createddate)
                             VALUES (
                                        arr_vee_accts (ix).prnumber,
                                        arr_vee_accts (ix).revision,
                                        arr_vee_accts (ix).meterid,
                                        arr_vee_accts (ix).dbname,
                                        TO_DATE (
                                            TO_CHAR (SYSDATE,
                                                     'DD-MON-YYYY HH:MI AM'),
                                            'DD-MON-YYYY HH:MI AM'));
                END LOOP;
            END IF;
        END IF;

        --
        COMMIT;
        arr_vee_accts.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            arr_vee_accts.delete;
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            RAISE;
            ROLLBACK;
    END insert_vee_mgt;

    PROCEDURE insert_vee_mgmt (p_meter_id   IN VARCHAR2,
                               p_quote_id   IN VARCHAR2)
    AS
        /*
                Purpose : This procedure setup accounts to be VEE(generating forecast) at the meter level

                Parameters:
                                    p_meter_id -  meter id
                                    p_quote_id  - Siebel Quote_id

                Execution : Called from external application
            */
        lv_pr_cnt   INTEGER := 0;
    BEGIN
        gv_code_proc := 'INSERT_VEE_MGMT';
        --
        gv_code_ln := $$plsql_line;

        SELECT COUNT (1)
          INTO lv_pr_cnt
          FROM alps_vee_mgt
         WHERE     meterid = p_meter_id
               AND status IN ('READY',
                              'PRIORITY',
                              'WAITING',
                              'FORECASTING IN PROGRESS');

        --
        IF lv_pr_cnt = 0
        THEN
            --
            gv_code_ln := $$plsql_line;

            SELECT *
              BULK COLLECT INTO arr_vee_accts
              FROM (SELECT a.prnumber,
                           a.revision,
                           SUBSTR (
                               NVL (
                                   c.meter_id,
                                      c.market_code
                                   || '_'
                                   || c.disco_code
                                   || '_'
                                   || c.ldc_account),
                               1,
                               64),
                           e.dbname
                      FROM alps_account  c,
                           (SELECT DISTINCT prnumber, revision
                              FROM alps_mml_pr_header
                             WHERE sbl_quote_id = p_quote_id) a,
                           (SELECT SUBSTR (db_link,
                                           1,
                                           INSTR (db_link, '.', 1) - 1)    dbname
                              FROM all_db_links
                             WHERE db_link LIKE '%PE%') e
                     WHERE     c.ldc_account = SUBSTR (p_meter_id,
                                                         INSTR (p_meter_id,
                                                                '_',
                                                                1,
                                                                2)
                                                       + 1)
                           AND c.disco_code =
                               SUBSTR (p_meter_id,
                                         INSTR (p_meter_id,
                                                '_',
                                                1,
                                                1)
                                       + 1,
                                       (  (  INSTR (p_meter_id,
                                                    '_',
                                                    1,
                                                    2)
                                           - 1)
                                        - INSTR (p_meter_id,
                                                 '_',
                                                 1,
                                                 1)))
                           AND c.market_code = SUBSTR (p_meter_id,
                                                       1,
                                                         INSTR (p_meter_id,
                                                                '_',
                                                                1,
                                                                1)
                                                       - 1));

            --
            gv_code_ln := $$plsql_line;

            IF arr_vee_accts.COUNT > 0
            THEN
                FOR ix IN arr_vee_accts.FIRST .. arr_vee_accts.LAST
                LOOP
                    INSERT INTO alps_vee_mgt (pr_num,
                                              revision,
                                              meterid,
                                              dbname,
                                              createddate)
                             VALUES (
                                        arr_vee_accts (ix).prnumber,
                                        arr_vee_accts (ix).revision,
                                        arr_vee_accts (ix).meterid,
                                        arr_vee_accts (ix).dbname,
                                        TO_DATE (
                                            TO_CHAR (SYSDATE,
                                                     'DD-MON-YYYY HH:MI AM'),
                                            'DD-MON-YYYY HH:MI AM'));
                END LOOP;
            END IF;
        END IF;

        --
        COMMIT;
        arr_vee_accts.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            arr_vee_accts.delete;
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            RAISE;
            ROLLBACK;
    END insert_vee_mgmt;

    FUNCTION f_acct_already_vee (p_meter_id IN VARCHAR2, p_setup_dt OUT DATE)
        RETURN VARCHAR2
    AS
        /*
                Purpose : This function check VEE data is up-to-date (< 120 days old)

                Parameters:
                                    p_meter_id -  meter id

                Execution : Called from external application
            */
        lv_cnt        VARCHAR2 (3);
        vVeeLimit     INTEGER;
        vTagStartDt   DATE;
        vTagStopDt    DATE;
        vMaxReadDt    DATE;
    BEGIN
        gv_code_proc := 'F_ACCT_ALREADY_VEE';
        --
        DBMS_OUTPUT.put_line (   'MARKET  ==> '
                              || SUBSTR (p_meter_id,
                                         1,
                                           INSTR (p_meter_id,
                                                  '_',
                                                  1,
                                                  1)
                                         - 1));
        gv_code_ln := $$plsql_line;

        -- Get PE threshold date
        SELECT lookup_num_value1
          INTO vVeeLimit
          FROM alps_mml_lookup
         WHERE lookup_group = 'DATA_AGING_LIMIT' AND lookup_code = 'VEE_DATA';

        DBMS_OUTPUT.put_line ('vVeeLimit) ==> ' || (vVeeLimit));

        -- Get the Current ICAP start date
        BEGIN
            SELECT alps_parser_pkg.f_get_planning_period_date (
                       'CAPACITY',
                       SUBSTR (p_meter_id,
                               1,
                                 INSTR (p_meter_id,
                                        '_',
                                        1,
                                        1)
                               - 1),
                       'START')
              INTO vTagStartDt
              FROM alps_mml_lookup
             WHERE     lookup_code LIKE 'MARKET_CAPACITY%'
                   AND lookup_str_value1 = SUBSTR (p_meter_id,
                                                   1,
                                                     INSTR (p_meter_id,
                                                            '_',
                                                            1,
                                                            1)
                                                   - 1);

            --
            DBMS_OUTPUT.put_line ('vTagStartDt) ==> ' || (vTagStartDt));
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                vTagStartDt := NULL;
        END;

        -- get the PE set up date
        SELECT MAX (ovrd.lstime)
          INTO p_setup_dt
          FROM serviceplan@TPpe       serv,
               account@TPpe           act,
               acctservicehist@TPpe   hist,
               acctoverridehist@TPpe  ovrd
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND act.uidaccount = ovrd.uidaccount
               AND ovrd.overridecode = 'VEE_COMPLETE_OVRD'
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        DBMS_OUTPUT.put_line ('p_setup_dt) ==> ' || (p_setup_dt));

        -- Get maximum meter read date
        SELECT MAX (meterread.STOPREADTIME)
          INTO vMaxReadDt
          FROM serviceplan@TPpe           serv,
               account@TPpe               act,
               acctservicehist@TPpe       hist,
               pwrline.meterread@TPpe     meterread,
               pwrline.meter@TPpe         meter,
               pwrline.meterhistory@TPpe  meterhist
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND act.uidaccount = meterhist.uidaccount
               AND meterread.uidmeter(+) = meterhist.uidmeter
               AND meter.uidmeter = meterhist.uidmeter
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        DBMS_OUTPUT.put_line ('vMaxReadDt) ==> ' || (vMaxReadDt));

        --
        IF p_setup_dt IS NOT NULL
        THEN
            IF    (    (SYSDATE - vMaxReadDt) < vVeeLimit
                   AND SUBSTR (p_meter_id,
                               1,
                                 INSTR (p_meter_id,
                                        '_',
                                        1,
                                        1)
                               - 1) = 'ERCOT')
               OR (    (SYSDATE - vMaxReadDt) < vVeeLimit
                   AND p_setup_dt >= vTagStartDt
                   AND SYSDATE >= vTagStartDt
                   AND SUBSTR (p_meter_id,
                               1,
                                 INSTR (p_meter_id,
                                        '_',
                                        1,
                                        1)
                               - 1) <> 'ERCOT')
            THEN
                RETURN ('YES');
            ELSE
                RETURN ('NO');
            END IF;
        ELSE
            RETURN ('NO');
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            RETURN ('NO');
    END;


 FUNCTION f_acct_already_vee_exp (p_meter_id IN VARCHAR2, p_setup_dt OUT DATE)
        RETURN VARCHAR2
    AS
        /*
                Purpose : This function check VEE data is up-to-date (< 120 days old)

                Parameters:
                                    p_meter_id -  meter id

                Execution : Called from external application
            */
        lv_cnt        VARCHAR2 (3);
        vVeeLimit     INTEGER;
        vTagStartDt   DATE;
        vTagStopDt    DATE;
        vMaxReadDt    DATE;
    BEGIN
        gv_code_proc := 'F_ACCT_ALREADY_VEE_EXP';
        --
        DBMS_OUTPUT.put_line (   'MARKET  ==> '
                              || SUBSTR (p_meter_id,
                                         1,
                                           INSTR (p_meter_id,
                                                  '_',
                                                  1,
                                                  1)
                                         - 1));
        gv_code_ln := $$plsql_line;

        -- Get PE threshold date
        SELECT lookup_num_value1
          INTO vVeeLimit
          FROM alps_mml_lookup
         WHERE lookup_group = 'DATA_AGING_LIMIT' AND lookup_code = 'VEE_DATA';

        DBMS_OUTPUT.put_line ('vVeeLimit) ==> ' || (vVeeLimit));

        -- Get the Current ICAP start date
        BEGIN
            SELECT alps_parser_pkg.f_get_planning_period_date (
                       'CAPACITY',
                       SUBSTR (p_meter_id,
                               1,
                                 INSTR (p_meter_id,
                                        '_',
                                        1,
                                        1)
                               - 1),
                       'START')
              INTO vTagStartDt
              FROM alps_mml_lookup
             WHERE     lookup_code LIKE 'MARKET_CAPACITY%'
                   AND lookup_str_value1 = SUBSTR (p_meter_id,
                                                   1,
                                                     INSTR (p_meter_id,
                                                            '_',
                                                            1,
                                                            1)
                                                   - 1);

            --
            DBMS_OUTPUT.put_line ('vTagStartDt) ==> ' || (vTagStartDt));
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                vTagStartDt := NULL;
        END;

        -- get the PE set up date
        SELECT MAX (ovrd.lstime)
          INTO p_setup_dt
          FROM serviceplan@TPpe       serv,
               account@TPpe           act,
               acctservicehist@TPpe   hist,
               acctoverridehist@TPpe  ovrd
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND act.uidaccount = ovrd.uidaccount
               AND ovrd.overridecode = 'VEE_COMPLETE_OVRD'
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        DBMS_OUTPUT.put_line ('p_setup_dt) ==> ' || (p_setup_dt));  

        --
        IF p_setup_dt IS NOT NULL
        THEN
            IF    ( (SYSDATE - p_setup_dt) < vVeeLimit
                   AND SUBSTR (p_meter_id,
                               1,
                                 INSTR (p_meter_id,
                                        '_',
                                        1,
                                        1)
                               - 1) = 'ERCOT')
               OR (    (SYSDATE - p_setup_dt) < vVeeLimit
                   AND p_setup_dt >= vTagStartDt
                   AND SYSDATE >= vTagStartDt
                   AND SUBSTR (p_meter_id,
                               1,
                                 INSTR (p_meter_id,
                                        '_',
                                        1,
                                        1)
                               - 1) <> 'ERCOT')
            THEN
                RETURN ('YES');
            ELSE
                RETURN ('NO');
            END IF;
        ELSE
            RETURN ('NO');
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            RETURN ('NO');
    END;

    FUNCTION f_acct_already_pe (p_meter_id     IN     VARCHAR2,
                                p_status_ind   IN     VARCHAR2,
                                p_setup_dt        OUT DATE)
        RETURN VARCHAR2
    AS
        /*
                Purpose : This function check PE Set up data is up-to-date (< 120 days old)

                Parameters:
                                    p_meter_id -  meter id

                Execution : Called from table trigger : ALPS.T_ALPS_PR_DETAIL_BIU
            */
        lv_cnt        VARCHAR2 (3);
        vTagStartDt   DATE;
        vMaxReadDt    DATE;
        vPeLimitDt    INTEGER;
    BEGIN
        gv_code_proc := 'F_ACCT_ALREADY_PE';
        DBMS_OUTPUT.put_line (   'MARKET  ==> '
                              || SUBSTR (p_meter_id,
                                         1,
                                           INSTR (p_meter_id,
                                                  '_',
                                                  1,
                                                  1)
                                         - 1));

        --
        IF p_status_ind = 'IDR'
        THEN
            gv_code_ln := $$plsql_line;

            SELECT MAX (chnlcuttimestamp)
              INTO p_setup_dt
              FROM (SELECT a.uidchannel,
                           a.uidrecorder,
                           b.recorderid,
                           a.lstime,
                           c.chnlcuttimestamp
                      FROM channel@TPpe             a,
                           recorder@TPpe            b,
                           lschannelcutheader@TPpe  c
                     WHERE     a.uidrecorder = b.uidrecorder
                           AND a.uidchannel = c.uidchannel(+)
                           AND b.recorderid = p_meter_id
                           AND a.channelnum = 1
                           AND SYSDATE - a.lstime <
                               (SELECT lookup_num_value1
                                  FROM alps_mml_lookup
                                 WHERE     lookup_group = 'DATA_AGING_LIMIT'
                                       AND lookup_code = 'PE_DATA'));

            --
            IF p_setup_dt IS NOT NULL
            THEN
                RETURN ('YES');
            ELSE
                RETURN ('NO');
            END IF;
        ELSE
            -- Get PE threshold date
            SELECT lookup_num_value1
              INTO vPeLimitDt
              FROM alps_mml_lookup
             WHERE     lookup_group = 'DATA_AGING_LIMIT'
                   AND lookup_code = 'PE_DATA';

            DBMS_OUTPUT.put_line ('vPeLimitDt) ==> ' || (vPeLimitDt));

            -- Get the Current ICAP start date
            BEGIN
                SELECT alps_parser_pkg.f_get_planning_period_date (
                           'CAPACITY',
                           SUBSTR (p_meter_id,
                                   1,
                                     INSTR (p_meter_id,
                                            '_',
                                            1,
                                            1)
                                   - 1),
                           'START')
                  INTO vTagStartDt
                  FROM alps_mml_lookup
                 WHERE     lookup_code LIKE 'MARKET_CAPACITY%'
                       AND lookup_str_value1 = SUBSTR (p_meter_id,
                                                       1,
                                                         INSTR (p_meter_id,
                                                                '_',
                                                                1,
                                                                1)
                                                       - 1);
            EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                    vTagStartDt := NULL;
            END;

            DBMS_OUTPUT.put_line ('vTagStartDt) ==> ' || (vTagStartDt));

            --
            SELECT MAX (hist.lstime), MAX (meterread.STOPREADTIME)
              INTO p_setup_dt, vMaxReadDt
              FROM serviceplan@TPpe           serv,
                   account@TPpe               act,
                   acctservicehist@TPpe       hist,
                   pwrline.meterread@TPpe     meterread,
                   pwrline.meter@TPpe         meter,
                   pwrline.meterhistory@TPpe  meterhist
             WHERE     act.uidaccount = serv.uidaccount
                   AND act.uidaccount = hist.uidaccount
                   AND act.uidaccount = meterhist.uidaccount
                   AND meterread.uidmeter(+) = meterhist.uidmeter
                   AND meter.uidmeter = meterhist.uidmeter
                   AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                     INSTR (p_meter_id,
                                                            '_',
                                                            1,
                                                            2)
                                                   + 1)
                   AND hist.marketcode = SUBSTR (p_meter_id,
                                                 1,
                                                   INSTR (p_meter_id,
                                                          '_',
                                                          1,
                                                          1)
                                                 - 1)
                   AND hist.discocode = SUBSTR (p_meter_id,
                                                  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         1)
                                                + 1,
                                                (  (  INSTR (p_meter_id,
                                                             '_',
                                                             1,
                                                             2)
                                                    - 1)
                                                 - INSTR (p_meter_id,
                                                          '_',
                                                          1,
                                                          1)));

            DBMS_OUTPUT.put_line ('p_setup_dt) ==> ' || (p_setup_dt));
            DBMS_OUTPUT.put_line ('vMaxReadDt) ==> ' || (vMaxReadDt));

            --

            IF p_setup_dt IS NOT NULL
            THEN
                IF    (    (SYSDATE - vMaxReadDt) < vPeLimitDt
                       AND SUBSTR (p_meter_id,
                                   1,
                                     INSTR (p_meter_id,
                                            '_',
                                            1,
                                            1)
                                   - 1) = 'ERCOT')
                   OR (    (SYSDATE - vMaxReadDt) < vPeLimitDt
                       AND p_setup_dt >= vTagStartDt
                       AND SYSDATE >= vTagStartDt
                       AND SUBSTR (p_meter_id,
                                   1,
                                     INSTR (p_meter_id,
                                            '_',
                                            1,
                                            1)
                                   - 1) <> 'ERCOT')
                THEN
                    RETURN ('YES');
                ELSE
                    RETURN ('NO');
                END IF;
            ELSE
                RETURN ('NO');
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
    END;

    PROCEDURE update_pe_setup_accts (p_meter_id      IN VARCHAR2,
                                     p_idr_sca       IN VARCHAR2,
                                     p_status        IN VARCHAR2,
                                     p_description   IN VARCHAR2)
    AS
        /*
                Purpose : This procedure perform following functions:
                                1. Update each meter individual status to reflect latest PE setup status.
                                2. Mark meter ready for VEE if IDR/SCA successfully set up in PE
                                3. Update PR status

                Parameters:
                                    p_meter_id -  meter id
                                    p_idr_sca  - Indicate whether to update IDR_STATUS or SCA_STATUS. Values are IDR, SCA
                                    p_status  -  Current PE set up status . Values are PE_SETUP_COMPLETE, PE_SETUP_FAILED
                                    p_description - Detail description

                Execution : Called from external application
            */
        lv_where              VARCHAR2 (500);
        lv_pr_status          VARCHAR2 (500);
        lv_set_status         VARCHAR2 (500);
        lv_update             VARCHAR2 (1000);
        lv_max_pe_dt          DATE;
        lv_uid_alps_account   alps_mml_pr_detail.uid_alps_account%TYPE;
        lv_status             alps_mml_pr_detail.status%TYPE;
        lv_setup_dt           DATE;
    BEGIN
        gv_code_proc := 'UPDATE_PE_SETUP_ACCTS';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        gv_stage := 'PE_SETUP';

        --Get the MAX  PE setup date for this meter
        gv_code_ln := $$plsql_line;

        SELECT MAX (hist.lstime)
          INTO lv_max_pe_dt
          FROM serviceplan@TPpe      serv,
               account@TPpe          act,
               acctservicehist@TPpe  hist
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        --
        CASE p_idr_sca
            WHEN 'IDR'
            THEN
                gv_code_ln := $$plsql_line;

                   UPDATE alps_mml_pr_detail dtl
                      SET dtl.idr_status = p_status,
                          dtl.stage = gv_stage,
                          dtl.description = p_description,
                          dtl.last_pe_setup_dt = lv_max_pe_dt -- DECODE(p_status,'PE_SETUP_COMPLETE',SYSDATE,NULL)
                    WHERE     dtl.meter_id = p_meter_id
                          AND dtl.idr_status IN
                                  ('DATA_RECEIVED', 'PE_SETUP_FAILED')
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_mml_pr_header hdr
                                    WHERE     dtl.uid_mml_master =
                                              hdr.uid_mml_master
                                          AND dtl.sbl_quote_id =
                                              hdr.sbl_quote_id
                                          AND hdr.active_flg = 'Y'
                                          AND hdr.status = 'PENDING')
                RETURNING uid_alps_account,
                          meter_id,
                          status,
                          sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct,
                          arr_meter_id,
                          arr_acct_status,
                          arr_quote_id;
            --
            WHEN 'SCA'
            THEN
                gv_code_ln := $$plsql_line;

                   UPDATE alps_mml_pr_detail dtl
                      SET dtl.sca_status = p_status,
                          dtl.stage = gv_stage,
                          dtl.description = p_description,
                          dtl.last_pe_setup_dt = lv_max_pe_dt -- DECODE(p_status,'PE_SETUP_COMPLETE',SYSDATE,NULL)
                    WHERE     dtl.meter_id = p_meter_id
                          AND dtl.sca_status IN
                                  ('DATA_RECEIVED', 'PE_SETUP_FAILED')
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_mml_pr_header hdr
                                    WHERE     dtl.uid_mml_master =
                                              hdr.uid_mml_master
                                          AND dtl.sbl_quote_id =
                                              hdr.sbl_quote_id
                                          AND hdr.active_flg = 'Y'
                                          AND hdr.status = 'PENDING')
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_account act
                                    WHERE     act.market_code = dtl.market_code
                                          AND act.disco_code = dtl.disco_code
                                          AND act.ldc_account = dtl.ldc_account)
                RETURNING uid_alps_account,
                          meter_id,
                          status,
                          sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct,
                          arr_meter_id,
                          arr_acct_status,
                          arr_quote_id;
            ELSE
                gv_code_ln := $$plsql_line;

                   UPDATE alps_mml_pr_detail dtl
                      SET dtl.idr_status = p_status,
                          dtl.sca_status = p_status,
                          dtl.stage = gv_stage,
                          dtl.description = p_description,
                          dtl.last_pe_setup_dt = lv_max_pe_dt -- DECODE(p_status,'PE_SETUP_COMPLETE',SYSDATE,NULL)
                    WHERE     dtl.meter_id = p_meter_id
                          AND (   dtl.idr_status IN
                                      ('DATA_RECEIVED', 'PE_SETUP_FAILED')
                               OR dtl.sca_status IN
                                      ('DATA_RECEIVED', 'PE_SETUP_FAILED'))
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_mml_pr_header hdr
                                    WHERE     dtl.uid_mml_master =
                                              hdr.uid_mml_master
                                          AND dtl.sbl_quote_id =
                                              hdr.sbl_quote_id
                                          AND hdr.active_flg = 'Y'
                                          AND hdr.status = 'PENDING')
                RETURNING uid_alps_account,
                          meter_id,
                          status,
                          sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct,
                          arr_meter_id,
                          arr_acct_status,
                          arr_quote_id;
        END CASE;

        --
        gv_code_ln := $$plsql_line;

        IF arr_uid_acct.COUNT > 0
        THEN
            FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
            LOOP
                -- Create account transactions
                gv_code_ln := $$plsql_line;
                arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                arr_acct_txn (1).status := p_status;
                arr_acct_txn (1).description := p_description;
                --
                gv_code_ln := $$plsql_line;

                CASE p_idr_sca
                    WHEN 'IDR'
                    THEN
                        arr_acct_txn (1).txn_indicator := 'HI';
                        alps_request_pkg.setup_acct_transact (gv_source_id,
                                                              arr_acct_txn);
                    WHEN 'SCA'
                    THEN
                        arr_acct_txn (1).txn_indicator := 'HU';
                        alps_request_pkg.setup_acct_transact (gv_source_id,
                                                              arr_acct_txn);
                END CASE;
            END LOOP;
        END IF;

        --
        IF arr_acct_status.COUNT > 0
        THEN
            FOR ix IN arr_acct_status.FIRST .. arr_acct_status.LAST
            LOOP
                -- Perform VEE  check if account is ready to be VEE
                IF arr_acct_status (ix) = 'READY_FOR_VEE'
                THEN
                    -- Verfy in PE if Account been VEE recently
                    gv_code_ln := $$plsql_line;

                    IF f_acct_already_vee (arr_meter_id (ix), lv_setup_dt) =
                       'YES'
                    THEN
                        -- If acct been VEE recently set it to VEE_COMPLETE
                        gv_code_ln := $$plsql_line;

                        UPDATE alps_mml_pr_detail dtl
                           SET dtl.idr_status = 'VEE_COMPLETE',
                               dtl.description =
                                   'Account was VEE_COMPLETE recently ',
                               last_vee_completed_dt = lv_setup_dt
                         WHERE     dtl.meter_id = arr_meter_id (ix)
                               AND EXISTS
                                       (SELECT 1
                                          FROM alps_mml_pr_header hdr
                                         WHERE     dtl.uid_mml_master =
                                                   hdr.uid_mml_master
                                               AND dtl.sbl_quote_id =
                                                   hdr.sbl_quote_id
                                               AND hdr.active_flg = 'Y'
                                               AND hdr.status = 'PENDING');

                        -- Create account transactions
                        gv_code_ln := $$plsql_line;
                        arr_acct_txn (1).uid_alps_account :=
                            arr_uid_acct (ix);
                        arr_acct_txn (1).status := 'VEE_COMPLETE';
                        arr_acct_txn (1).description :=
                            'Account was VEE_COMPLETE recently ';
                        --
                        gv_code_ln := $$plsql_line;

                        CASE p_idr_sca
                            WHEN 'IDR'
                            THEN
                                arr_acct_txn (1).txn_indicator := 'HI';
                                alps_request_pkg.setup_acct_transact (
                                    gv_source_id,
                                    arr_acct_txn);
                            WHEN 'SCA'
                            THEN
                                arr_acct_txn (1).txn_indicator := 'HU';
                                alps_request_pkg.setup_acct_transact (
                                    gv_source_id,
                                    arr_acct_txn);
                        END CASE;
                    ELSE
                        insert_vee_mgt (arr_meter_id (ix));
                    END IF;
                END IF;

                -- Update PR status to reflect overall current staus of all accounts
                gv_code_ln := $$plsql_line;

                IF arr_quote_id.COUNT > 0
                THEN
                    FOR ix IN arr_quote_id.FIRST .. arr_quote_id.LAST
                    LOOP
                        update_pr_status (arr_quote_id (ix), gv_stage);
                    END LOOP;
                END IF;
            END LOOP;
        END IF;

        COMMIT;
        --
        arr_meter_id.delete;
        arr_acct_status.delete;
        arr_quote_id.delete;
        arr_uid_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- Set up the error message to be return to calling App.
            arr_meter_id.delete;
            arr_acct_status.delete;
            arr_quote_id.delete;
            arr_uid_acct.delete;
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_pe_setup_accts;

    PROCEDURE update_pe_setup_accounts (p_meter_id      IN     VARCHAR2,
                                        p_idr_sca       IN     VARCHAR2,
                                        p_status        IN     VARCHAR2,
                                        p_description   IN     VARCHAR2,
                                        p_out_status       OUT VARCHAR2,
                                        p_out_desc         OUT VARCHAR2)
    AS
        /*
                Purpose : This procedure perform following functions:
                                1. Update each meter individual status to reflect latest PE setup status.
                                2. Mark meter ready for VEE if IDR/SCA successfully set up in PE
                                3. Update PR status

                Parameters:
                                    p_meter_id -  meter id
                                    p_idr_sca  - Indicate whether to update IDR_STATUS or SCA_STATUS. Values are IDR, SCA
                                    p_status  -  Current PE set up status . Values are PE_SETUP_COMPLETE, PE_SETUP_FAILED
                                    p_description - Detail description

                Execution : Called from external application
            */
        lv_where              VARCHAR2 (500);
        lv_pr_status          VARCHAR2 (500);
        lv_set_status         VARCHAR2 (500);
        lv_update             VARCHAR2 (1000);
        lv_max_pe_dt          DATE;
        lv_uid_alps_account   alps_mml_pr_detail.uid_alps_account%TYPE;
        lv_status             alps_mml_pr_detail.status%TYPE;

        lv_description        VARCHAR2 (2000);
    BEGIN
        gv_code_proc := 'UPDATE_PE_SETUP_ACCOUNTS';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        gv_stage := 'PE_SETUP';

        --Get the MAX  PE setup date for this meter
        gv_code_ln := $$plsql_line;

        SELECT MAX (hist.lstime)
          INTO lv_max_pe_dt
          FROM serviceplan@TPpe      serv,
               account@TPpe          act,
               acctservicehist@TPpe  hist
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        lv_description := SUBSTR (p_description, 1, 2000);

        --
        CASE p_idr_sca
            WHEN 'IDR'
            THEN
                gv_code_ln := $$plsql_line;

                   UPDATE alps_mml_pr_detail dtl
                      SET dtl.idr_status = p_status,
                          dtl.stage = gv_stage,
                          dtl.description = lv_description,
                          dtl.last_pe_setup_dt = lv_max_pe_dt -- DECODE(p_status,'PE_SETUP_COMPLETE',SYSDATE,NULL)
                    WHERE     dtl.meter_id = p_meter_id
                          AND dtl.idr_status IN
                                  ('DATA_RECEIVED', 'PE_SETUP_FAILED')
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_mml_pr_header hdr
                                    WHERE     dtl.uid_mml_master =
                                              hdr.uid_mml_master
                                          AND dtl.sbl_quote_id =
                                              hdr.sbl_quote_id
                                          AND hdr.active_flg = 'Y'
                                          AND hdr.status = 'PENDING')
                RETURNING uid_alps_account,
                          meter_id,
                          status,
                          sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct,
                          arr_meter_id,
                          arr_acct_status,
                          arr_quote_id;
            --  RETURNING  sbl_quote_id  BULK COLLECT INTO  arr_quote_id;
            --
            WHEN 'SCA'
            THEN
                gv_code_ln := $$plsql_line;

                   UPDATE alps_mml_pr_detail dtl
                      SET dtl.sca_status = p_status,
                          dtl.stage = gv_stage,
                          dtl.description = lv_description,
                          dtl.last_pe_setup_dt = lv_max_pe_dt -- DECODE(p_status,'PE_SETUP_COMPLETE',SYSDATE,NULL)
                    WHERE     dtl.meter_id = p_meter_id
                          AND dtl.sca_status IN
                                  ('DATA_RECEIVED', 'PE_SETUP_FAILED')
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_mml_pr_header hdr
                                    WHERE     dtl.uid_mml_master =
                                              hdr.uid_mml_master
                                          AND dtl.sbl_quote_id =
                                              hdr.sbl_quote_id
                                          AND hdr.active_flg = 'Y'
                                          AND hdr.status = 'PENDING')
                RETURNING uid_alps_account,
                          meter_id,
                          status,
                          sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct,
                          arr_meter_id,
                          arr_acct_status,
                          arr_quote_id;
            --  AND EXISTS (SELECT 1 FROM alps_account act
            --                       WHERE act.market_code =dtl.market_code
            --                     AND act.disco_code = dtl.disco_code
            --                   AND act.ldc_account = dtl.ldc_account)  RETURNING  uid_alps_account,meter_id, status, sbl_quote_id  BULK COLLECT INTO  arr_uid_acct,arr_meter_id, arr_acct_status, arr_quote_id;

            ELSE
                gv_code_ln := $$plsql_line;

                   UPDATE alps_mml_pr_detail dtl
                      SET dtl.idr_status = p_status,
                          dtl.sca_status = p_status,
                          dtl.stage = gv_stage,
                          dtl.description = lv_description,
                          dtl.last_pe_setup_dt = lv_max_pe_dt -- DECODE(p_status,'PE_SETUP_COMPLETE',SYSDATE,NULL)
                    WHERE     dtl.meter_id = p_meter_id
                          AND (   dtl.idr_status IN
                                      ('DATA_RECEIVED', 'PE_SETUP_FAILED')
                               OR dtl.sca_status IN
                                      ('DATA_RECEIVED', 'PE_SETUP_FAILED'))
                          AND EXISTS
                                  (SELECT 1
                                     FROM alps_mml_pr_header hdr
                                    WHERE     dtl.uid_mml_master =
                                              hdr.uid_mml_master
                                          AND dtl.sbl_quote_id =
                                              hdr.sbl_quote_id
                                          AND hdr.active_flg = 'Y'
                                          AND hdr.status = 'PENDING')
                RETURNING uid_alps_account,
                          meter_id,
                          status,
                          sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct,
                          arr_meter_id,
                          arr_acct_status,
                          arr_quote_id;
        END CASE;

        -- Associate all parsed BA_SA accounts to the incoming data file.
        gv_code_ln := $$plsql_line;

        IF arr_uid_acct.COUNT > 0
        THEN
            FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
            LOOP
                -- Create account transactions
                gv_code_ln := $$plsql_line;
                arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                arr_acct_txn (1).status := p_status;
                arr_acct_txn (1).description := lv_description;
                --
                gv_code_ln := $$plsql_line;

                CASE p_idr_sca
                    WHEN 'IDR'
                    THEN
                        arr_acct_txn (1).txn_indicator := 'HI';
                        alps_request_pkg.setup_acct_transact (gv_source_id,
                                                              arr_acct_txn);
                    WHEN 'SCA'
                    THEN
                        arr_acct_txn (1).txn_indicator := 'HU';
                        alps_request_pkg.setup_acct_transact (gv_source_id,
                                                              arr_acct_txn);
                END CASE;
            END LOOP;
        END IF;

        --
        IF arr_acct_status.COUNT > 0
        THEN
            FOR ix IN arr_acct_status.FIRST .. arr_acct_status.LAST
            LOOP
                -- Perform VEE  check if account is ready to be VEE
                IF arr_acct_status (ix) = 'READY_FOR_VEE'
                THEN
                    insert_vee_mgt (arr_meter_id (ix));
                END IF;

                -- Update PR status to reflect overall current staus of all accounts
                gv_code_ln := $$plsql_line;

                IF arr_quote_id.COUNT > 0
                THEN
                    FOR ix IN arr_quote_id.FIRST .. arr_quote_id.LAST
                    LOOP
                        update_pr_status (arr_quote_id (ix), gv_stage);
                    END LOOP;
                END IF;
            END LOOP;
        END IF;

        COMMIT;
        p_out_status := 'SUCCESS';
        p_out_desc := NULL;
        arr_meter_id.delete;
        arr_acct_status.delete;
        arr_quote_id.delete;
        arr_uid_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- Set up the error message to be return to calling App.
            arr_meter_id.delete;
            arr_acct_status.delete;
            arr_quote_id.delete;
            arr_uid_acct.delete;
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            --
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_pe_setup_accounts;

    PROCEDURE update_vee_accts (p_meter_id      IN VARCHAR2,
                                p_status        IN VARCHAR2,
                                p_description   IN VARCHAR2)
    AS
        /*
                Purpose : This procedure perform following functions:
                                1. Update each meter individual status to reflect latest VEE status.
                                2. Mark meter ready for Siebel Setup if account complete VEE successfuly
                                3. Update PR status

                Parameters:
                                    p_meter_id -  meter id
                                    p_status  -  Current PE set up status . Values are PE_SETUP_COMPLETE, PE_SETUP_FAILED
                                    p_description - Detail description

                Execution : Called from external application
            */
        lv_max_vee_dt   DATE;
        vOutStatus            VARCHAR2 (10);
        vOutDesc              VARCHAR2 (1000);        
    BEGIN
        gv_code_proc := 'UPDATE_VEE_ACCTS';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        gv_stage := 'PE_SETUP';

        gv_code_ln := $$plsql_line;
        alps_mml_pkg.process_logging (
            gv_source_id,
            'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
            NULL,
            NULL,
            NULL,
            NULL,
            0,
               'Received Status Update from VEE Process:p_meter_id:'
            || p_meter_id
            || ';p_status-'
            || p_status
            || ';p_description:'
            || p_description,
            gv_stage,
            gv_source_type);

        --Get the MAX  last VEE date for this meter in from PE
        SELECT MAX (ovrd.lstime)
          INTO lv_max_vee_dt
          FROM serviceplan@TPpe       serv,
               account@TPpe           act,
               acctservicehist@TPpe   hist,
               acctoverridehist@TPpe  ovrd
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND act.uidaccount = ovrd.uidaccount
               AND ovrd.overridecode = 'VEE_COMPLETE_OVRD'
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        --
        gv_code_ln := $$plsql_line;

           UPDATE alps_mml_pr_detail dtl
              SET dtl.status = p_status,
                  dtl.stage = gv_stage,
                  dtl.description = p_description,
                  dtl.last_vee_completed_dt = lv_max_vee_dt -- DECODE(p_status,'VEE_COMPLETE', sysdate)
            WHERE     dtl.meter_id = p_meter_id
                  AND dtl.status IN ('READY_FOR_VEE', 'VEE_FAILED')
                  AND EXISTS
                          (SELECT 1
                             FROM alps_mml_pr_header hdr
                            WHERE     dtl.uid_mml_master = hdr.uid_mml_master
                                  AND dtl.sbl_quote_id = hdr.sbl_quote_id
                                  AND hdr.active_flg = 'Y'
                                  AND hdr.status = 'PENDING')
        RETURNING uid_alps_account, sbl_quote_id, meter_id 
             BULK COLLECT INTO arr_uid_acct, arr_quote_id, arr_meter_id;

        -- Associate all parsed BA_SA accounts to the incoming data file.
        gv_code_ln := $$plsql_line;

        IF arr_uid_acct.COUNT > 0
        THEN
            FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
            LOOP
                -- Create account transactions
                gv_code_ln := $$plsql_line;
                arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                arr_acct_txn (1).status := p_status;
                arr_acct_txn (1).description := p_description;
                --
                gv_code_ln := $$plsql_line;
                alps_request_pkg.setup_acct_transact (gv_source_id,
                                                      arr_acct_txn);
            END LOOP;
        END IF;
        
        gv_code_ln := $$plsql_line;        
        -- Generate MISO future Trans Tags
                    gv_code_ln := $$plsql_line;
        IF arr_meter_id.COUNT > 0
        THEN
            FOR ix IN arr_meter_id.FIRST .. arr_meter_id.LAST
            LOOP
               ALPS.MISO_TAGS_RECALC_PR (arr_quote_id (ix),
                                         arr_meter_id (ix),
                                              vOutStatus,
                                              vOutDesc);
            END LOOP;
        END IF;                    

        --
        gv_code_ln := $$plsql_line;

        -- Update PR status to reflect overall current staus of all accounts
        IF arr_quote_id.COUNT > 0
        THEN
            FOR ix IN arr_quote_id.FIRST .. arr_quote_id.LAST
            LOOP
                update_pr_status (arr_quote_id (ix), gv_stage);
            END LOOP;
        END IF;

        --

        COMMIT;
        arr_quote_id.delete;
        arr_uid_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            arr_quote_id.delete;
            arr_uid_acct.delete;
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_vee_accts;

    PROCEDURE update_vee_account (p_meter_id      IN     VARCHAR2,
                                  p_status        IN     VARCHAR2,
                                  p_description   IN     VARCHAR2,
                                  p_out_status       OUT VARCHAR2,
                                  p_out_desc         OUT VARCHAR2)
    AS
        /*
                Purpose : This procedure perform following functions:
                                1. Update each meter individual status to reflect latest VEE status.
                                2. Mark meter ready for Siebel Setup if account complete VEE successfuly
                                3. Update PR status

                Parameters:
                                    p_meter_id -  meter id
                                    p_status  -  Current PE set up status . Values are PE_SETUP_COMPLETE, PE_SETUP_FAILED
                                    p_description - Detail description
                                    p_out_status - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                                    p_out_description - Description of the function call returned to calling program(process).

                Execution : Called from external application
            */
        lv_max_vee_dt   DATE;
        vOutStatus            VARCHAR2 (10);
        vOutDesc              VARCHAR2 (1000);         
    BEGIN
        gv_code_proc := 'UPDATE_VEE_ACCOUNT';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        gv_stage := 'PE_SETUP';

        --Get the MAX  last VEE date for this meter in from PE
        SELECT MAX (ovrd.lstime)
          INTO lv_max_vee_dt
          FROM serviceplan@TPpe       serv,
               account@TPpe           act,
               acctservicehist@TPpe   hist,
               acctoverridehist@TPpe  ovrd
         WHERE     act.uidaccount = serv.uidaccount
               AND act.uidaccount = hist.uidaccount
               AND act.uidaccount = ovrd.uidaccount
               AND ovrd.overridecode = 'VEE_COMPLETE_OVRD'
               AND serv.ldcaccountno = SUBSTR (p_meter_id,
                                                 INSTR (p_meter_id,
                                                        '_',
                                                        1,
                                                        2)
                                               + 1)
               AND hist.marketcode = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
               AND hist.discocode = SUBSTR (p_meter_id,
                                              INSTR (p_meter_id,
                                                     '_',
                                                     1,
                                                     1)
                                            + 1,
                                            (  (  INSTR (p_meter_id,
                                                         '_',
                                                         1,
                                                         2)
                                                - 1)
                                             - INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)));

        --
        gv_code_ln := $$plsql_line;

           UPDATE alps_mml_pr_detail dtl
              SET dtl.status = p_status,
                  dtl.stage = gv_stage,
                  dtl.description = p_description,
                  dtl.last_vee_completed_dt = lv_max_vee_dt -- DECODE(p_status,'VEE_COMPLETE', sysdate)
            WHERE     dtl.meter_id = p_meter_id
                  AND dtl.status IN ('READY_FOR_VEE', 'VEE_FAILED')
                  AND EXISTS
                          (SELECT 1
                             FROM alps_mml_pr_header hdr
                            WHERE     dtl.uid_mml_master = hdr.uid_mml_master
                                  AND dtl.sbl_quote_id = hdr.sbl_quote_id
                                  AND hdr.active_flg = 'Y'
                                  AND hdr.status = 'PENDING')
        RETURNING uid_alps_account, sbl_quote_id, meter_id 
             BULK COLLECT INTO arr_uid_acct, arr_quote_id , arr_meter_id;

        -- Associate all parsed BA_SA accounts to the incoming data file.
        gv_code_ln := $$plsql_line;

        IF arr_uid_acct.COUNT > 0
        THEN
            FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
            LOOP
                -- Create account transactions
                gv_code_ln := $$plsql_line;
                arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                arr_acct_txn (1).status := p_status;
                arr_acct_txn (1).description := p_description;
                --
                gv_code_ln := $$plsql_line;
                alps_request_pkg.setup_acct_transact (gv_source_id,
                                                      arr_acct_txn);
            END LOOP;
        END IF;

        gv_code_ln := $$plsql_line;        
        -- Generate MISO future Trans Tags
                    gv_code_ln := $$plsql_line;
        IF arr_meter_id.COUNT > 0
        THEN
            FOR ix IN arr_meter_id.FIRST .. arr_meter_id.LAST
            LOOP
               ALPS.MISO_TAGS_RECALC_PR (arr_quote_id (ix),
                                         arr_meter_id (ix),
                                              vOutStatus,
                                              vOutDesc);
            END LOOP;
        END IF;                    

        --
        gv_code_ln := $$plsql_line;

        -- Update PR status to reflect overall current staus of all accounts
        IF arr_quote_id.COUNT > 0
        THEN
            FOR ix IN arr_quote_id.FIRST .. arr_quote_id.LAST
            LOOP
                update_pr_status (arr_quote_id (ix), gv_stage);
            END LOOP;
        END IF;

        --

        COMMIT;
        arr_quote_id.delete;
        arr_uid_acct.delete;
        p_out_status := 'SUCCESS';
        p_out_desc := NULL;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- Set up the error message to be return to calling App.
            arr_quote_id.delete;
            arr_uid_acct.delete;
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_vee_account;

    PROCEDURE update_vee_pr (p_prnumber      IN     VARCHAR2,
                             p_status        IN     VARCHAR2,
                             p_description   IN     VARCHAR2,
                             p_out_status       OUT VARCHAR2,
                             p_out_desc         OUT VARCHAR2)
    AS
    /*
            Purpose : This procedure perform following functions for a given PR:
                            1. Update each meter individual status to reflect latest VEE status.
                            2. Mark meter ready for Siebel Setup if account complete VEE successfuly
                            3. Update PR status

            Parameters:
                                p_prnumber -  PR Number
                                p_status  -  Current PE set up status . Values are PE_SETUP_COMPLETE, PE_SETUP_FAILED
                                p_description - Detail description
                                p_out_status - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                                p_out_description - Description of the function call returned to calling program(process).

            Execution : Called from external application
        */
    BEGIN
        gv_code_proc := 'UPDATE_VEE_PR';
        gv_source_id := p_prnumber;
        gv_source_type := 'PR_NUMBER';
        gv_stage := 'PE_SETUP';
        --
        gv_code_ln := $$plsql_line;

        SELECT uid_alps_account
          BULK COLLECT INTO arr_uid_acct
          FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
         WHERE     dtl.uid_mml_master = hdr.uid_mml_master
               AND dtl.sbl_quote_id = hdr.sbl_quote_id
               AND hdr.prnumber = p_prnumber
               AND hdr.active_flg = 'Y'
               AND dtl.status IN ('READY_FOR_VEE', 'VEE_FAILED')
               AND hdr.status = 'PENDING';

        --
        IF arr_uid_acct.COUNT > 0
        THEN
            FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
            LOOP
                   UPDATE alps_mml_pr_detail
                      SET status = p_status,
                          stage = gv_stage,
                          description = p_description,
                          last_vee_completed_dt =
                              DECODE (p_status, 'VEE_COMPLETE', SYSDATE)
                    WHERE uid_alps_account = arr_uid_acct (ix)
                RETURNING uid_alps_account, sbl_quote_id
                     BULK COLLECT INTO arr_uid_acct, arr_quote_id;

                --
                -- Associate all parsed BA_SA accounts to the incoming data file.
                gv_code_ln := $$plsql_line;

                IF arr_uid_acct.COUNT > 0
                THEN
                    FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
                    LOOP
                        -- Create account transactions
                        gv_code_ln := $$plsql_line;
                        arr_acct_txn (1).uid_alps_account :=
                            arr_uid_acct (ix);
                        arr_acct_txn (1).status := p_status;
                        arr_acct_txn (1).description := p_description;
                        --
                        gv_code_ln := $$plsql_line;
                        alps_request_pkg.setup_acct_transact (gv_source_id,
                                                              arr_acct_txn);
                    END LOOP;
                END IF;

                --
                gv_code_ln := $$plsql_line;

                -- Update PR status to reflect overall current staus of all accounts
                IF arr_quote_id.COUNT > 0
                THEN
                    FOR ix IN arr_quote_id.FIRST .. arr_quote_id.LAST
                    LOOP
                        update_pr_status (arr_quote_id (ix), gv_stage);
                    END LOOP;
                END IF;
            END LOOP;
        END IF;

        --
        COMMIT;
        arr_quote_id.delete;
        arr_uid_acct.delete;
        p_out_status := 'SUCCESS';
        p_out_desc := NULL;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- Set up the error message to be return to calling App.
            arr_quote_id.delete;
            arr_uid_acct.delete;
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_vee_pr;


    PROCEDURE update_acct_status (p_sbl_quote_id   IN VARCHAR2,
                                  p_status         IN VARCHAR2,
                                  p_description    IN VARCHAR2)
    AS
        /*
            Purpose : This procedure update the accounts(meters) overall status for a given Quote_ID.
                            Every time there is a status change at PR level, the underlying accounts will inherit the same status

            Parameter :
                              p_sbl_quote_id - Siebel  quote_id
                               p_status - Overall PR status
                               p_description - Detailed  description

            Execution :  This procedure executed  via table (ALPS_MML_PR_HEADER) trigger(T_ALPS_PR_HEADER_AU) .
        */
        lv_uid_alps_account   alps_mml_pr_detail.uid_alps_account%TYPE;
        lv_sbl_quote_id       alps_mml_pr_detail.sbl_quote_id%TYPE;
        lv_status_cnt         INTEGER := 0;
    BEGIN
        gv_code_proc := 'UPDATE_ACCT_STATUS';
        gv_source_id := p_sbl_quote_id;
        gv_source_type := 'QUOTE_ID';

        IF p_status IN ('SETUP_COMPLETE', 'SETUP_FAILED', 'VEE_COMPLETE')
        THEN
            gv_stage := 'SIEBEL_SETUP';
            gv_code_ln := $$plsql_line;

            SELECT dtl.uid_alps_account
              BULK COLLECT INTO arr_uid_acct
              FROM alps_mml_pr_detail dtl
             WHERE     dtl.sbl_quote_id = p_sbl_quote_id
                   AND status IN
                           ('SETUP_COMPLETE', 'SETUP_FAILED', 'VEE_COMPLETE');

            --
            gv_code_ln := $$plsql_line;

            IF arr_uid_acct.COUNT > 0
            THEN
                FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
                LOOP
                    -- Other stages which Update only Account overall STATUS
                    gv_code_ln := $$plsql_line;

                    UPDATE alps_mml_pr_detail dtl
                       SET status = p_status,
                           stage = gv_stage,
                           description = p_description
                     WHERE dtl.uid_alps_account = arr_uid_acct (ix);

                    -- Create account transactions
                    gv_code_ln := $$plsql_line;
                    arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                    arr_acct_txn (1).status := p_status;
                    arr_acct_txn (1).description := p_description;
                    --
                    gv_code_ln := $$plsql_line;
                    alps_request_pkg.setup_acct_transact (gv_source_id,
                                                          arr_acct_txn);
                END LOOP;
            END IF;
        ELSIF p_status IN
                  ('OFFER_SUMMARY_COMPLETE',
                   'OFFER_SUMMARY_FAILED',
                   'SETUP_COMPLETE')
        THEN
            gv_stage := 'OFFER_SETUP';
            gv_code_ln := $$plsql_line;

            SELECT dtl.uid_alps_account
              BULK COLLECT INTO arr_uid_acct
              FROM alps_mml_pr_detail dtl
             WHERE     dtl.sbl_quote_id = p_sbl_quote_id
                   AND status IN
                           ('OFFER_SUMMARY_COMPLETE',
                            'OFFER_SUMMARY_FAILED',
                            'SETUP_COMPLETE');

            --
            gv_code_ln := $$plsql_line;

            IF arr_uid_acct.COUNT > 0
            THEN
                FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
                LOOP
                    gv_code_ln := $$plsql_line;

                    UPDATE alps_mml_pr_detail dtl
                       SET status = p_status, description = p_description
                     WHERE dtl.uid_alps_account = arr_uid_acct (ix);

                    --
                    -- Create account transactions
                    gv_code_ln := $$plsql_line;
                    arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                    arr_acct_txn (1).status := p_status;
                    arr_acct_txn (1).description := p_description;
                    --
                    gv_code_ln := $$plsql_line;
                    alps_request_pkg.setup_acct_transact (gv_source_id,
                                                          arr_acct_txn);
                END LOOP;
            END IF;
        END IF;

        --
        --COMMIT;
        arr_uid_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            --ROLLBACK;
            RAISE;
    END update_acct_status;

    PROCEDURE update_pr_detail_status (p_meter_id      IN VARCHAR2,
                                       p_meter_type    IN VARCHAR2,
                                       p_status_ind    IN VARCHAR2,
                                       p_status_val    IN VARCHAR2,
                                       p_description   IN VARCHAR2,
                                       p_uid_file      IN NUMBER)
    AS
        /*
            Purpose : This procedure update each Account(Meter) individual status(IDR_STATUS/SCA_STATUS) and meter type(METER_TYPE) post parsing.
                           For every account(meter) parsed , the individual status should be update to reflect whether it was parsed successfully(DATA_RECEIVED) or unsuccessfully(DATA_FAILED).
                           It also perform other essential functions such as:
                           1.  Update meter type of an account(meter) .
                           2. Create file/account(meter) transaction. Connect the accounts(meters) to a file it was parsed from.
                           3. For utility that request SCALAR initially and parser indicated that request of IDR is necessary, the process will automatically request IDR data(IDR_STATUS is set REQUEST_ATTEMPT)
                           4. Also handle update for all accounts that were originally requestes using BA only.  The parsed BA_SA will be matched to its original BA and inserted into ALPS_MML_PR_DETAIL.
            Parameters :
                                p_meter_id - Meter ID which is combination of Market_disco_ldc_account
                                p_meter_type  - Meter type of a meter. Values are IDR , SCA
                                p_status_ind  -  Indicated individual status to be updated. Values are IDR_STATUS, SCA_STATUS
                                p_status_val  -  Values of individual status. Values are DATA_RECEIVED, DATA_ERROR
                                p_description  -  Meaningful description
                                p_uid_file -  The uid of the Parsed file

            Execution :  This procedure executed  via table (ALPS_ACCOUNT_RAW) trigger(ALPS_ACCOUNT_RAW_AIU) .
        */
        lv_att_file_type         alps_file_attachment.att_file_type%TYPE;
        lv_uid_alps_account      alps_mml_pr_detail.uid_alps_account%TYPE;
        lv_sbl_quote_id          alps_mml_pr_detail.sbl_quote_id%TYPE;
        lv_where                 VARCHAR2 (2000);
        lv_update                VARCHAR2 (2000);
        lv_sql                   VARCHAR2 (1000);
        lv_out_status            VARCHAR2 (20);
        lv_out_description       VARCHAR2 (500);
        lv_ba_cnt                INTEGER := 0;
        lv_row_proc              INTEGER := 0;
        lv_recent_idr            VARCHAR2 (3);
        lv_meter_type            VARCHAR2 (10);

        lv_cnt                   INTEGER;
        --
        lv_uid_mml_master        alps_mml_pr_detail.uid_mml_master%TYPE;
        lv_market_code           alps_mml_pr_detail.market_code%TYPE;
        lv_disco_code            alps_mml_pr_detail.disco_code%TYPE;
        lv_digit_code_key        alps_mml_pr_detail.digit_code_key%TYPE;
        lv_idr_source            alps_mml_pr_detail.idr_source%TYPE;
        lv_sca_source            alps_mml_pr_detail.sca_source%TYPE;
        lv_idr_status            alps_mml_pr_detail.idr_status%TYPE;
        lv_sca_status            alps_mml_pr_detail.sca_status%TYPE;
        lv_idr_status_val        alps_mml_pr_detail.idr_status%TYPE;
        lv_sca_status_val        alps_mml_pr_detail.sca_status%TYPE;
        lv_ba_uid_alps_account   alps_mml_pr_detail.uid_alps_account%TYPE;
        lv_address1              alps_mml_pr_detail.address1%TYPE;
        lv_address2              alps_mml_pr_detail.address2%TYPE;
        lv_city                  alps_mml_pr_detail.city%TYPE;
        lv_state                 alps_mml_pr_detail.state%TYPE;
        lv_zip                   alps_mml_pr_detail.zip%TYPE;
        lv_prnumber              ALPS_MML_PR_HEADER.PRNUMBER%TYPE;

        --
        TYPE tabquoteid IS TABLE OF alps_mml_pr_detail.sbl_quote_id%TYPE
            INDEX BY PLS_INTEGER;

        vquoteid                 tabquoteid;
        voutstatus               VARCHAR2 (60);
        voutdesc                 VARCHAR2 (500);
        vWebReQdT                DATE;
        vEdiReQdT                DATE;
    BEGIN
        --  Get the MML meter's meter type and its IDR_STATUS.
        gv_code_proc := 'UPDATE_PR_DETAIL_STATUS';
        -- Get account attributes
        DBMS_OUTPUT.put_line (
               'UPDATE_PR_DETAIL_STATUS: p_meter_id ==> '
             || p_meter_id 
             ||  ',  p_meter_type ==> '
             || p_meter_type
             || ', p_status_ind ==> '
             || p_status_ind);

        -- get the PR INFO
        BEGIN
            SELECT PRNUMBER
              INTO lv_prnumber
              FROM alps_mml_pr_header  A,
                   (SELECT MAX (HDR.uid_mml_master)     uid_mml_master
                      FROM alps_mml_pr_detail dtl1, alps_mml_pr_header hdr
                     WHERE     (   dtl1.meter_id = p_meter_id
                                OR dtl1.meter_id =
                                   SUBSTR (p_meter_id,
                                           1,
                                           INSTR (p_meter_id, '_', -1) - 1))
                           AND dtl1.sca_status IN ('REQUEST_COMPLETE',
                                                   'REQUEST_PENDING',
                                                   'DATA_ERROR',
                                                   'REQUEST_FAILED',
                                                   'INTERNAL_NEW',
                                                   'WEB_SCRAPE_PENDING',
                                                   'METERTYPE_TBD',
                                                   'NOT_IDR')
                           AND dtl1.uid_mml_master = hdr.uid_mml_master
                           AND dtl1.sbl_quote_id = hdr.sbl_quote_id
                           AND hdr.active_flg = 'Y'
                           AND hdr.status = 'PENDING') B
             WHERE a.uid_mml_master = b.uid_mml_master;             
        EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
             BEGIN
              SELECT PRNUMBER
              INTO lv_prnumber
              FROM alps_mml_pr_header  A,
                   (SELECT MAX (HDR.uid_mml_master)     uid_mml_master
                      FROM alps_mml_pr_detail dtl1, alps_mml_pr_header hdr
                     WHERE     (   dtl1.meter_id = p_meter_id
                                OR dtl1.ldc_account = SUBSTR( SUBSTR (p_meter_id,
                                                    INSTR (p_meter_id,'_',
                                                    -1, 2)+1                                            
                        ),
            1, INSTR( SUBSTR (p_meter_id,
                                                    INSTR (p_meter_id,'_',
                                                    -1, 2)+1                                            
                        ),'_' ) -1   ) )
                           AND dtl1.sca_status IN ('REQUEST_COMPLETE',
                                                   'REQUEST_PENDING',
                                                   'DATA_ERROR',
                                                   'REQUEST_FAILED',
                                                   'INTERNAL_NEW',
                                                   'WEB_SCRAPE_PENDING',
                                                   'METERTYPE_TBD',
                                                   'NOT_IDR')
                           AND dtl1.uid_mml_master = hdr.uid_mml_master
                           AND dtl1.sbl_quote_id = hdr.sbl_quote_id
                           AND hdr.active_flg = 'Y'
                           AND hdr.status = 'PENDING') B
             WHERE a.uid_mml_master = b.uid_mml_master;
           EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN lv_prnumber := NULL;
           END;
       END;
 DBMS_OUTPUT.put_line ('lv_prnumber : '||lv_prnumber );

            gv_code_ln := $$plsql_line;
        BEGIN
            SELECT DISTINCT NVL(address1, 'UNKNOWN'),
                            NVL(address2,'UNKNOWN'),
                            NVL(city,'UNKNOWN'),
                            NVL(state,'UNKNOWN'),
                            NVL(zip,'00000')
              INTO lv_address1,
                   lv_address2,
                   lv_city,
                   lv_state,
                   lv_zip
              FROM alps_account
             WHERE     meter_id = p_meter_id
             AND last_refresh_dt >= sysdate -10;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            lv_address1 := 'UNKNOWN';      
            lv_address2 := 'UNKNOWN';     
            lv_city := 'UNKNOWN';     
            lv_state := 'UNKNOWN'; 
            lv_zip := '00000';                                                                   
        END;  
                           
     --   END IF;
 DBMS_OUTPUT.put_line ('lv_address1 : '||lv_address1 );
        --
        IF p_uid_file IS NOT NULL
        THEN
            -- get the file type
            BEGIN
                gv_code_ln := $$plsql_line;

                SELECT att_file_type
                  INTO lv_att_file_type
                  FROM alps_file_attachment
                 WHERE uid_attachment = p_uid_file;
            EXCEPTION
                WHEN NO_DATA_FOUND
                THEN
                    lv_att_file_type := NULL;
            END;
        END IF;
        
        -- Retrieve IDR_STATUS 
        gv_code_ln := $$plsql_line;
        BEGIN
            --  Get the MML meter's meter type and its IDR_STATUS.
            gv_code_ln := $$plsql_line;
            SELECT dtl.idr_status
              INTO lv_idr_status_val
              FROM alps_mml_pr_detail dtl
             WHERE dtl.uid_alps_account =
                   (SELECT MAX (uid_alps_account)
                      FROM alps_mml_pr_detail dtl1, alps_mml_pr_header hdr
                     WHERE      (dtl1.meter_id = p_meter_id
                                OR dtl1.meter_id =
                                   SUBSTR (p_meter_id,
                                           1,
                                           INSTR (p_meter_id, '_', -1) - 1))
                           AND dtl1.sca_status IN ('REQUEST_COMPLETE',
                                                   'REQUEST_PENDING',
                                                   'DATA_ERROR',
                                                   'REQUEST_FAILED',
                                                   'INTERNAL_NEW',
                                                   'WEB_SCRAPE_PENDING',
                                                   'METERTYPE_TBD',
                                                   'NOT_IDR')
                           AND dtl1.uid_mml_master = hdr.uid_mml_master
                           AND dtl1.sbl_quote_id = hdr.sbl_quote_id
                           AND hdr.active_flg = 'Y'
                           AND hdr.status = 'PENDING');

        EXCEPTION             
        WHEN NO_DATA_FOUND
        THEN
            BEGIN
                SELECT dtl.idr_status
                  INTO lv_idr_status_val
                  FROM alps_mml_pr_detail dtl
                 WHERE dtl.uid_alps_account =
                       (SELECT MAX (uid_alps_account)
                          FROM alps_mml_pr_detail  dtl1,
                               alps_mml_pr_header  hdr
                         WHERE     dtl1.market_code =
                                   SUBSTR (p_meter_id,
                                           1,
                                             INSTR (p_meter_id,
                                                    '_',
                                                    1,
                                                    1)
                                           - 1)
                               AND dtl1.ldc_account =
                                                                      SUBSTR(
                 SUBSTR (p_meter_id,
                                                    INSTR (p_meter_id,'_',
                                                    -1, 2)+1                                            
                        ),
            1, INSTR( SUBSTR (p_meter_id,
                                                    INSTR (p_meter_id,'_',
                                                    -1, 2)+1                                            
                        ),'_' ) -1   ) 
                               AND dtl1.sca_status IN
                                       ('REQUEST_COMPLETE',
                                        'REQUEST_PENDING',
                                        'DATA_ERROR',
                                        'REQUEST_FAILED',
                                        'INTERNAL_NEW',
                                        'WEB_SCRAPE_PENDING',
                                        'METERTYPE_TBD',
                                        'NOT_IDR')
                               AND dtl1.uid_mml_master =
                                   hdr.uid_mml_master
                               AND dtl1.sbl_quote_id =
                                   hdr.sbl_quote_id
                               AND hdr.active_flg = 'Y'
                               AND hdr.status = 'PENDING');

            EXCEPTION
                WHEN OTHERS
                THEN
                   NULL;
            END;
                
        END;

        DBMS_OUTPUT.put_line ('IDR STATUS  ==>  '|| lv_idr_status_val);
                    
        IF p_meter_type = 'IDR'
        THEN
           -- Meter type has been determined  as IDR and if MML meter indicated otherwise then request IDR
            IF  lv_idr_status_val IN ('METERTYPE_TBD', 'NOT_IDR')
               AND SUBSTR (lv_prnumber, 1, 1) NOT IN ('E', 'M', 'T')
            THEN    --AND lv_meter_type = 'UNKNOWN' ) OR lv_meter_type = 'SCA'
                lv_idr_status_val := 'REQUEST_ATTEMPT';
            END IF;
        ELSIF p_meter_type IN ('SCA', 'SCALAR')
        THEN
            DBMS_OUTPUT.put_line ('SCALAR Meter type');
            -- Meter type has been determined as SCALAR thus set  IDR_STATUS= 'NOT_IDR'
            lv_idr_status_val := 'NOT_IDR';

            -- Remove IDR meter ALPS_IDR_LOG
            DELETE FROM alps_idr_log
                  WHERE meter_id = p_meter_id;
        END IF;

        -- Update MML PR DETAIL
        gv_code_ln := $$plsql_line;
        --
        IF p_status_ind = 'MARKET_DISCO_LDC'  THEN
                    DBMS_OUTPUT.put_line ('ln 5624 : Update MARKET_DISCO_LDC');
        lv_update :=
               ' SET dtl.sca_status = DECODE('
            || ''''
            || p_status_val
            || ''''
            || ' , ''DATA_PENDING'' , dtl.sca_status , '
            || ''''
            || p_status_val
            || ''' )'
            || ' , dtl.idr_status ='
            || ''''
            || lv_idr_status_val
            || ''''
            || ' , dtl.stage = '
            || ''''
            || gv_stage
            || ''''
            || ' , dtl.description = '
            || ''''
            || p_description
            || ''''
            || ' , dtl.meter_type = DECODE('
            || ''''
            || p_meter_type
            || ''''
            || ' , ''UNKNOWN'' , dtl.meter_type ,  '
            || ''''
            || p_meter_type
            || ''')'
            || ' , dtl.address1 = '
            || ''''
            || lv_address1
            || ''''
            || ' , dtl.address2 = '
            || ''''
            || lv_address2
            || ''''
            || ' , dtl.city = '
            || ''''
            || lv_city
            || ''''
            || ' , dtl.state = '
            || ''''
            || lv_state
            || ''''
            || ' , dtl.zip = '
            || ''''
            || lv_zip
            || '''';             
            
            --
                     
            lv_where :=
                   ' WHERE  dtl.meter_id = '
                || ''''
                || p_meter_id
                || ''''
                || ' AND dtl.sca_status IN  ( ''REQUEST_COMPLETE'' , ''REQUEST_PENDING'' ,  ''DATA_ERROR'',''REQUEST_FAILED'',''INTERNAL_NEW'',''WEB_SCRAPE_PENDING'')
                         AND EXISTS (SELECT 1 FROM alps_mml_pr_header hdr
                                             WHERE dtl.uid_mml_master = hdr.uid_mml_master
                                             AND dtl.sbl_quote_id = hdr.sbl_quote_id
                                             AND hdr.active_flg = ''Y'' AND  hdr.status =  ''PENDING''  )  RETURNING uid_alps_account  INTO :1  ';
        ELSIF p_status_ind = 'MARKET_LDC'  THEN
                    DBMS_OUTPUT.put_line ('ln 5687 : Update MARKET_LDC');        
            lv_update :=
                   ' SET dtl.sca_status = DECODE('
                || ''''
                || p_status_val
                || ''''
                || ' , ''DATA_PENDING'' , dtl.sca_status , '
                || ''''
                || p_status_val
                || ''' )'
                || ' , dtl.idr_status ='
                || ''''
                || lv_idr_status_val
                || ''''
                || ' , dtl.stage = '
                || ''''
                || gv_stage
                || ''''
                || ' , dtl.description = '
                || ''''
                || p_description
                || ''''
                || ' , dtl.meter_type = DECODE('
                || ''''
                || p_meter_type
                || ''''
                || ' , ''UNKNOWN'' , dtl.meter_type ,  '
                || ''''
                || p_meter_type
                || ''')'
                || ' , dtl.address1 = '
                || ''''
                || lv_address1
                || ''''
                || ' , dtl.address2 = '
                || ''''
                || lv_address2
                || ''''
                || ' , dtl.city = '
                || ''''
                || lv_city
                || ''''
                || ' , dtl.state = '
                || ''''
                || lv_state
                || ''''
                || ' , dtl.zip = '
                || ''''
                || lv_zip
                || ''''
                || ' , dtl.disco_code =SUBSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ', INSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ' ,''_'', 1, 1) + 1, ( INSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ' ,''_'', 1, 2) -1) -  INSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ' ,''_'', 1, 1))'
                || ' , dtl.ldc_account  = SUBSTR('
                || ''''
                || p_meter_id
                || ''''
                || ', INSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ',''_'', 1, 2) +1)';
                                
            --                                                
           lv_where :=
                   ' WHERE  dtl.market_code = SUBSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ', 1, INSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ' ,''_'', 1, 1) - 1) '
                || ' AND  dtl.ldc_account  =  SUBSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ', INSTR ('
                || ''''
                || p_meter_id
                || ''''
                || ',  ''_'', 1, 2)+ 1)'
                || ' AND dtl.sca_status IN  ( ''REQUEST_COMPLETE'' , ''REQUEST_PENDING'' , ''DATA_ERROR'',''REQUEST_FAILED'',''INTERNAL_NEW'',''WEB_SCRAPE_PENDING'')
                         AND EXISTS (SELECT 1 FROM alps_mml_pr_header hdr
                                             WHERE dtl.uid_mml_master = hdr.uid_mml_master
                                             AND dtl.sbl_quote_id = hdr.sbl_quote_id
                                             AND hdr.active_flg = ''Y'' AND  hdr.status =  ''PENDING'' )  RETURNING uid_alps_account  INTO :1  ';

        ELSE
            NULL;                                                      
        END IF;
                                             
        DBMS_OUTPUT.put_line ('BEFORE UPDATE, lv_where ==> ' || lv_where);
        DBMS_OUTPUT.put_line ('BEFORE UPDATE, lv_update ==> ' || lv_update);

        gv_code_ln := $$plsql_line;

        IF lv_update IS NOT NULL OR lv_where IS NOT NULL
        THEN
            EXECUTE IMMEDIATE   ' UPDATE alps_mml_pr_detail  dtl '
                             || lv_update
                             || lv_where
                RETURNING BULK COLLECT INTO arr_uid_acct;
                        DBMS_OUTPUT.put_line ('ROW UPDATED ==> '||SQL%ROWCOUNT);
        END IF;

        IF arr_uid_acct.COUNT > 0
        THEN
            IF p_status_val <> 'DATA_PENDING'
            THEN
                -- Create transactions.
                DBMS_OUTPUT.put_line ('Create Transaction logs');

                FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
                LOOP
                    -- Create account transactions
                    arr_acct_txn (1).uid_attachment := p_uid_file;
                    arr_acct_txn (1).uid_alps_account := arr_uid_acct (ix);
                    arr_acct_txn (1).transact_type := lv_att_file_type;
                    arr_acct_txn (1).status := p_status_val;
                    arr_acct_txn (1).description := p_description;

                    --
                    gv_code_ln := $$plsql_line;

                    arr_acct_txn (1).txn_indicator := 'HU';
                    alps_request_pkg.setup_acct_transact (
                        gv_source_id,
                        arr_acct_txn);

                END LOOP;
            END IF;

        END IF;

        --
        -- User story 27310-  Continue to locate remaning pending BA only.
        --
        gv_code_ln := $$plsql_line;
        IF p_status_ind = 'BA'  THEN
                            DBMS_OUTPUT.put_line ('ln 5859 : Update BA');  
            SELECT DISTINCT
                   dtl.uid_alps_account,
                   dtl.sbl_quote_id,
                   dtl.uid_mml_master,
                   dtl.market_code,
                   SUBSTR( (SUBSTR (p_meter_id,
                             INSTR (p_meter_id,
                                    '_',
                                    1,
                                    1) + 1)  ),1, INSTR((SUBSTR (p_meter_id,
                             INSTR (p_meter_id,
                                    '_',
                                    1,
                                    1) + 1)   )   ,'_')-1)  ,
                   dtl.address1,
                   dtl.address2,
                   dtl.city,
                   dtl.state,
                   dtl.zip,
                   dtl.digit_code_key,
                   dtl.idr_source,
                   dtl.sca_source,
                   lv_idr_status_val  idr_status,
                   p_status_val  sca_status
              BULK COLLECT INTO arr_ba_acct
              FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
             WHERE     market_code = SUBSTR (p_meter_id,
                                             1,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      1)
                                             - 1)
                   AND ldc_account = SUBSTR( (SUBSTR (p_meter_id,
                                             INSTR (p_meter_id , '_', 1, 2) + 1)  ),1, 
                                             INSTR((SUBSTR (p_meter_id,
                                             INSTR (p_meter_id,'_',1,2) + 1)),'_')-1) -- Matched by BA only
                   AND dtl.sbl_quote_id = hdr.sbl_quote_id
                   AND (   dtl.idr_status IN ('REQUEST_COMPLETE',
                                              'REQUEST_PENDING',
                                              'DATA_ERROR',
                                              'REQUEST_FAILED',
                                              'REQUEST_DELAY',
                                              'WEB_SCRAPE_PENDING')
                        OR dtl.sca_status IN ('REQUEST_COMPLETE',
                                              'REQUEST_PENDING',
                                              'DATA_ERROR',
                                              'REQUEST_FAILED',
                                              'REQUEST_DELAY',
                                              'WEB_SCRAPE_PENDING'))
                   AND hdr.active_flg = 'Y'
                   AND hdr.status = 'PENDING'
                   AND (SELECT COUNT (1)
                          FROM alps_mml_pr_detail dtl1
                         WHERE     dtl1.market_code =
                                   SUBSTR (p_meter_id,
                                           1,
                                             INSTR (p_meter_id,
                                                    '_',
                                                    1,
                                                    1)
                                           - 1)
                               AND dtl1.ldc_account =
                                   SUBSTR (p_meter_id,
                                             INSTR (p_meter_id,
                                                    '_',
                                                    1,
                                                    2)
                                           + 1) 
                               AND dtl1.sbl_quote_id = dtl.sbl_quote_id) =  0;  --  BA_SA not been loaded yet

            DBMS_OUTPUT.put_line (
                ' BA_SA ACCOUNT ,  arr_ba_acct.COUNT ==> ' || arr_ba_acct.COUNT);

            IF arr_ba_acct.COUNT > 0
            THEN

                -- BA account founded, let insert the parsed BA_SA account into PR _DETAIL and retain its attributes
                FOR xx IN arr_ba_acct.FIRST .. arr_ba_acct.LAST
                LOOP
                    gv_code_ln := $$plsql_line;
                    DBMS_OUTPUT.put_line ('Insert BA_SA into alps_mml_pr_detail');

                    INSERT INTO alps_mml_pr_detail (uid_alps_account,
                                                    created_user,
                                                    updated_user,
                                                    sbl_quote_id,
                                                    uid_mml_master,
                                                    ldc_account,
                                                    meter_type,
                                                    market_code,
                                                    disco_code,
                                                    description,
                                                    address1,
                                                    address2,
                                                    city,
                                                    state,
                                                    zip,
                                                    digit_code_key,
                                                    stage,
                                                    status,
                                                    idr_status,
                                                    sca_status,
                                                    idr_source,
                                                    sca_source,
                                                    meter_id)
                         VALUES (
                                    alps_mml_acct_seq.NEXTVAL,
                                    USER,
                                    USER,
                                    arr_ba_acct (xx).sbl_quote_id,
                                    arr_ba_acct (xx).uid_mml_master,
                                    (SUBSTR (p_meter_id,
                                               INSTR (p_meter_id,
                                                      '_',
                                                      1,
                                                      2)
                                             + 1)),  --BA_SA
                                    p_meter_type,
                                    arr_ba_acct (xx).market_code,
                                    arr_ba_acct (xx).disco_code,
                                    p_description,
                                    lv_address1,       --arr_ba_acct(xx).address1,
                                    lv_address2,       --arr_ba_acct(xx).address2,
                                    lv_city,               --arr_ba_acct(xx).city,
                                    lv_state,             --arr_ba_acct(xx).state,
                                    lv_zip,                 --arr_ba_acct(xx).zip,
                                    arr_ba_acct (xx).digit_code_key,
                                    gv_stage,
                                    (SELECT status
                                       FROM alps_status_matrix
                                      WHERE     idr_status =
                                                arr_ba_acct (xx).idr_status
                                            AND sca_status =
                                                arr_ba_acct (xx).sca_status
                                            AND applicability = 'MML_DETAIL'
                                            AND active_flag = 'Y'),
                                    arr_ba_acct (xx).idr_status,
                                    arr_ba_acct (xx).sca_status,
                                    arr_ba_acct (xx).idr_source,
                                    arr_ba_acct (xx).sca_source,
                                    p_meter_id)
                      RETURNING uid_alps_account
                           BULK COLLECT INTO arr_uid_acct;

                    gv_code_ln := $$plsql_line;

                    IF p_status_val <> 'DATA_PENDING'
                    THEN
                        IF arr_uid_acct.COUNT > 0
                        THEN
                            FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
                            LOOP
                                -- Retain the Original BA (HU/HI)Transaction
                                DBMS_OUTPUT.put_line (
                                    'BEFORE INSERT - Original Request (HU/HI)Transaction');

                                INSERT INTO alps_account_transaction
                                    SELECT alps_acct_txn_seq.NEXTVAL,
                                           created_dt,
                                           created_user,
                                           updated_dt,
                                           updated_user,
                                           uid_attachment,
                                           arr_uid_acct (ix),
                                           transact_type,
                                           status,
                                           description,
                                           txn_indicator
                                      FROM alps_account_transaction
                                     WHERE     uid_alps_account =
                                               arr_ba_acct (xx).uid_alps_account
                                           AND txn_indicator =
                                               DECODE (p_status_ind,
                                                       'IDR_STATUS', 'HI',
                                                       'HU');

                                -- Create new transactions for incoming BA_SA
                                gv_code_ln := $$plsql_line;
                                arr_acct_txn (1).uid_attachment := p_uid_file;
                                arr_acct_txn (1).uid_alps_account :=
                                    arr_uid_acct (ix);
                                arr_acct_txn (1).status := p_status_val;
                                arr_acct_txn (1).transact_type :=
                                    lv_att_file_type;
                                arr_acct_txn (1).description := p_description;
                                --
                                gv_code_ln := $$plsql_line;
                                
                                arr_acct_txn (1).txn_indicator := 'HU';
                                DBMS_OUTPUT.put_line (
                                    'BEFORE INSERT - new HUTransaction');
                                alps_request_pkg.setup_acct_transact (
                                    gv_source_id,
                                    arr_acct_txn);
                            END LOOP;
                        END IF;
                    END IF;
                END LOOP;
            END IF;
        END IF;
        --
        arr_quote_id.delete;
        arr_uid_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            arr_quote_id.delete;
            arr_uid_acct.delete;
            --  ALPS_MML_PKG.PROCESS_LOGGING(gv_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_stage,gv_source_type);
            --ROLLBACK;
            RAISE;
    END update_pr_detail_status;


    PROCEDURE pe_staging_data_chk (p_market_code   IN     VARCHAR2,
                                   p_disco_code    IN     VARCHAR2,
                                   p_ldc_account   IN     VARCHAR2,
                                   p_status           OUT VARCHAR2,
                                   p_status_desc      OUT VARCHAR2)
    AS
        lv_uid_account                  alps_account.uid_account%TYPE;
        lv_market_code                  alps_account.market_code%TYPE;
        lv_disco_code                   alps_account.disco_code%TYPE;
        lv_ldc_account                  alps_account.ldc_account%TYPE;
        lv_rate_class                   alps_account.rate_class%TYPE;
        lv_loss_factor                  alps_account.loss_factor%TYPE;
        lv_updated_dt                   alps_account.updated_dt%TYPE;
        lv_load_profile                 alps_account.load_profile%TYPE;
        lv_voltage                      alps_account.voltage%TYPE;
        lv_valid_zone                   alps_account.zone%TYPE;
        lv_lwsp_flag                    alps_account.lwsp_flag%TYPE;
        lv_zone_cnt                     INTEGER;
        lv_meter_type                   alps_account_raw.meter_type%TYPE;
        lv_cap_tag                      INTEGER;
        lv_trans_tag                    INTEGER;
        --Usage
        lv_start_stop_is_null           VARCHAR2 (3) := 'NO';
        lv_start_stop_is_missing        VARCHAR2 (3) := 'NO';   
        -- ICAP   
        lv_captag_is_null               VARCHAR2 (3) := 'NO';
        lv_captag_is_missing            VARCHAR2 (3) := 'NO';
        lv_captag_is_zero               VARCHAR2 (3) := 'NO';     
        -- TRANS       
        lv_transtag_is_null             VARCHAR2 (3) := 'NO';
        lv_transtag_is_missing          VARCHAR2 (3) := 'NO';             
        lv_transtag_is_zero               VARCHAR2 (3) := 'NO';      
        
        vDataTxn                        VARCHAR2 (3);
        vEdiTag                         VARCHAR2 (3);

        vNimoUsghFile                   VARCHAR2 (3);
        vexpressflg                     ALPS_MML_PR_HEADER.EXPRESS_FLAG%TYPE;
    /*
     Purpose :  The purpose is validate values of key data fields in the PE staging tables for specific disco.
                     1. If all required fields having non-NULL values then set detail status = 'DATA_RECEIVED'
                     2. If any required fields having NULL values then set detail status = 'DATA_ERROR'

     Parameter :
                       p_market_code - Meter id
                        p_disco_code - Meter type. Values are IDR/SCA
                        p_ldc_account - Indicte whether to update individual IDR or SCA Status. Values are IDR_STATUS, SCA_STATUS
                        p_status - Detailed description

     Execution :  This procedure executed inside procedure UPDATE_PARSED_SCALAR_ACCTS
    */

BEGIN
    
    gv_code_proc := 'PE_STAGING_DATA_CHK';
    gv_source_id :=
        p_market_code || '_' || p_disco_code || '_' || p_ldc_account;
    gv_source_type := 'METER_ID';
    gv_stage := 'GATHERING';
    DBMS_OUTPUT.put_line ('**PE_STAGING_DATA_CHK** : PROCESSING  p_market_code  ==> ' || p_market_code);
        DBMS_OUTPUT.put_line ('**PE_STAGING_DATA_CHK** : PROCESSING  p_disco_code  ==> ' || p_disco_code);
            DBMS_OUTPUT.put_line ('**PE_STAGING_DATA_CHK** : PROCESSING  p_ldc_account  ==> ' || p_ldc_account);
        
    -- Check for matrix/express flag
    BEGIN
        SELECT NVL (express_flag, 'N')
          INTO vexpressflg
          FROM alps_mml_pr_header  A,
               (SELECT MAX (HDR.uid_mml_master)     uid_mml_master
                  FROM alps_mml_pr_detail dtl1, alps_mml_pr_header hdr
                 WHERE     dtl1.market_code = p_market_code
                       AND dtl1.disco_code = p_disco_code
                       AND (   dtl1.ldc_account = p_ldc_account
                            OR p_ldc_account LIKE dtl1.ldc_account || '%')
                       AND dtl1.sca_status IN ('REQUEST_COMPLETE',
                                               'REQUEST_PENDING',
                                               'DATA_ERROR',
                                               'REQUEST_FAILED',
                                               'WEB_SCRAPE_PENDING')
                       AND dtl1.uid_mml_master = hdr.uid_mml_master
                       AND dtl1.sbl_quote_id = hdr.sbl_quote_id
                       AND hdr.active_flg = 'Y'
                       AND hdr.status = 'PENDING') B
         WHERE a.uid_mml_master = b.uid_mml_master;
    EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
            vexpressflg := 'N';
    END;

    DBMS_OUTPUT.put_line ('vexpressflg ==> ' || vexpressflg);

    -- NIMO file typoe check
    select DECODE(count(1),0,'NO','YES')
    into vNimoUsghFile
    from  alps_parser_instance a ,
    (select max(uid_batch) uid_batch
    from alps_parser_sca_final
    where  disco_code = 'NIMO'
    and  disco_code = p_disco_code
    and  ldc_account = p_ldc_account
    ) b
    where a.uid_batch = b.uid_batch
    and simx_parser_name = 'EDI_USGH_SIMX';
        DBMS_OUTPUT.PUT_LINE ('vNimoUsghFile : '||vNimoUsghFile);

    --Delete Old Capacity and Transmission Tags with Blank values
    DELETE FROM
        alps_account_attributes a
          WHERE     a.attr_code IN ('CAPACITY_TAG', 'TRANSMISSION_TAG')
                AND NVL (a.stop_time, SYSDATE - 1) < SYSDATE
                AND a.value1 IS NULL
                AND a.uid_account =
                    (SELECT MAX (uid_account)
                       FROM alps_account b
                      WHERE     b.market_code = p_market_code
                            AND b.disco_code = p_disco_code
                            AND b.ldc_account = p_ldc_account);

    --
    IF p_market_code IN ('PJM',
                         'NEPOOL',
                         'NYISO',
                         'ERCOT',
                         'MISO')
    THEN
        gv_code_ln := $$plsql_line;

        SELECT a.uid_account,
               a.market_code,
               a.disco_code,
               a.ldc_account,
               a.rate_class,
               a.voltage,
               a.load_profile,
               DECODE (a.disco_code, 'FGE', 'WCMASS', a.zone)     zone,
               a.meter_type,
               NVL(lwsp_flag,'N')
          INTO lv_uid_account,
               lv_market_code,
               lv_disco_code,
               lv_ldc_account,
               lv_rate_class,
               lv_voltage,
               lv_load_profile,
               lv_valid_zone,
               lv_meter_type,
               lv_lwsp_flag
          FROM alps_account a
         WHERE     a.market_code = p_market_code
               AND a.disco_code = p_disco_code
               AND a.ldc_account = p_ldc_account
               AND a.uid_account =
                   (SELECT MAX (uid_account)
                      FROM alps_account b
                     WHERE     b.market_code = p_market_code
                           AND b.disco_code = p_disco_code
                           AND b.ldc_account = p_ldc_account);

        --
        IF (    (   p_market_code = 'PJM'
                 OR p_market_code = 'MISO'
                 OR p_market_code = 'NYISO'
                 OR (    p_market_code = 'NEPOOL'
                     AND p_disco_code NOT IN ('UINH', 'NHECO')))
            AND lv_lwsp_flag = 'N')
        THEN
            SELECT NVL (value1, value2)     value1
              BULK COLLECT INTO arr_cap_tag
              FROM alps_account_attributes
             WHERE     uid_account = lv_uid_account
                   AND attr_code = 'CAPACITY_TAG'
             AND SYSDATE BETWEEN start_time AND stop_time
             and round(months_between(stop_time , start_time)) = 12;

           IF p_market_code = 'MISO'
           THEN      
             SELECT NVL (value1, value2)     value1
              BULK COLLECT INTO arr_cap_tag
              FROM alps_account_attributes
             WHERE     uid_account = lv_uid_account
                   AND attr_code = 'CAPACITY_TAG'
             AND SYSDATE BETWEEN start_time AND stop_time;
             
            END IF;
            DBMS_OUTPUT.PUT_LINE (
                'lv_uid_account ==> ' || lv_uid_account);
            DBMS_OUTPUT.PUT_LINE (
                'arr_cap_tag .COUNT ==> ' || arr_cap_tag.COUNT);

            --
            IF arr_cap_tag.COUNT > 0
            THEN
                FOR ix IN arr_cap_tag.FIRST .. arr_cap_tag.LAST
                LOOP
                    IF arr_cap_tag (ix).value1 IS NULL
                    THEN
                        lv_captag_is_null := 'YES';
                    
                    END IF;
                END LOOP;

            DBMS_OUTPUT.PUT_LINE ('lv_captag_is_null : '||lv_captag_is_null );
                    
            ELSIF arr_cap_tag.COUNT = 0
            THEN
                lv_captag_is_missing := 'YES';
            END IF;
        END IF;

        --
        IF (    (p_market_code IN ('PJM') AND p_disco_code <> 'RECO')
            AND lv_lwsp_flag = 'N')
        THEN
            SELECT NVL (value1, value2)     value1
              BULK COLLECT INTO arr_trans_tag
              FROM alps_account_attributes
             WHERE     uid_account = lv_uid_account
             AND attr_code = 'TRANSMISSION_TAG'
             AND SYSDATE BETWEEN start_time AND stop_time
             and round(months_between(stop_time , start_time)) = 12;                       

            --
            IF arr_trans_tag.COUNT > 0
            THEN
                FOR ix IN arr_trans_tag.FIRST .. arr_trans_tag.LAST
                LOOP
                    IF arr_trans_tag (ix).value1 IS NULL
                    THEN
                        lv_transtag_is_null := 'YES';
                    END IF;
                END LOOP;

            ELSIF arr_trans_tag.COUNT = 0
            THEN
                lv_transtag_is_missing := 'YES';
            END IF;
        END IF;

        --

        SELECT  stop_time, usage
          BULK COLLECT INTO arr_usg
          FROM alps_usage
         WHERE uid_account = lv_uid_account;


     /*   SELECT aa.stop_time, 
               usage
        BULK COLLECT INTO arr_usg                   
        FROM alps_usage aa,
        (
        SELECT a.uid_account,
        (SELECT max(stop_time)  from alps_usage where uid_account = a.uid_account) max_read_date
        FROM alps_usage a
         WHERE uid_account = lv_uid_account
         AND rownum = 1
        ) cc
        WHERE  aa.uid_account  = cc.uid_account
        AND aa.stop_time > add_months(cc.max_read_date,-12);
        */

        --
        IF arr_usg.COUNT > 0
        THEN
            FOR ix IN arr_usg.FIRST .. arr_usg.LAST
            LOOP
                IF    arr_usg (ix).stop_time IS NULL
                   OR arr_usg (ix).usage IS NULL
                THEN
                    lv_start_stop_is_null := 'YES';
                END IF;
            END LOOP;
        ELSIF arr_usg.COUNT = 0
        THEN
            lv_start_stop_is_missing := 'YES';
        END IF;

        --
        IF lv_valid_zone IS NOT NULL
        THEN
            SELECT COUNT (1)
              INTO lv_zone_cnt
              FROM alps_mml_lookup
             WHERE     lookup_group = 'DISCO_ZONE'
                   AND lookup_code = lv_disco_code
                   AND lookup_str_value1 = lv_valid_zone;

            --
            /*Check siebel zone for MISO and ERCOT*/
            FOR siebelZone
                IN (SELECT *
                      FROM v_alps_sbl_zone
                     WHERE     market IN ('ERCOT', 'MISO')
                           AND ZONE = NVL (lv_valid_zone, 'DUMMY'))
            LOOP
                lv_zone_cnt := 1;
            END LOOP;

            --

            IF lv_zone_cnt > 1 OR lv_zone_cnt = 0
            THEN
                lv_valid_zone := 'NO';
            ELSIF lv_zone_cnt = 1
            THEN
                lv_valid_zone := 'YES';
            END IF;
        ELSE
            SELECT COUNT (1)
              INTO lv_zone_cnt
              FROM alps_mml_lookup
             WHERE     lookup_group = 'DISCO_ZONE'
                   AND lookup_code = lv_disco_code;

            --
            IF    lv_zone_cnt > 1
               OR lv_zone_cnt = 0
               OR p_market_code IN ('ERCOT', 'MISO')
            THEN
                lv_valid_zone := 'NO';
            ELSIF lv_zone_cnt = 1
            THEN
                lv_valid_zone := 'YES';

                UPDATE alps_account
                   SET zone =
                           (SELECT lookup_str_value1
                              FROM alps_mml_lookup
                             WHERE     lookup_group = 'DISCO_ZONE'
                                   AND lookup_code = lv_disco_code)
                 WHERE uid_account = lv_uid_account;
            END IF;
        END IF;

        IF (   (    (   lv_rate_class IS NULL
                     OR lv_voltage IS NULL
                     OR lv_load_profile IS NULL
                     OR lv_meter_type IS NULL)
                AND lv_lwsp_flag = 'N')
            OR (    lv_rate_class IS NULL
                AND     -- LWSP matrix  RATE validation for non ERCOT only
                    lv_lwsp_flag = 'Y'
                AND lv_market_code <> 'ERCOT')
            OR (    lv_load_profile IS NULL
                AND  -- LWSP matrix  PROFILE validation for non ERCOT only
                    lv_lwsp_flag = 'Y'
                AND lv_market_code <> 'ERCOT'))
        THEN
            p_status := 'DATA_ERROR';

            IF lv_rate_class IS NULL
            THEN
                p_status_desc :=
                    'Rate Class with NULL value';
            ELSIF lv_voltage IS NULL
            THEN
                p_status_desc :=
                    'Voltage with NULL value';
            ELSIF lv_load_profile IS NULL
            THEN
                p_status_desc :=
                    'Load Profile with NULL value';
            ELSIF lv_meter_type IS NULL
            THEN
                p_status_desc :=
                    'Meter Type with NULL value';
            END IF;
        ELSE
            IF     lv_valid_zone = 'NO'
               AND lv_market_code IN ('PJM', 'NEPOOL', 'NYISO')
            THEN
                p_status := 'DATA_ERROR';
                p_status_desc :=
                    'Invalid Zone or Multi Zones';
            ELSE
                DBMS_OUTPUT.PUT_LINE (
                    'lv_captag_is_null ==> ' || lv_captag_is_null);
                DBMS_OUTPUT.PUT_LINE (
                    'lv_disco_code ==> ' || lv_disco_code);

                IF     (   lv_transtag_is_null = 'YES'
                        OR lv_captag_is_null = 'YES' 
                        OR lv_transtag_is_missing = 'YES'
                        OR lv_captag_is_missing = 'YES')
                   AND vexpressflg <> 'Y'
                THEN
                    p_status := 'DATA_ERROR';

                    --
                    IF     lv_captag_is_null = 'YES'
                       AND lv_transtag_is_null = 'YES'
                    THEN
                        p_status_desc :=
                            'ICAP/TRANS tag with NULL value';
                    ELSIF     lv_captag_is_null = 'YES'
                          AND lv_transtag_is_null = 'NO'
                    THEN
                        p_status_desc :=
                            'ICAP tag with NULL value';
                    ELSIF     lv_captag_is_null = 'NO'
                          AND lv_transtag_is_null = 'YES'
                    THEN
                        p_status_desc :=
                            'TRANS tag with NULL value';
                    ELSIF     lv_transtag_is_missing = 'YES'
                          AND lv_captag_is_missing = 'YES'
                    THEN
                        p_status_desc :=
                            'ICAP/TRANS tag are misisng';
                    ELSIF     lv_transtag_is_missing = 'YES'
                          AND lv_captag_is_missing = 'NO'
                    THEN
                        p_status_desc :=
                            'TRANS tag is missing';
                    ELSIF     lv_transtag_is_missing = 'NO'
                          AND lv_captag_is_missing = 'YES'
                    THEN
                        p_status_desc :=
                            'ICAP tag is missing';                        
                    END IF;
                ELSE
                    IF     lv_start_stop_is_null = 'YES' 
                     OR lv_start_stop_is_missing = 'YES'
                    THEN
                        p_status := 'DATA_ERROR';
                        --
                        IF lv_start_stop_is_null = 'YES' 
                        THEN
                            p_status_desc := 'Usage data with NULL Start/Stop time';                            
                        ELSIF lv_start_stop_is_missing = 'YES'
                        THEN
                           p_status_desc := 'Usage data is missing';                            
                        END IF;

                    ELSE
                        IF     p_disco_code = 'WMECO'
                           AND lv_rate_class = 'G2'
                        THEN
                            UPDATE alps_account
                               SET load_profile =
                                       (SELECT lookup_str_value1
                                          FROM alps_mml_lookup
                                         WHERE     lookup_group =
                                                   'LOAD_PROFILE'
                                               AND lookup_code =
                                                   'WMECO_G2')
                             WHERE     meter_type = 'SCALAR' /*Per Adam, do this for SCALAR only*/
                                   AND uid_account = lv_uid_account;
                        END IF;

                        --
                        p_status := 'DATA_RECEIVED';
                        p_status_desc := NULL;
                    END IF;
                END IF;
            END IF;
        END IF;
    ELSE
        DBMS_OUTPUT.put_line ('Other Markets - ALL OK ');
        p_status := 'DATA_RECEIVED';
        p_status_desc := NULL;
    END IF;

        DBMS_OUTPUT.put_line ('FINAL Status ==>'||p_status);
    --
    arr_cap_tag.delete;
    arr_trans_tag.delete;
    arr_usg.delete;

EXCEPTION
    WHEN OTHERS
    THEN
        arr_cap_tag.delete;
        arr_trans_tag.delete;
        arr_usg.delete;
        RAISE;
END pe_staging_data_chk;

    PROCEDURE update_parsed_scalar_accts (p_meter_id         IN VARCHAR2,
                                          p_meter_type       IN VARCHAR2,
                                          p_status_ind       IN VARCHAR2,
                                          p_status_val       IN VARCHAR2,
                                          p_description      IN VARCHAR2,
                                          p_uid_file         IN NUMBER,
                                          p_exception_type   IN VARCHAR2,
                                          p_exception_code   IN VARCHAR2,
                                          p_exception_desc   IN VARCHAR2,
                                          p_assigned         IN VARCHAR2)
    AS
        /*
            Purpose : For every account(meter) scalar data that was parsed :
                            1. If successfully,   Loaded scalar data into PE Staging and update the scalar status(SCA_STATUS)
                            2. If Un-successfully,  log the exception then update the scalar status(SCA_STATUS)

            Parameter :
                              p_meter_id - Meter id
                               p_meter_type - Meter type. Values are IDR/SCA
                               p_status_ind - Indicte whether to update individual IDR or SCA Status. Values are IDR_STATUS, SCA_STATUS
                               p_description - Detailed description
                               p_uid_file  -  REsponse file UID
                               p_exception_type - Predifined Exception type
                               p_exception_code  - Predifined Exception code
                               p_exception_desc  - Detailed exception description
                               p_assigned  - Analyst assigned to work on this exception

            Execution :  This procedure executed  via table (ALPS_ACCOUNT_RAW trigger(ALPS_ACCOUNT_RAW_AIU) .
        */
        lv_insert_status     VARCHAR2 (30);
        lv_out_description   VARCHAR2 (500);
        lv_meter_type        alps_mml_pr_detail.meter_type%TYPE;
        lv_status            alps_mml_pr_detail.status%TYPE;
        lv_status_ind        alps_mml_pr_detail.sca_status%TYPE;
        lv_status_desc       alps_mml_pr_detail.description%TYPE;

        --
        TYPE tabquoteid IS TABLE OF alps_mml_pr_detail.sbl_quote_id%TYPE
            INDEX BY PLS_INTEGER;

        vquoteid             tabquoteid;
        voutstatus           VARCHAR2 (60);
        voutdesc             VARCHAR2 (500);
    BEGIN
        gv_code_proc := 'UPDATE_PARSED_SCALAR_ACCTS';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        gv_stage := 'GATHERING';
        --
        DBMS_OUTPUT.put_line (
               'Initial meter type ==> '
            || p_meter_type
            || ', p_status_val ==> '
            || p_status_val);

        IF p_meter_type IN ('SCALAR', 'SCA')
        THEN
            lv_meter_type := 'SCALAR';
        ELSIF p_meter_type = 'IDR'
        THEN
            lv_meter_type := 'IDR';
        ELSE
            lv_meter_type := 'UNKNOWN';
        END IF;

        DBMS_OUTPUT.put_line ('Final meter type ==> ' || lv_meter_type);
        --
        gv_code_ln := $$plsql_line;

        IF p_status_val = 'DATA_RECEIVED'
        THEN

            gv_code_ln := $$plsql_line;
            insert_pe_staging (SUBSTR (p_meter_id, 1, INSTR (p_meter_id, '_', 1, 1) - 1), 
                               SUBSTR (p_meter_id, INSTR (p_meter_id,'_', 1, 1)+ 1,( ( INSTR (p_meter_id,'_', 1, 2) - 1)- INSTR (p_meter_id, '_',1, 1))),
                               SUBSTR (p_meter_id, INSTR (p_meter_id, '_', 1, 2) + 1),
                               lv_insert_status,
                               lv_out_description);
            DBMS_OUTPUT.put_line ('After insert_pe_staging: lv_insert_status ==> ' || lv_insert_status);

            --
            IF lv_insert_status IS NOT null
            THEN
                -- Validate required Fields
                gv_code_ln := $$plsql_line;
                pe_staging_data_chk (SUBSTR (p_meter_id, 1,INSTR (p_meter_id,'_', 1, 1) - 1),
                                     SUBSTR (p_meter_id, INSTR (p_meter_id, '_',1,1) + 1,(  (  INSTR (p_meter_id, '_', 1,2)- 1)- INSTR (p_meter_id, '_',1,1))),
                                     SUBSTR (p_meter_id,INSTR (p_meter_id, '_', 1, 2) + 1),
                                     lv_status,
                                     lv_status_desc);
                --

                DBMS_OUTPUT.put_line (
                       'After Pe_Staging_Data_Chk ,  lv_status ==> '
                    || lv_status);

                -- Additional  Data check for DISCO that request and received  both EDI and WEB SCRAPE  data
                UTILITY_DATA_CHK (SUBSTR (p_meter_id,
                                          1,
                                            INSTR (p_meter_id,
                                                   '_',
                                                   1,
                                                   1)
                                          - 1),
                                  SUBSTR (p_meter_id,
                                            INSTR (p_meter_id,
                                                   '_',
                                                   1,
                                                   1)
                                          + 1,
                                          (  (  INSTR (p_meter_id,
                                                       '_',
                                                       1,
                                                       2)
                                              - 1)
                                           - INSTR (p_meter_id,
                                                    '_',
                                                    1,
                                                    1))),
                                  SUBSTR (p_meter_id,
                                            INSTR (p_meter_id,
                                                   '_',
                                                   1,
                                                   2)
                                          + 1),
                                  lv_status,
                                  lv_status_desc);
                DBMS_OUTPUT.put_line (
                    ' AFTER UTILITY_DATA_CHK - lv_status ==> ' || lv_status);
                    
                -- Once account successfully loaded into PE Staging tables, Update the account individual status in PR_DETAIL
                update_pr_detail_status (p_meter_id,
                                         lv_meter_type,
                                         lv_insert_status,
                                         lv_status,            --p_status_val,
                                         NVL (lv_status_desc, p_description),
                                         p_uid_file);
                DBMS_OUTPUT.put_line (
                       'AFTER  UPDATE_PR_DETAIL_STATUS  lv_status ==> '
                    || lv_status);
            END IF;
        ELSE    -- data error - log the error and Update the individual status
            insert_exception_log (p_meter_id,
                                  p_status_val,
                                  p_exception_desc,
                                  p_assigned,
                                  p_exception_type,
                                  p_exception_code,
                                  lv_insert_status);

            --
            IF lv_insert_status = 'SUCCESS'
            THEN
                update_pr_detail_status (p_meter_id,
                                         lv_meter_type,
                                         p_status_ind,
                                         p_status_val,
                                         p_description,
                                         p_uid_file);
            END IF;
        END IF;

        DBMS_OUTPUT.put_line ('Before Sync Address data ');

        -- Sync Address data
        DECLARE
        BEGIN
            FOR REC IN (SELECT DISTINCT A.UID_ACCOUNT,
                                        A.METER_ID,
                                        A.ADDRESS1,
                                        A.ADDRESS2,
                                        A.CITY,
                                        A.STATE,
                                        A.ZIP,
                                        B.ADDRESS1     SRC_ADDRESS1,
                                        B.ADDRESS2     SRC_ADDRESS2,
                                        B.CITY         SRC_CITY,
                                        B.STATE        SRC_STATE,
                                        B.ZIP          SRC_ZIP
                          FROM ALPS_ACCOUNT A, ALPS_MML_PR_DETAIL B
                         WHERE     A.METER_ID = p_meter_id
                               AND A.METER_ID = B.METER_ID
                               AND (   REGEXP_INSTR (A.ADDRESS1,
                                                     'UNKNOWN',
                                                     1,
                                                     1,
                                                     0,
                                                     'i') > 0
                                    OR A.ADDRESS1 IS NULL)
                               AND REGEXP_INSTR (B.ADDRESS1,
                                                 'UNKNOWN',
                                                 1,
                                                 1,
                                                 0,
                                                 'i') = 0)
            LOOP
                DBMS_OUTPUT.put_line ('in update - ALPS_ACCOUNT ');

                UPDATE ALPS_ACCOUNT
                   SET ADDRESS1 = REC.SRC_ADDRESS1,
                       ADDRESS2 = REC.SRC_ADDRESS2,
                       CITY = REC.SRC_CITY,
                       STATE = REC.SRC_STATE,
                       ZIP = REC.SRC_ZIP
                 WHERE UID_ACCOUNT = REC.UID_ACCOUNT;
            END LOOP;

            COMMIT;
        END;

        DECLARE
        BEGIN
            FOR REC IN (SELECT DISTINCT B.UID_ALPS_ACCOUNT,
                                        B.METER_ID,
                                        B.ADDRESS1,
                                        B.ADDRESS2,
                                        B.CITY,
                                        B.STATE,
                                        B.ZIP,
                                        A.ADDRESS1     SRC_ADDRESS1,
                                        A.ADDRESS2     SRC_ADDRESS2,
                                        A.CITY         SRC_CITY,
                                        A.STATE        SRC_STATE,
                                        A.ZIP          SRC_ZIP
                          FROM ALPS_ACCOUNT A, ALPS_MML_PR_DETAIL B
                         WHERE     A.METER_ID = p_meter_id
                               AND A.METER_ID = B.METER_ID
                               AND (    REGEXP_INSTR (A.ADDRESS1,
                                                      'UNKNOWN',
                                                      1,
                                                      1,
                                                      0,
                                                      'i') = 0
                                    AND A.ADDRESS1 IS NOT NULL)
                               AND (   REGEXP_INSTR (B.ADDRESS1,
                                                     'UNKNOWN',
                                                     1,
                                                     1,
                                                     0,
                                                     'i') > 0
                                    OR B.ADDRESS1 IS NULL))
            LOOP
                DBMS_OUTPUT.put_line ('in update - ALPS_MML_PR_DETAIL ');

                UPDATE ALPS_MML_PR_DETAIL
                   SET ADDRESS1 = REC.SRC_ADDRESS1,
                       ADDRESS2 = REC.SRC_ADDRESS2,
                       CITY = REC.SRC_CITY,
                       STATE = REC.SRC_STATE,
                       ZIP = REC.SRC_ZIP
                 WHERE UID_ALPS_ACCOUNT = REC.UID_ALPS_ACCOUNT;
            END LOOP;

            COMMIT;
        END;

        --
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_parsed_scalar_accts;

    PROCEDURE update_parsed_idr_accts (p_meter_id         IN VARCHAR2,
                                       p_meter_type       IN VARCHAR2,
                                       p_status_ind       IN VARCHAR2,
                                       p_status_val       IN VARCHAR2,
                                       p_description      IN VARCHAR2,
                                       p_uid_file         IN NUMBER,
                                       p_exception_type   IN VARCHAR2,
                                       p_exception_code   IN VARCHAR2,
                                       p_exception_desc   IN VARCHAR2,
                                       p_assigned         IN VARCHAR2)
    AS
        /*
            Purpose : For every account(meter) IDR data that was parsed :
                            1. If successfully,   update the IDR status(IDR_STATUS)
                            2. If Un-successfully, log the exception then update the IDR status(IDR_STATUS)

            Parameter :
                              p_meter_id - Meter id
                               p_meter_type - Meter type. Values are IDR/SCA
                               p_status_ind - Indicte whether to update individual IDR or SCA Status. Values are IDR_STATUS, SCA_STATUS
                               p_description - Detailed description
                               p_uid_file  -  REsponse file UID
                               p_exception_type - Predifined Exception type
                               p_exception_code  - Predifined Exception code
                               p_exception_desc  - Detailed exception description
                               p_assigned  - Analyst assigned to work on this exception

            Execution :  This procedure executed  via table (ALPS_ACCOUNT_RAW trigger(ALPS_ACCOUNT_RAW_AIU) .
        */
        lv_insert_status     VARCHAR2 (15);
        lv_out_description   VARCHAR2 (500);
        lv_meter_type        alps_mml_pr_detail.meter_type%TYPE;
    BEGIN
        gv_code_proc := 'UPDATE_PARSED_IDR_ACCTS';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';
        gv_stage := 'GATHERING';

        --
        IF p_meter_type = 'SCALAR'
        THEN
            lv_meter_type := 'SCA';
        ELSE
            lv_meter_type := 'IDR';
        END IF;

        gv_code_ln := $$plsql_line;

        IF p_status_val = 'DATA_RECEIVED'
        THEN
            update_pr_detail_status (p_meter_id,
                                     lv_meter_type,
                                     p_status_ind,
                                     p_status_val,
                                     p_description,
                                     p_uid_file);
        ELSE    -- data error - log the error and Update the individual status
            insert_exception_log (p_meter_id,
                                  p_status_val,
                                  p_exception_desc,
                                  p_assigned,
                                  p_exception_type,
                                  p_exception_code,
                                  lv_insert_status);

            --
            IF lv_insert_status = 'SUCCESS'
            THEN
                update_pr_detail_status (p_meter_id,
                                         lv_meter_type,
                                         p_status_ind,
                                         p_status_val,
                                         p_description,
                                         p_uid_file);
            END IF;
        END IF;

        --
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_stage,
                gv_source_type);
            ROLLBACK;
            RAISE;
    END update_parsed_idr_accts;

    FUNCTION f_insert_alps_account_raw (p_uid_batch       IN NUMBER,
                                        p_simx_parser     IN VARCHAR2,
                                        p_ldc_account     IN VARCHAR2,
                                        p_market_code     IN VARCHAR2,
                                        p_disco_code      IN VARCHAR2,
                                        p_version         IN NUMBER,
                                        p_address1        IN VARCHAR2,
                                        p_address2        IN VARCHAR2,
                                        p_city            IN VARCHAR2,
                                        p_state           IN VARCHAR2,
                                        p_zip             IN VARCHAR2,
                                        p_rate_class      IN VARCHAR2,
                                        p_rate_subclass   IN VARCHAR2,
                                        p_strata          IN FLOAT,
                                        p_voltage         IN VARCHAR2,
                                        p_load_profile    IN VARCHAR2,
                                        p_meter_cycle     IN VARCHAR2,
                                        p_station_id      IN VARCHAR2,
                                        p_zone            IN VARCHAR2,
                                        p_meter_type      IN VARCHAR2,
                                        p_county          IN VARCHAR2,
                                        p_country         IN VARCHAR2,
                                        p_advance_meter   IN VARCHAR2,
                                        P_load_type          VARCHAR2)
        RETURN NUMBER
    AS
        uidaccountraw   NUMBER (9);
        lv_version      alps_account_raw.version%TYPE;
    BEGIN
        gv_code_proc := 'F_INSERT_ALPS_ACCOUNT_RAW';
        gv_source_id := p_ldc_account;
        gv_source_type := 'LDC_ACCOUNT';

        --
        IF P_load_type = 'ODM'
        THEN
            --uidaccountraw:= ODM_ACCT_RAW_SEQ.NEXTVAL;
            --Load CDA account
            BEGIN
                INSERT INTO alps.odm_account_raw (uid_account_raw,
                                                  ldc_account,
                                                  market_code,
                                                  disco_code,
                                                  version,
                                                  address1,
                                                  address2,
                                                  city,
                                                  state,
                                                  zip,
                                                  rate_class,
                                                  rate_subclass,
                                                  strata,
                                                  voltage,
                                                  load_profile,
                                                  meter_cycle,
                                                  station_id,
                                                  zone,
                                                  meter_type,
                                                  county,
                                                  country,
                                                  acct_status)
                     VALUES (
                                uidaccountraw,
                                p_ldc_account,
                                p_market_code,
                                p_disco_code,
                                  ALPS.ODM_PKG.f_getmaxversion (
                                      p_market_code,
                                      p_disco_code,
                                      p_ldc_account)
                                + 1,
                                p_address1,
                                p_address2,
                                p_city,
                                p_state,
                                p_zip,
                                p_rate_class,
                                p_rate_subclass,
                                p_strata,
                                p_voltage,
                                p_load_profile,
                                p_meter_cycle,
                                p_station_id,
                                DECODE (p_disco_code,
                                        'ORU', NVL (p_zone, 'G'),
                                        p_zone),
                                p_meter_type,
                                p_county,
                                p_country,
                                'DATA_RECEIVED')
                  RETURNING uid_account_raw
                       INTO uidaccountraw;
            END;
        ELSIF P_load_type = 'CDA'
        THEN
            --uidaccountraw:= CDA_ACCT_RAW_SEQ.NEXTVAL;
            --Load CDA account
            BEGIN
                INSERT INTO alps.cda_account_raw (uid_account_raw,
                                                  ldc_account,
                                                  market_code,
                                                  disco_code,
                                                  version,
                                                  address1,
                                                  address2,
                                                  city,
                                                  state,
                                                  zip,
                                                  rate_class,
                                                  rate_subclass,
                                                  strata,
                                                  voltage,
                                                  load_profile,
                                                  meter_cycle,
                                                  station_id,
                                                  zone,
                                                  meter_type,
                                                  county,
                                                  country,
                                                  acct_status,
                                                  uid_file,
                                                  exception_assigned)
                     VALUES (
                                uidaccountraw,
                                p_ldc_account,
                                p_market_code,
                                p_disco_code,
                                  ALPS.CDA_PKG.f_getmaxversion (
                                      p_market_code,
                                      p_disco_code,
                                      p_ldc_account)
                                + 1,
                                p_address1,
                                p_address2,
                                p_city,
                                p_state,
                                p_zip,
                                p_rate_class,
                                p_rate_subclass,
                                p_strata,
                                p_voltage,
                                p_load_profile,
                                p_meter_cycle,
                                p_station_id,
                                DECODE (p_disco_code,
                                        'ORU', NVL (p_zone, 'G'),
                                        p_zone),
                                p_meter_type,
                                p_county,
                                p_country,
                                'DATA_RECEIVED',
                                p_uid_batch,
                                p_simx_parser)
                  RETURNING uid_account_raw
                       INTO uidaccountraw;
            END;
        ELSE
            uidaccountraw := alps_acct_raw_seq.NEXTVAL;

            --
            INSERT INTO alps.alps_account_raw (uid_account_raw,
                                               created_user,
                                               updated_user,
                                               ldc_account,
                                               market_code,
                                               disco_code,
                                               version,
                                               address1,
                                               address2,
                                               city,
                                               state,
                                               zip,
                                               rate_class,
                                               rate_subclass,
                                               strata,
                                               voltage,
                                               load_profile,
                                               meter_cycle,
                                               station_id,
                                               zone,
                                               meter_type,
                                               county,
                                               country,
                                               acct_status,
                                               adv_meter_flag,
                                               uid_file,
                                               exception_assigned)
                     VALUES (
                                uidaccountraw,
                                USER,
                                USER,
                                p_ldc_account,
                                p_market_code,
                                p_disco_code,
                                  f_getmaxversion (p_market_code,
                                                   p_disco_code,
                                                   p_ldc_account)
                                + 1,
                                REPLACE (REPLACE (p_address1, '''', ''),
                                         ',',
                                         ''),
                                REPLACE (REPLACE (p_address2, '''', ''),
                                         ',',
                                         ''),
                                p_city,
                                p_state,
                                p_zip,
                                p_rate_class,
                                p_rate_subclass,
                                p_strata,
                                p_voltage,
                                p_load_profile,
                                p_meter_cycle,
                                p_station_id,
                                DECODE (p_disco_code,
                                        'ORU', NVL (p_zone, 'G'),
                                        p_zone),
                                p_meter_type,
                                p_county,
                                p_country,
                                'DATA_RECEIVED',
                                p_advance_meter,
                                p_uid_batch,
                                p_simx_parser);


            /***********************
             Handle Meter Number Changes
            ************************/
            BEGIN
                FOR rec
                    IN (SELECT DISTINCT a.uid_alps_account, c.newldcaccount
                          FROM alps_mml_pr_detail  a,
                               alps_mml_pr_header  b,
                               (SELECT p_ldc_account       newldcaccount,
                                       p_rate_subclass     oldldcaccount,
                                       p_disco_code        disco_code
                                  FROM DUAL
                                 WHERE     p_rate_subclass IS NOT NULL
                                       AND p_ldc_account != p_rate_subclass
                                       AND p_disco_code IN
                                               (SELECT DISTINCT lookup_code
                                                  FROM alps_mml_lookup
                                                 WHERE lookup_group =
                                                       'DISCO_NUMBER_CHANGE'))
                               c
                         WHERE     NVL (a.sca_status, 'DUMMY') !=
                                   'PE_SETUP_COMPLETE'
                               AND b.active_flg = 'Y'
                               AND a.uid_mml_master = b.uid_mml_master
                               AND a.disco_code = c.disco_code
                               AND a.ldc_account = c.oldldcaccount)
                LOOP
                    UPDATE alps_mml_pr_detail
                       SET ldc_account = rec.newldcaccount,
                           idr_status =
                               DECODE (
                                   idr_status,
                                   'PE_SETUP_COMPLETE', 'REQUEST_COMPLETE',
                                   idr_status)
                     WHERE uid_alps_account = rec.uid_alps_account;
                END LOOP;
            EXCEPTION
                WHEN OTHERS
                THEN
                    DBMS_OUTPUT.put_line (SQLERRM);
                    NULL;
            END;
        END IF;

        --
        RETURN uidaccountraw;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (gv_source_id,
                                          'Procedure: ' || gv_code_proc,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          SQLCODE,
                                          SUBSTR (SQLERRM, 1, 200),
                                          NULL,
                                          gv_source_type);
            RAISE;
    END f_insert_alps_account_raw;


    PROCEDURE insert_edi_acct_attr_raw (p_new_uid_acct_raw   IN     VARCHAR2,
                                        p_meter_id           IN     VARCHAR2,
                                        p_out_status            OUT VARCHAR2,
                                        p_out_desc              OUT VARCHAR2)
    AS
        /*
            Purpose :   For EDI, Load incoming CAP-TRANS from interim table into ALPS_ACCT_ATR_RAW

            Parameter :
                               p_new_uid_acct_raw -  Primary key(uid_account_raw ) of ALPS_ACCOUNT_RAW
                              p_meter_id - Meter id
                              p_out_status  - status of the procedure call returned to calling program(process). Values are SUCCESS, ERROR
                              p_out_description - Description of the function call returned to calling program(process).

            Execution :  This procedure executed  via table ALPS_ACCOUNT_RAW trigger (ALPS_ACCOUNT_RAW_AIU) .
        */
        lv_new_start_time   alps_acct_cap_trans_raw.start_time%TYPE;
        lv_new_stop_time    alps_acct_cap_trans_raw.stop_time%TYPE;
        lv_start_time       alps_account_attr_raw.start_time%TYPE;
        lv_stop_time        alps_account_attr_raw.stop_time%TYPE;
        lv_uid_acct_raw     alps_account_raw.uid_account_raw%TYPE;
        lv_total_qty        alps_acct_cap_trans_raw.qty%TYPE;
        lv_value1           alps_account_attr_raw.value1%TYPE;
        lv_value2           alps_account_attr_raw.value2%TYPE;
        lv_disco_code       alps_acct_cap_trans_raw.disco_code%TYPE;
    BEGIN
        gv_code_proc := 'INSERT_EDI_ACCT_ATTR_RAW';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';

        --
        --   SELECT SUBSTR(p_meter_id,INSTR(p_meter_id,'_',1,1)+1, ((INSTR(p_meter_id,'_',1,2)-1) - INSTR(p_meter_id,'_',1,1))  )
        --    INTO lv_disco_code
        --    FROM DUAL;
        --
        IF SUBSTR (p_meter_id,
                     INSTR (p_meter_id,
                            '_',
                            1,
                            1)
                   + 1,
                   (  (  INSTR (p_meter_id,
                                '_',
                                1,
                                2)
                       - 1)
                    - INSTR (p_meter_id,
                             '_',
                             1,
                             1))) NOT IN ('WMECO', 'CLP', 'PSNH')
        THEN
              -- Get incoming or new CAP-TRANS tag
              SELECT MAX (a.qty)                    qty,
                     a.start_time,
                     a.stop_time,
                     DECODE (a.tag_type,
                             'CAP_TAG', 'CAPACITY_TAG',
                             'TRANSMISSION_TAG')    tag_type
                BULK COLLECT INTO arr_cap_trans
                FROM alps_acct_cap_trans_raw a,
                     (  SELECT MAX (created_dt)     created_dt,
                               market_code,
                               disco_code,
                               ldc_account,
                               tag_type
                          FROM alps_acct_cap_trans_raw
                         WHERE        market_code
                                   || '_'
                                   || disco_code
                                   || '_'
                                   || ldc_account =
                                   p_meter_id
                               AND TRUNC (created_dt) = TRUNC (SYSDATE)
                      GROUP BY market_code,
                               disco_code,
                               ldc_account,
                               tag_type) a1
               WHERE     a.market_code = a1.market_code
                     AND a.disco_code = a1.disco_code
                     AND a.ldc_account = a1.ldc_account
                     AND a.created_dt = a1.created_dt
                     AND a.tag_type = a1.tag_type
            GROUP BY a.start_time, a.stop_time, a.tag_type;

            --
            DBMS_OUTPUT.put_line (
                   'NEW:  lv_new_start_time ==> '
                || lv_new_start_time
                || ',  lv_new_stop_time ==> '
                || lv_new_stop_time
                || ',  lv_total_qty ==> '
                || lv_total_qty);

            IF arr_cap_trans.COUNT > 0
            THEN
                FOR ix IN arr_cap_trans.FIRST .. arr_cap_trans.LAST
                LOOP
                    -- remove previously loaded for the same account, tag, start/stop time
                    DELETE FROM
                        alps_account_attr_raw
                          WHERE     uid_account_raw = p_new_uid_acct_raw
                                AND attr_code = arr_cap_trans (ix).tag_type
                                AND start_time =
                                    arr_cap_trans (ix).start_time
                                AND TRUNC (stop_time) =
                                    TRUNC (arr_cap_trans (ix).stop_time);

                    --  insert latest CAP-TRANS tags
                    INSERT INTO alps_account_attr_raw (uid_attr,
                                                       uid_account_raw,
                                                       attr_code,
                                                       attr_type,
                                                       start_time,
                                                       stop_time,
                                                       value1,
                                                       value2,
                                                       str_value1,
                                                       str_value2)
                         VALUES (alps_acct_attr_raw_seq.NEXTVAL,
                                 p_new_uid_acct_raw,
                                 --                CASE arr_cap_trans(ix).tag_type
                                 --                WHEN 'CAP_TAG' THEN 'CAPACITY_TAG'
                                 --                WHEN 'TRANS_TAG' THEN 'TRANSMISSION_TAG'
                                 --END ,
                                 arr_cap_trans (ix).tag_type,
                                 'Planning Period',
                                 arr_cap_trans (ix).start_time,
                                 arr_cap_trans (ix).stop_time,
                                 arr_cap_trans (ix).qty,
                                 arr_cap_trans (ix).qty,
                                 'H',
                                 'H');
                END LOOP;
            END IF;

            --
            DELETE FROM
                alps_acct_cap_trans_raw
                  WHERE    market_code
                        || '_'
                        || disco_code
                        || '_'
                        || ldc_account =
                        p_meter_id;
        END IF;

        --
        -- COMMIT;
        p_out_status := 'SUCCESS';
        p_out_desc := NULL;
    EXCEPTION
        WHEN OTHERS
        THEN
            -- Set up the error message to be return to calling App.
            p_out_status := 'ERROR';
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();
            alps_mml_pkg.process_logging (
                gv_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                NULL,
                gv_source_type);
            -- ROLLBACK;
            RAISE;
    END insert_edi_acct_attr_raw;


    PROCEDURE insert_alps_acct_attr_raw (p_uid_account_raw   IN NUMBER,
                                         p_attr_code         IN VARCHAR2,
                                         p_start_time        IN DATE,
                                         p_stop_time         IN DATE,
                                         p_value1            IN VARCHAR2,
                                         p_str_value1        IN VARCHAR2,
                                         p_load_type         IN VARCHAR2)
    AS
    BEGIN
        gv_code_proc := 'INSERT_ALPS_ACCT_ATTR_RAW';
        gv_source_id := p_attr_code;
        gv_source_type := 'ATTR_CODE';

        --
        IF p_load_type = 'ODM'
        THEN
            BEGIN
                INSERT INTO alps.ODM_ACCOUNT_ATTR_RAW (uid_account_raw,
                                                       attr_code,
                                                       attr_type,
                                                       start_time,
                                                       stop_time,
                                                       value1,
                                                       str_value1)
                     VALUES (p_uid_account_raw,
                             p_attr_code,
                             'Planning Period',
                             p_start_time,
                             p_stop_time,
                             p_value1,
                             p_str_value1);
            END;
        ELSIF p_load_type = 'CDA'
        THEN
            BEGIN
                INSERT INTO alps.CDA_ACCOUNT_ATTR_RAW (uid_account_raw,
                                                       attr_code,
                                                       attr_type,
                                                       start_time,
                                                       stop_time,
                                                       value1,
                                                       str_value1)
                     VALUES (p_uid_account_raw,
                             p_attr_code,
                             'Planning Period',
                             p_start_time,
                             p_stop_time,
                             p_value1,
                             p_str_value1);
            END;
        ELSE
            INSERT INTO alps.alps_account_attr_raw (uid_attr,
                                                    created_user,
                                                    updated_user,
                                                    uid_account_raw,
                                                    attr_code,
                                                    attr_type,
                                                    start_time,
                                                    stop_time,
                                                    value1,
                                                    value2,
                                                    str_value1,
                                                    str_value2)
                 VALUES (alps_acct_attr_raw_seq.NEXTVAL,
                         USER,
                         USER,
                         p_uid_account_raw,
                         p_attr_code,
                         'Planning Period',
                         p_start_time,
                         p_stop_time,
                         p_value1,
                         NULL,
                         p_str_value1,
                         NULL);
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (gv_source_id,
                                          'Procedure: ' || gv_code_proc,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          SQLCODE,
                                          SUBSTR (SQLERRM, 1, 200),
                                          NULL,
                                          gv_source_type);
            RAISE;
    END insert_alps_acct_attr_raw;

    PROCEDURE insert_alps_usage_raw (p_uid_account_raw   IN NUMBER,
                                     p_start_time        IN DATE,
                                     p_stop_time         IN DATE,
                                     p_usage             IN FLOAT,
                                     p_demand            IN FLOAT,
                                     p_usage_type        IN VARCHAR2,
                                     p_load_type         IN VARCHAR2)
    AS
    BEGIN
        gv_code_proc := 'INSERT_ALPS_USAGE_RAW';
        gv_source_id := p_stop_time;
        gv_source_type := 'STOP_TIME';

        --

        IF p_load_type = 'ODM'
        THEN
            BEGIN
                INSERT INTO alps.odm_usage_raw (uid_account_raw,
                                                start_time,
                                                stop_time,
                                                usage,
                                                demand,
                                                usage_type)
                     VALUES (p_uid_account_raw,
                             p_start_time,
                             p_stop_time,
                             p_usage,
                             p_demand,
                             p_usage_type);
            END;
        ELSIF p_load_type = 'CDA'
        THEN
            BEGIN
                INSERT INTO alps.cda_usage_raw (uid_account_raw,
                                                start_time,
                                                stop_time,
                                                usage,
                                                demand,
                                                usage_type)
                     VALUES (p_uid_account_raw,
                             p_start_time,
                             p_stop_time,
                             p_usage,
                             p_demand,
                             p_usage_type);
            END;
        ELSE
            BEGIN
                INSERT INTO alps.alps_usage_raw (created_user,
                                                 updated_user,
                                                 uid_account_raw,
                                                 start_time,
                                                 stop_time,
                                                 usage,
                                                 demand,
                                                 usage_type)
                     VALUES (USER,
                             USER,
                             p_uid_account_raw,
                             p_start_time,
                             p_stop_time,
                             p_usage,
                             p_demand,
                             p_usage_type);
            --
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
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (gv_source_id,
                                          'Procedure: ' || gv_code_proc,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          SQLCODE,
                                          SUBSTR (SQLERRM, 1, 200),
                                          NULL,
                                          gv_source_type);
            RAISE;
    END insert_alps_usage_raw;

    PROCEDURE insert_alps_gaah_info (p_market_code   IN VARCHAR2,
                                     p_disco_code    IN VARCHAR2,
                                     p_ldc_account   IN VARCHAR2,
                                     p_comments      IN VARCHAR2)
    AS
    /*
        Purpose :   Insert GAAH data into  ALPS_GAAH_INFO

        Parameter :
                           p_market_code - market code
                          p_disco_code -  Disco code
                          p_ldc_account - ldc_account
                          p_comments  - status of the ldc_account .

        Execution :  This procedure executed  via table (ALPS_ACCOUNT_RAW trigger(ALPS_ACCOUNT_RAW_AIU) .
    */
    BEGIN
        gv_code_proc := 'INSERT_ALPS_GAAH_INFO';
        gv_source_id :=
            p_market_code || '_' || p_disco_code || '_' || p_ldc_account;
        gv_source_type := 'METER_ID';

        INSERT INTO alps.alps_gaah_info (market_code,
                                         disco_code,
                                         ldc_account,
                                         comments)
             VALUES (p_market_code,
                     p_disco_code,
                     p_ldc_account,
                     p_comments);

        --
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (gv_source_id,
                                          'Procedure: ' || gv_code_proc,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          SQLCODE,
                                          SUBSTR (SQLERRM, 1, 200),
                                          NULL,
                                          gv_source_type);
            RAISE;
    END insert_alps_gaah_info;

    /*PROCEDURE ARCHIVE_ALPS_DATA(p_out_Status OUT VARCHAR2 ,
                                                      p_out_desc OUT VARCHAR2)  AS
        v_sql VARCHAR2(2000);
        v_days_old   NUMBER;
    BEGIN
        gv_code_proc := 'ARCHIVE_ALPS_DATA';
        gv_source_id           := NULL;
        gv_source_type       := NULL ;
        gv_code_ln :=  $$plsql_line;

        -- Get the list of source tables to be archived
        FOR c_data IN  (SELECT a. lookup_str_value1 AS source_name, b. lookup_num_value1 AS day_olds
                                FROM alps_mml_lookup a, alps_mml_lookup b
                                WHERE a.lookup_group = 'ARCHIVE'
                                AND a.lookup_code = 'SOURCE_NAME'
                                AND a.lookup_group  = b.lookup_group
                                AND a. lookup_str_value1  = b.lookup_code
                                ORDER BY 1 DESC )
        LOOP
            -- For each SOURCE  table, find records that need to be archived and Insert into archive table
            v_sql := 'INSERT INTO '||c_data.source_name||'_HIST'||' SELECT * FROM '||c_data.source_name||
            ' WHERE TRUNC(sysdate - created_dt) >= '||c_data.day_olds ||
            ' RETURNING uid_account_raw  INTO :1';
     dbms_output.put_line(' v_sql ==> '||v_sql);
            EXECUTE IMMEDIATE v_sql RETURNING BULK COLLECT INTO arr_acct_raw;
             dbms_output.put_line(' row archived ==> '||arr_acct_raw.count);

            -- Delete from SOURCE table
            v_sql := 'DELETE FROM  '||c_data.source_name||
            ' WHERE TRUNC(sysdate - created_dt) >= '||c_data.day_olds;
     dbms_output.put_line(' v_sql ==> '||v_sql);
            EXECUTE IMMEDIATE v_sql;
             dbms_output.put_line(' row archived ==> '||sql%rowcount);
            --
           COMMIT;
       END LOOP;
       p_out_Status := 'SUCCESS';
    EXCEPTION WHEN OTHERS THEN
        -- Set up the error message to be return to calling App.
        p_out_Status  := 'ERROR';
        p_out_desc := p_out_desc
                    || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ();
       ALPS_MML_PKG.PROCESS_LOGGING(gv_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),NULL,gv_source_type);
       ROLLBACK;
    END ARCHIVE_ALPS_DATA;
    */
    --
    /*
    PROCEDURE ARCHIVE_ALPS_DATA(p_out_Status OUT VARCHAR2 ,
                                                      p_out_desc OUT VARCHAR2)  AS
        CURSOR c_data IS
        SELECT uid_account_raw, market_code||'_'||disco_code||'_'||ldc_account  meter_id
        FROM ALPS_ACCOUNT_RAW
        WHERE TRUNC(sysdate - updated_dt) >= (SELECT lookup_num_value1
                                                                    FROM alps_mml_lookup  A
                                                                    WHERE a.lookup_group = 'ARCHIVE'
                                                                    AND a.lookup_code = 'ALPS_ACCOUNT_RAW');
        --
        lv_acct_limit  INTEGER := 50;
    BEGIN
        gv_code_proc := 'ARCHIVE_ALPS_DATA';
        gv_source_id           := NULL;
        gv_source_type       := NULL;
        gv_stage                 := NULL;
        --
        gv_code_ln :=  $$plsql_line;
        OPEN c_data ;
        LOOP
            gv_code_ln :=  $$plsql_line;
            FETCH c_data BULK COLLECT INTO arr_acct_raw  LIMIT lv_acct_limit;
            IF arr_acct_raw.count > 0 THEN
                -- Insert into archive table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    INSERT INTO ALPS_USAGE_RAW_HIST
                    SELECT * FROM ALPS_USAGE_RAW
                    WHERE  uid_account_raw  = arr_acct_raw(ix).uid_account_raw;

                -- Remove these accounts from the source table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    DELETE FROM ALPS_USAGE_RAW
                    WHERE  uid_account_raw  = arr_acct_raw(ix).uid_account_raw;

                -- Insert into archive table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    INSERT INTO ALPS_IDR_LOG_HIST
                    SELECT * FROM ALPS_IDR_LOG
                    WHERE  meter_id  = arr_acct_raw(ix).meter_id;

                -- Remove these accounts from the source table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    DELETE FROM ALPS_IDR_LOG
                    WHERE  meter_id  = arr_acct_raw(ix).meter_id;

                -- Insert into archive table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    INSERT INTO ALPS_ACCOUNT_ATTR_RAW_HIST
                    SELECT * FROM ALPS_ACCOUNT_ATTR_RAW
                    WHERE   uid_account_raw  = arr_acct_raw(ix).uid_account_raw;

                -- Remove these accounts from the source table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    DELETE FROM ALPS_ACCOUNT_ATTR_RAW
                    WHERE  uid_account_raw  = arr_acct_raw(ix).uid_account_raw;

                -- Archiving Parent account table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    INSERT INTO ALPS_ACCOUNT_RAW_HIST
                    SELECT * FROM ALPS_ACCOUNT_RAW
                    WHERE  uid_account_raw  = arr_acct_raw(ix).uid_account_raw;

            --  Delete from Parent account table
                gv_code_ln :=  $$plsql_line    ;
                FORALL ix IN arr_acct_raw.FIRST..arr_acct_raw.LAST
                    DELETE FROM ALPS_ACCOUNT_RAW
                    WHERE  uid_account_raw  = arr_acct_raw(ix).uid_account_raw;

                --  COMMIT every 50 accounts
                    COMMIT;
             END IF;
            EXIT WHEN c_data%NOTFOUND;
        END LOOP;
        CLOSE c_data;
        COMMIT;
       p_out_Status := 'SUCCESS';
    EXCEPTION WHEN OTHERS THEN
        -- Set up the error message to be return to calling App.
        p_out_Status  := 'ERROR';
        p_out_desc := p_out_desc
                    || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ();
       ALPS_MML_PKG.PROCESS_LOGGING(gv_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_stage,gv_source_type);
       ROLLBACK;
      RAISE;
    END ARCHIVE_ALPS_DATA;
    */

    PROCEDURE refresh_idrstatus_from_pe (p_out_status   OUT VARCHAR2,
                                         p_out_desc     OUT VARCHAR2)
    AS
        CURSOR c1 IS
            SELECT CAST (NULL AS VARCHAR2 (200))     meter_id,
                   CAST (NULL AS DATE)               chlsetuptimestamp
              FROM DUAL;

        TYPE type_c1_data IS TABLE OF c1%ROWTYPE;

        tbl_data         type_c1_data;
        tbl_data_empty   type_c1_data;
        v_count          NUMBER := 0;
        lv_setup_dt      DATE;
        vAlpsUidAccount  ALPS_MML_PR_DETAIL.UID_ALPS_ACCOUNT%TYPE;
        
    BEGIN
    DBMS_OUTPUT.PUT_LINE(' REFRESH_IDRSTATUS_FROM_PE ');
        gv_code_proc := 'REFRESH_IDRSTATUS_FROM_PE';
        gv_source_id := SYSDATE;
        gv_source_type := NULL;

        --Get the Data from PE
        gv_code_ln := $$plsql_line;

          SELECT /*+  DRIVING_SITE(a) */
                 recorderid, MAX (chnlcuttimestamp) channelcuttimestamp
            BULK COLLECT INTO tbl_data
            FROM (SELECT a.uidchannel, b.recorderid, d.*
                    FROM channel@TPpe           a,
                         recorder@TPpe          b,
                         (SELECT DISTINCT b.meter_id
                            FROM alps_mml_pr_header a, alps_mml_pr_detail b
                           WHERE     b.idr_status IN
                                         ('REQUEST_COMPLETE',
                                          'REQUEST_FAILED',
                                          'INTERNAL_NEW',
                                          'PE_SETUP_FAILED',
                                          'WEB_SCRAPE_PENDING',
                                           'REQUEST_PENDING',
                                           'DATA_ERROR',
                                            'METERTYPE_TBD',
                                            'NOT_IDR')
                                 AND a.sbl_quote_id = b.sbl_quote_id
                                 AND b.updated_dt > SYSDATE - 30
                                 AND a.active_flg = 'Y'
                                AND A.STATUS = 'PENDING') c,
                         lschannelcutheader@TPpe d
                   WHERE     a.uidchannel = d.uidchannel(+)
                         AND a.uidrecorder = b.uidrecorder
                         AND b.recorderid = c.meter_id
                         AND a.channelnum = 1) a
           WHERE channel = 1 AND chnlcuttimestamp > TRUNC (SYSDATE) - 30
        GROUP BY recorderid, chnlcuttimestamp;
        DBMS_OUTPUT.PUT_LINE('tbl_data (i).COUNT:'||tbl_data.COUNT ()); 
DBMS_OUTPUT.PUT_LINE('tbl_data (i).meter_id :'||tbl_data (1).meter_id); 

        IF tbl_data.COUNT () > 0
        THEN
            FOR i IN tbl_data.FIRST .. tbl_data.LAST
            LOOP
                v_count := 0;
                gv_code_ln := $$plsql_line;

                SELECT COUNT (*)
                  INTO v_count
                  FROM alps_idr_log
                 WHERE     status = 'ALPS_UPDATE_COMPLETE'
                       AND pesetup_date = tbl_data (i).chlsetuptimestamp
                       AND meter_id = tbl_data (i).meter_id;

                IF v_count = 0
                THEN
                    gv_code_ln := $$plsql_line;

                    INSERT INTO alps_idr_log (created_user,
                                              updated_user,
                                              meter_id,
                                              usage_type,
                                              status,
                                              pesetup_date)
                         VALUES (USER,
                                 USER,
                                 tbl_data (i).meter_id,
                                 'IDR',
                                 'PE_SETUP_COMPLETE',
                                 tbl_data (i).chlsetuptimestamp);
                END IF;
            END LOOP;
        END IF;

        FOR rec IN (SELECT *
                      FROM alps_idr_log
                     WHERE status = 'PE_SETUP_COMPLETE')
        LOOP
          FOR REC1 IN 
          (SELECT uid_alps_account
           FROM alps_mml_pr_detail dtl
           WHERE meter_id = rec.meter_id
           AND idr_status IN ('REQUEST_COMPLETE',
                              'REQUEST_FAILED',
                              'PE_SETUP_FAILED',
                              'WEB_SCRAPE_PENDING',
                               'REQUEST_PENDING',
                               'DATA_ERROR',
                                'METERTYPE_TBD',
                                'NOT_IDR')
           AND EXISTS(SELECT 1 FROM alps_mml_pr_header 
                     WHERE status = 'PENDING'
                     AND sbl_quote_id = dtl.sbl_quote_id
                     AND active_flg = 'Y') 
          )
          LOOP
            --Call Proc to update MML Records
            UPDATE alps_mml_pr_detail dtl
            SET idr_status = 'PE_SETUP_COMPLETE'
            WHERE UID_ALPS_ACCOUNT = rec1.UID_ALPS_ACCOUNT
            RETURN uid_alps_account INTO vAlpsUidAccount;
                       
            --Add transaction log
            IF vAlpsUidAccount IS NOT NULL 
            THEN
              
                    -- Create account transactions
                    arr_acct_txn (1).uid_attachment := NULL;
                    arr_acct_txn (1).uid_alps_account := vAlpsUidAccount;
                    arr_acct_txn (1).transact_type := NULL;
                    arr_acct_txn (1).status := 'PE_SETUP_COMPLETE';
                    arr_acct_txn (1).description := 'SUCCESS';
                    arr_acct_txn (1).txn_indicator := 'HI';
                    --
                    alps_request_pkg.setup_acct_transact (
                        NULL,
                        arr_acct_txn);
            
            END IF;
           END LOOP;
           
            --Update current record
            UPDATE alps_idr_log
               SET status = 'ALPS_UPDATE_COMPLETE'
             WHERE     status = rec.status
                   AND pesetup_date = rec.pesetup_date
                   AND meter_id = rec.meter_id;
        END LOOP;
        
        --Insert Data into ALPS_VEE_MGMT
        FOR rec IN (SELECT DISTINCT meterid
                      FROM v_alps_vee_acct_setup)
        LOOP
            DBMS_OUTPUT.put_line ('Working in Meter_Id:' || rec.meterid);

            IF f_acct_already_vee (rec.meterid, lv_setup_dt) = 'YES'
            THEN
                SELECT sbl_quote_id, uid_alps_account
                  BULK COLLECT INTO arr_quote_id, arr_uid_acct
                  FROM alps_mml_pr_detail
                 WHERE     meter_id = rec.meterid
                       AND status = 'READY_FOR_VEE'
                       AND sbl_quote_id IN
                               (SELECT DISTINCT sbl_quote_id
                                  FROM alps_mml_pr_header     hdr,
                                       v_alps_vee_acct_setup  vw
                                 WHERE     vw.prnumber = hdr.prnumber
                                       AND vw.revision = hdr.revision
                                       AND vw.meterid = rec.meterid);

                UPDATE alps_mml_pr_detail
                   SET status = 'VEE_COMPLETE',
                       last_vee_completed_dt = lv_setup_dt
                 WHERE     meter_id = rec.meterid
                       AND status = 'READY_FOR_VEE'
                       AND sbl_quote_id IN
                               (SELECT DISTINCT sbl_quote_id
                                  FROM alps_mml_pr_header     hdr,
                                       v_alps_vee_acct_setup  vw
                                 WHERE     vw.prnumber = hdr.prnumber
                                       AND vw.revision = hdr.revision
                                       AND vw.meterid = rec.meterid);

                /*
                Returning sbl_quote_id, uid_alps_account
                     Bulk Collect Into arr_quote_id, arr_uid_acct;
                */

                -- Create account transactions
                gv_code_ln := $$plsql_line;

                IF arr_uid_acct.COUNT > 0
                THEN
                    FOR ix IN arr_uid_acct.FIRST .. arr_uid_acct.LAST
                    LOOP
                        arr_acct_txn (1).uid_alps_account :=
                            arr_uid_acct (ix);
                        arr_acct_txn (1).status := 'VEE_COMPLETE';
                        arr_acct_txn (1).description := 'NO_FORECAST_ISSUES';
                        --
                        gv_code_ln := $$plsql_line;
                        arr_acct_txn (1).txn_indicator := 'HI';
                        alps_request_pkg.setup_acct_transact (gv_source_id,
                                                              arr_acct_txn);
                    END LOOP;
                END IF;

                -- Update PR STATUS
                IF arr_quote_id.COUNT > 0
                THEN
                    FOR ix IN arr_quote_id.FIRST .. arr_quote_id.LAST
                    LOOP
                        update_pr_status (arr_quote_id (ix), gv_stage);
                    END LOOP;
                END IF;
            ELSE
                DBMS_OUTPUT.put_line (
                       'Working in Meter_Id:'
                    || rec.meterid
                    || ' trying to insert into vee_mgt');
                insert_vee_mgt (rec.meterid);
            END IF;
        END LOOP;

        --Return OUTPUT
        arr_quote_id.delete;
        p_out_status := 'SUCCESS';
        COMMIT;

        --Release
        tbl_data := tbl_data_empty;
    EXCEPTION
        WHEN OTHERS
        THEN
            p_out_status := 'ERROR';
            p_out_desc :=
                SQLERRM || CHR (13) || DBMS_UTILITY.format_error_backtrace ();

            alps_mml_pkg.process_logging (gv_source_id,
                                          'Procedure: ' || gv_code_proc,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          SQLCODE,
                                          SUBSTR (p_out_desc, 1, 200),
                                          NULL,
                                          gv_source_type);
            ROLLBACK;

            --Release
            tbl_data := tbl_data_empty;
    END;

    PROCEDURE insert_alps_idr_log (p_meter_id IN VARCHAR2)
    AS
        lv_cnt   INTEGER := 0;
    BEGIN
        gv_code_proc := 'INSERT_ALPS_IDR_LOG';
        gv_source_id := p_meter_id;
        gv_source_type := 'METER_ID';

        --
        SELECT COUNT (1)
          INTO lv_cnt
          FROM alps.alps_idr_log
         WHERE meter_id = p_meter_id;

        --
        IF lv_cnt = 0
        THEN
            gv_code_ln := $$plsql_line;

            INSERT INTO alps.alps_idr_log (meter_id,
                                           usage_type,
                                           status,
                                           description)
                 VALUES (p_meter_id,
                         'IDR',
                         'UP',
                         'TBD');
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            alps_mml_pkg.process_logging (gv_source_id,
                                          'Procedure: ' || gv_code_proc,
                                          NULL,
                                          NULL,
                                          NULL,
                                          NULL,
                                          SQLCODE,
                                          SUBSTR (SQLERRM, 1, 200),
                                          NULL,
                                          gv_source_type);
            RAISE;
    END insert_alps_idr_log;
END alps_data_pkg;
////