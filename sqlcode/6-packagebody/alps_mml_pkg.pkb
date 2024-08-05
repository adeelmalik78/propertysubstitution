CREATE OR REPLACE PACKAGE BODY ${schema.name}.alps_mml_pkg
AS
/*
*********************************************************************************************************
Rev 10.12.2023    cerebro 70455 LCE NAME IS BLANK CAUSING AN ERROR
*********************************************************************************************************
*/
    -- Global Var
    gv_code_ln                 NUMBER;
    gv_code_proc               VARCHAR2 (50);
    gv_stg_cnt                 NUMBER := 0;
    gv_bulk_limit              NUMBER := 100;
    gv_pkg_name                VARCHAR2 (50) := 'ALPS_MML_PKG';

    -- Variable used for Logging
    gv_log_stage               alps_mml_process_log.stage%TYPE := 'VALIDATION';
    gv_log_source_typ          alps_mml_process_log.source_type%TYPE := 'QUOTE_ID';
    gv_log_source_id           alps_mml_pr_header.sbl_quote_id%TYPE;

    -- user defined Exception
    failed_validation          EXCEPTION;
    missing_accounts           EXCEPTION;
    exceed_account_threshold   EXCEPTION;
    loa_missing                EXCEPTION;

    gv_express_flag            CHAR (1) := 'N';
    p_out_status               VARCHAR2 (30);
    p_out_desc                 VARCHAR2 (1000);

    PROCEDURE setup_revised_pr (p_prnumber       IN     VARCHAR2,
                                p_new_quote_id   IN     VARCHAR2,
                                p_old_quote_id   IN     VARCHAR2,
                                p_out_status        OUT VARCHAR2)
    AS
        --
        lv_arr_sca_status   alps_mml_pr_detail.sca_status%TYPE;
        lv_arr_idr_status   alps_mml_pr_detail.idr_status%TYPE;
        lv_prnumber         alps_mml_pr_header.prnumber%TYPE;
        lv_old_status       alps_mml_pr_detail.status%TYPE;
        lv_old_stage        alps_mml_pr_detail.stage%TYPE;
        lv_pe_dt            DATE;
        lv_vee_dt           DATE;
        lv_lpss_ind         VARCHAR2 (3);
    BEGIN
        gv_code_proc := 'SETUP_REVISED_PR';
        gv_log_source_id := p_new_quote_id;

        -- The PR latest Revision
        gv_code_ln := $$plsql_line;

        SELECT *
          BULK COLLECT INTO arr_revised_accts
          FROM alps_mml_pr_detail dtl
         WHERE     sbl_quote_id = p_new_quote_id
               AND EXISTS
                       (SELECT 1
                          FROM alps_mml_pr_header hdr
                         WHERE     hdr.active_flg = 'Y'
                               AND hdr.sbl_quote_id = dtl.sbl_quote_id);

        --
        IF arr_revised_accts.COUNT > 0
        THEN
            DBMS_OUTPUT.put_line ('Process Revised accounts ');
            gv_code_ln := $$plsql_line;

            --
            FOR ix IN arr_revised_accts.FIRST .. arr_revised_accts.LAST
            LOOP
                -- Aging data check
                IF f_request_data_is_old (p_old_quote_id,
                                          arr_revised_accts (ix).meter_id) =
                   'YES'
                THEN
                    DBMS_OUTPUT.put_line (
                        'Process Revised accounts- old  data , requesting  ');
                    -- Exising data is dated. Setup accounts for Request.
                    alps_request_pkg.setup_pr_accts (
                        lv_prnumber,
                        arr_revised_accts (ix).sbl_quote_id,
                        arr_revised_accts (ix).market_code,
                        arr_revised_accts (ix).disco_code,
                        arr_revised_accts (ix).ldc_account,
                        arr_revised_accts (ix).meter_type,
                        arr_revised_accts (ix).idr_status,
                        arr_revised_accts (ix).sca_status,
                        arr_revised_accts (ix).idr_source,
                        arr_revised_accts (ix).sca_source,
                        arr_revised_accts (ix).digit_code_key,
                        lv_arr_idr_status,
                        lv_arr_sca_status,
                        lv_pe_dt,
                        lv_vee_dt);

                    -- Update account individual  status
                    gv_code_ln := $$plsql_line;

                    UPDATE alps_mml_pr_detail
                       SET idr_status = lv_arr_idr_status,
                           sca_status = lv_arr_idr_status
                     WHERE uid_alps_account =
                           arr_revised_accts (ix).uid_alps_account;
                ELSE
                    IF f_vee_data_is_old (p_old_quote_id,
                                          arr_revised_accts (ix).meter_id) =
                       'YES'
                    THEN
                        DBMS_OUTPUT.put_line (
                            'Process Revised accounts- old VEE data, Setup for VEE  ');
                        -- VEE data is dated. Setup data for RE-VEE
                        gv_code_ln := $$plsql_line;

                        UPDATE alps_mml_pr_detail
                           SET stage = 'PE_SETUP', status = 'READY_FOR_VEE'
                         WHERE uid_alps_account =
                               arr_revised_accts (ix).uid_alps_account;
                    ELSE
                        DBMS_OUTPUT.put_line (
                            'Process Revised accounts- Data is current  ');

                        -- Get previous status for this meter
                        SELECT status, stage
                          INTO lv_old_status, lv_old_stage
                          FROM alps_mml_pr_detail
                         WHERE     sbl_quote_id = p_old_quote_id
                               AND meter_id = arr_revised_accts (ix).meter_id;

                        --   Assigned the previous status. Thid is necessary as the table(alps_mml_pr_detail) trigger overwrite the previous status as the new revision account being INSERT
                        IF lv_old_status <> arr_revised_accts (ix).status
                        THEN
                            gv_code_ln := $$plsql_line;

                            UPDATE alps_mml_pr_detail
                               SET stage = lv_old_stage,
                                   status = lv_old_status
                             WHERE uid_alps_account =
                                   arr_revised_accts (ix).uid_alps_account;
                        END IF;

                        --
                        IF arr_revised_accts (ix).status IN
                               ('SETUP_COMPLETE',
                                'OFFER_SUMMARY_COMPLETE',
                                'OFFER_SUMMARY_FAILED',
                                'SUBMITTED')
                        THEN
                            --Data is current(Request and VEE data)  , setup data to update SIEBEL
                            DBMS_OUTPUT.put_line (
                                'Process Revised accounts- Data is current  , setup data to update SIEBEL  ');
                            gv_code_ln := $$plsql_line;

                            UPDATE alps_mml_pr_detail
                               SET stage = 'SIEBEL_SETUP',
                                   status = 'VEE_COMPLETE'
                             WHERE uid_alps_account =
                                   arr_revised_accts (ix).uid_alps_account;
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END IF;

        --
        p_out_status := 'SUCCESS';
    EXCEPTION
        WHEN OTHERS
        THEN
            p_out_status := 'ERROR';
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),NULL,gv_log_source_typ );
            ROLLBACK;
    END setup_revised_pr;

    PROCEDURE process_mml_exceptions (p_attach_id IN VARCHAR2)
    AS
        v_prev_uid      alps_mml_validation_exceptions.uid_mml_account%TYPE
                            := NULL;
        v_description   alps_mml_validation_exceptions.description%TYPE
                            := NULL;
    BEGIN
        gv_code_proc := 'PROCESS_MML_EXCEPTIONS';

        -- Update MML account status in the staging table
        -- For those accounts with exception, set status  to fail
        gv_code_ln := $$plsql_line;

        FOR c_data IN (  SELECT sbl_attach_id, uid_mml_account, description
                           FROM alps_mml_validation_exceptions
                          WHERE sbl_attach_id = p_attach_id
                       ORDER BY uid_mml_account)
        LOOP
            IF v_prev_uid IS NULL
            THEN
                v_prev_uid := c_data.uid_mml_account;
                v_description := c_data.description;
            ELSIF     v_prev_uid IS NOT NULL
                  AND v_prev_uid = c_data.uid_mml_account
            THEN
                v_description := v_description || '; ' || c_data.description;
            ELSIF     v_prev_uid IS NOT NULL
                  AND v_prev_uid <> c_data.uid_mml_account
            THEN
                UPDATE alps_mml_stg stg
                   SET stg.status = 'VALIDATION_FAILED',
                       stg.description = v_description
                 WHERE     stg.sbl_attach_id = p_attach_id
                       AND stg.status = 'VALIDATION_PENDING'
                       AND stg.uid_mml_account = v_prev_uid;

                v_prev_uid := c_data.uid_mml_account;
                v_description := c_data.description;
            END IF;
        END LOOP;

        -- Update last record
        gv_code_ln := $$plsql_line;

        UPDATE alps_mml_stg stg
           SET stg.status = 'VALIDATION_FAILED',
               stg.description = v_description
         WHERE     stg.sbl_attach_id = p_attach_id
               AND stg.status = 'VALIDATION_PENDING'
               AND stg.uid_mml_account = v_prev_uid;

        COMMIT;

        -- Update all remain rows that did not Failed the validation
        gv_code_ln := $$plsql_line;

        UPDATE alps_mml_stg stg
           SET stg.status = 'VALIDATION_SUCCESS'
         WHERE     stg.sbl_attach_id = p_attach_id
               AND stg.status = 'VALIDATION_PENDING';

        /*Express*/
        --Insert Data into EXP_QUOTE_ACCT
        FOR rec
            IN (SELECT sbl_attach_id, sbl_quote_id
                  FROM alps_mml_stg
                 WHERE     sbl_attach_id = p_attach_id
                       AND NVL (exp_flag, 'N') = 'Y'
                       AND ROWNUM < 2)
        LOOP
            INSERT INTO exp_quote_acct (uid_account,
                                        exp_quote_id,
                                        ldc_account,
                                        market_code,
                                        disco_code,
                                        meter_type,
                                        uid_mml_account,
                                        status,
                                        description,
                                        retain_attrusage_flg)
                SELECT alps_generic_seq.NEXTVAL,
                       sbl_quote_id,
                       ldc_account,
                       market_code,
                       disco_code,
                       meter_type,
                       uid_mml_account,
                       status,
                       description,
                       'N'
                  FROM alps_mml_stg
                 WHERE     sbl_quote_id = rec.sbl_quote_id
                       AND sbl_attach_id = rec.sbl_attach_id;
        END LOOP;


        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            process_logging (
                gv_log_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_log_stage,
                gv_log_source_typ);
            ROLLBACK;
            RAISE;
    END process_mml_exceptions;

    FUNCTION mml_exception_count (p_attach_id IN VARCHAR2)
        RETURN NUMBER
    AS
        lv_excp_cnt   NUMBER;
    BEGIN
        gv_code_proc := 'MML_EXCEPTION_COUNT';
        gv_code_ln := $$plsql_line;

        SELECT COUNT (1)
          INTO lv_excp_cnt
          FROM alps_mml_validation_exceptions
         WHERE sbl_attach_id = p_attach_id;

        --
        RETURN (lv_excp_cnt);
    EXCEPTION
        WHEN OTHERS
        THEN
            -- PROCESS_LOGGING(gv_log_source_id,  'Function: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END;

    PROCEDURE process_all_rules (p_attach_id   IN VARCHAR2,
                                 p_rule_catg   IN VARCHAR2)
    AS
        TYPE rec_acct_rule IS RECORD
        (
            uid_master_rule    alps_rules_header.uid_master_rule%TYPE,
            rule_catg          alps_rules_header.rule_catg%TYPE,
            rule_type          alps_rules_header.rule_type%TYPE,
            procedure_call     VARCHAR2 (32767),
            boilertext1        alps_error_msg_template.boilertext1%TYPE,
            boilertext2        alps_error_msg_template.boilertext1%TYPE,
            boilertext3        alps_error_msg_template.boilertext1%TYPE,
            boilertext4        alps_error_msg_template.boilertext1%TYPE,
            boilertext5        alps_error_msg_template.boilertext1%TYPE
        );

        arr_rules        rec_acct_rule;

        TYPE refcursor IS REF CURSOR
            RETURN rec_acct_rule;

        c_rule           refcursor;
        lv_excp_cnt      NUMBER;
        /*Express Change*/
        lv_expressflag   CHAR (1) := 'N';
    BEGIN
        gv_code_proc := 'PROCESS_ALL_RULES';

        /*Get Express Flag*/
        FOR rec IN (SELECT prnumber
                      FROM alps_mml_stg
                     WHERE sbl_attach_id = p_attach_id AND ROWNUM < 2)
        LOOP
            IF SUBSTR (rec.prnumber, 1, 1) IN ('E', 'M', 'T')
            THEN
                lv_expressflag := 'Y';
            END IF;
        END LOOP;

        DBMS_OUTPUT.put_line ('lv_expressflag ==> ' || lv_expressflag);

        --
        CASE p_rule_catg
            WHEN 'MML'
            THEN
                gv_code_ln := $$plsql_line;

                OPEN c_rule FOR
                    SELECT ru.uid_master_rule,
                           ru.rule_catg,
                           ru.rule_type,
                           ru.procedure_call,
                           aemt.boilertext1,
                           aemt.boilertext2,
                           aemt.boilertext3,
                           aemt.boilertext4,
                           aemt.boilertext5
                      FROM alps_rules_header ru, alps_error_msg_template aemt
                     WHERE     ru.rule_catg = 'MML'
                           AND ru.status = 'ACTIVE'
                           AND DECODE (lv_expressflag,
                                       'Y', express_flg,
                                       non_express_flg) =
                               'Y'
                           AND ru.rule_type = aemt.msg_type
                           AND ru.rule_catg = aemt.msg_group;
            WHEN 'SYSTEM'
            THEN
                gv_code_ln := $$plsql_line;

                OPEN c_rule FOR
                    SELECT ru.uid_master_rule,
                           ru.rule_catg,
                           ru.rule_type,
                           ru.procedure_call,
                           aemt.boilertext1,
                           aemt.boilertext2,
                           aemt.boilertext3,
                           aemt.boilertext4,
                           aemt.boilertext5
                      FROM alps_rules_header ru, alps_error_msg_template aemt
                     WHERE     ru.rule_catg = 'SYSTEM'
                           AND ru.status = 'ACTIVE'
                           AND DECODE (lv_expressflag,
                                       'Y', express_flg,
                                       non_express_flg) =
                               'Y'
                           AND ru.rule_type = aemt.msg_type
                           AND ru.rule_catg = aemt.msg_group;
        END CASE;

        LOOP
            FETCH c_rule INTO arr_rules;

            EXIT WHEN c_rule%NOTFOUND;

            IF arr_rules.procedure_call IS NOT NULL
            THEN
                gv_code_ln := $$plsql_line;
                DBMS_OUTPUT.put_line (
                    'rule_type ==> ' || arr_rules.rule_type);

                EXECUTE IMMEDIATE arr_rules.procedure_call
                    USING IN p_attach_id,
                          arr_rules.uid_master_rule,
                          arr_rules.rule_catg,
                          arr_rules.rule_type,
                          arr_rules.boilertext1,
                          arr_rules.boilertext2,
                          arr_rules.boilertext3,
                          arr_rules.boilertext4,
                          arr_rules.boilertext5;
            END IF;
        END LOOP;

        CLOSE c_rule;
    EXCEPTION
        WHEN OTHERS
        THEN
            process_logging (
                gv_log_source_id,
                   'Procedure: '
                || gv_code_proc
                || ',  Rule Category: '
                || arr_rules.rule_catg
                || ',   Rule : '
                || arr_rules.rule_type,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_log_stage,
                gv_log_source_typ);
            ROLLBACK;
            RAISE;
    END process_all_rules;

    PROCEDURE process_logging (p_source_id     IN VARCHAR2,
                               p_source_desc   IN VARCHAR2,
                               p_ins_cnt       IN NUMBER,
                               p_upd_cnt       IN NUMBER,
                               p_del_cnt       IN NUMBER,
                               p_proc_cnt      IN NUMBER,
                               p_sqlcode       IN NUMBER,
                               p_sqlerrm       IN VARCHAR2,
                               p_stage         IN VARCHAR2,
                               p_source_type   IN VARCHAR2)
    AS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO alps_mml_process_log (uid_process,
                                          created_user,
                                          updated_user,
                                          source_id,
                                          description,
                                          ERROR_CODE,
                                          error_desc,
                                          insert_cnt,
                                          update_cnt,
                                          delete_cnt,
                                          process_cnt,
                                          stage,
                                          source_type)
             VALUES (alps_mml_process_seq.NEXTVAL,
                     USER,
                     USER,
                     p_source_id,
                     p_source_desc,
                     p_sqlcode,
                     p_sqlerrm,
                     p_ins_cnt,
                     p_upd_cnt,
                     p_del_cnt,
                     p_proc_cnt,
                     p_stage,
                     p_source_type);

        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            process_logging (
                gv_log_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_log_stage,
                gv_log_source_typ);
            ROLLBACK;
            RAISE;
    END process_logging;

    PROCEDURE set_staging_account (p_attach_id   IN VARCHAR2,
                                   p_proc_stat   IN VARCHAR2)
    AS
    BEGIN
        gv_code_proc := 'SET_STG_ACCOUNT_STATUS';
        gv_code_ln := $$plsql_line;

        UPDATE alps_mml_stg
           SET status = p_proc_stat
         WHERE sbl_attach_id = p_attach_id AND status = 'VALIDATION_PENDING';

        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END set_staging_account;

    PROCEDURE setup_mml_accounts (p_attach_id IN VARCHAR2)
    AS
        vCdaStatus      VARCHAR2 (30);

        --
        CURSOR c_mstr (p_attach_id IN VARCHAR2)
        IS
            SELECT DISTINCT NULL,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            stg.sbl_attach_id,
                            stg.sbl_quote_id,
                            stg.prnumber,
                            stg.revision,
                            stg.customer_id,
                            stg.salerep_id,
                            NVL (stg.lwsp_flag, 'N'),
                            stg.pr_trans_type,
                            'PENDING',
                            NULL,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            stg.exp_flag     express_flag
              FROM alps_mml_stg stg
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND status = 'VALIDATION_PENDING';

        --
        CURSOR c_detl (p_attach_id IN VARCHAR2)
        IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   stg.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   DECODE (gv_express_flag, 'Y', 'VALIDATION_SUCCESS', NULL), --  'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   CASE
                       WHEN stg.pr_trans_type = 'Amendment - Add' -- AND stg.disco_code In ('WMECO', 'CLP', 'PSNH','AMERENCIPS','AMERENCILCO','AMERENIP')
                       THEN
                           'REQUEST_OVERRIDE'
                       ELSE
                           'VALIDATION_SUCCESS'
                   END,
                   CASE
                       WHEN stg.pr_trans_type = 'Amendment - Add' --AND stg.disco_code In ('WMECO', 'CLP', 'PSNH','AMERENCIPS','AMERENCILCO','AMERENIP')
                       THEN
                           'REQUEST_OVERRIDE'
                       ELSE
                           alps.f_digit_key_requestovrd (
                               stg.sbl_quote_id,
                               stg.ldc_account,
                               stg.market_code,
                               stg.disco_code,
                               'VALIDATION_SUCCESS',
                               stg.digit_code_key)
                   END,
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_attach_id = stg.sbl_attach_id;

        --

        lv_mml_id       NUMBER;
        arr_mstr_acct   tab_mstr_acct;
        arr_detl_acct   tab_detl_acct;
    BEGIN
        gv_code_proc := 'SETUP_MML_ACCOUNTS';
        gv_code_ln := $$plsql_line;
        DBMS_OUTPUT.put_line ('IN SETUP_MML_ACCOUNTS');

        OPEN c_mstr (p_attach_id);

        LOOP
            FETCH c_mstr BULK COLLECT INTO arr_mstr_acct LIMIT gv_bulk_limit;

            IF arr_mstr_acct.COUNT > 0
            THEN
                lv_mml_id := alps_mml_mstr_seq.NEXTVAL; -- Generate the account master sequence_id

                --
                FOR ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                LOOP
                    arr_mstr_acct (ix).uid_mml_master := lv_mml_id; -- Assign account master sequence_id
                END LOOP;

                --
                FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                    INSERT INTO alps_mml_pr_header
                         VALUES arr_mstr_acct (ix);
            END IF;

            EXIT WHEN c_mstr%NOTFOUND;
        END LOOP;

        CLOSE c_mstr;

        -- stored the MML account detail into array
        gv_code_ln := $$plsql_line;

        OPEN c_detl (p_attach_id);

        LOOP
            FETCH c_detl BULK COLLECT INTO arr_detl_acct LIMIT gv_bulk_limit;

            --
            IF arr_detl_acct.COUNT > 0
            THEN
                FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                LOOP
                    arr_detl_acct (ix).uid_alps_account :=
                        alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                END LOOP;

                --
                FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    INSERT INTO alps_mml_pr_detail
                         VALUES arr_detl_acct (ix);
            END IF;

            EXIT WHEN c_detl%NOTFOUND;
        END LOOP;

        CLOSE c_detl;

        COMMIT;
        arr_detl_acct.delete;
        arr_mstr_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END setup_mml_accounts;

    PROCEDURE setup_lwsp_accounts (p_attach_id IN VARCHAR2)
    AS
    BEGIN
        -- Get the Distinct disco
        FOR c_data
            IN (SELECT DISTINCT disco_code
                  FROM alps_mml_stg
                 WHERE     sbl_attach_id = p_attach_id
                       AND status = 'VALIDATION_PENDING'
                       AND NVL (lwsp_flag, 'N') = 'Y')
        LOOP
            IF c_data.disco_code IN ('WMECO',
                                     'CLP',
                                     'PSNH',
                                     'AMERENCIPS',
                                     'AMERENCILCO',
                                     'AMERENIP')
            THEN
                DBMS_OUTPUT.put_line ('execute SETUP_LWSP_BASA_ACCOUNTS');
                setup_lwsp_basa_accounts (p_attach_id);
            ELSIF c_data.disco_code IN ('COMELEC',
                                        'BECO',
                                        'CMBRDG',
                                        'ATSICE',
                                        'ATSIOE',
                                        'ATSITE',
                                        'DPL',
                                        'DPLDE')
            THEN
                DBMS_OUTPUT.put_line ('execute SETUP_LWSP_NSTAR_ACCOUNTS');
                setup_lwsp_nstar_accounts (p_attach_id);
            ELSE
                DBMS_OUTPUT.put_line ('execute SETUP_LWSP_OTHER_ACCOUNTS');
                setup_lwsp_other_accounts (p_attach_id);
            END IF;
        END LOOP;
    END setup_lwsp_accounts;

    PROCEDURE setup_lwsp_nstar_accounts (p_attach_id IN VARCHAR2)
    AS
        -- Same PR/QUOTEID
        CURSOR c_same_pr IS
            SELECT DISTINCT uid_mml_master,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            stg.sbl_attach_id,
                            stg.sbl_quote_id,
                            stg.prnumber,
                            stg.revision,
                            stg.customer_id,
                            stg.salerep_id,
                            NVL (stg.lwsp_flag, 'N'),
                            stg.pr_trans_type,
                            'PENDING'        status,            --mstr.status,
                            NULL             description,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            stg.exp_flag     express_flag
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE');

        --  Same PR -  Existing Accounts
        CURSOR c_same_pr_old IS
            SELECT dtl.uid_alps_account,
                   DECODE (dtl.status,
                           'VEE_COMPLETE', 'READY_FOR_VEE',
                           'OFFER_SUMMARY_COMPLETE', 'READY_FOR_VEE',
                           dtl.status)    status,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.ldc_account = stg.ldc_account
                   AND stg.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE');

        --  Same PR - New accounts
        CURSOR c_same_pr_new IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   mstr.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   --'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE')
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.ldc_account = stg.ldc_account);

        --   New PR
        CURSOR c_new_pr IS
            SELECT DISTINCT NULL,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            sbl_attach_id,
                            sbl_quote_id,
                            prnumber,
                            revision,
                            customer_id,
                            salerep_id,
                            NVL (lwsp_flag, 'N'),
                            pr_trans_type,
                            'VALIDATION_SUCCESS',
                            NULL,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            exp_flag     express_flag
              FROM alps_mml_stg
             WHERE     sbl_attach_id = p_attach_id
                   AND status = 'VALIDATION_PENDING'
                   AND NVL (lwsp_flag, 'N') = 'Y'
                   AND disco_code IN ('COMELEC',
                                      'BECO',
                                      'CMBRDG',
                                      'ATSICE',
                                      'ATSIOE',
                                      'ATSITE',
                                      'DPL',
                                      'DPLDE');

        -- New PR accounts
        CURSOR c_new_pr_acct IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   stg.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   NULL,                             --  'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (stg.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND stg.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE')
                   AND mstr.sbl_attach_id = stg.sbl_attach_id;

        lv_new_quote_id   alps_mml_pr_detail.sbl_quote_id%TYPE;
        lv_mml_id         INTEGER := 0;
    BEGIN
        gv_code_proc := 'SETUP_LWSP_NSTAR_ACCOUNTS';

        IF f_lwsp_same_pr_chk (p_attach_id) = 'YES'
        THEN
            -- Update PR HEADER
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr;

            LOOP
                FETCH c_same_pr
                    BULK COLLECT INTO arr_mstr_acct
                    LIMIT gv_bulk_limit;

                IF arr_mstr_acct.COUNT > 0
                THEN
                    --
                    FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                        UPDATE alps_mml_pr_header
                           SET sbl_attach_id =
                                   arr_mstr_acct (ix).sbl_attach_id,
                               customer_id = arr_mstr_acct (ix).customer_id,
                               salerep_id = arr_mstr_acct (ix).salerep_id,
                               pr_trans_type =
                                   arr_mstr_acct (ix).pr_trans_type,
                               description = arr_mstr_acct (ix).description,
                               status = arr_mstr_acct (ix).status,
                               last_siebel_setup_dt = NULL,
                               last_offer_summary_dt = NULL
                         WHERE uid_mml_master =
                               arr_mstr_acct (ix).uid_mml_master;
                END IF;

                EXIT WHEN c_same_pr%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr;

            -- Update existing Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr_old;

            LOOP
                FETCH c_same_pr_old
                    BULK COLLECT INTO arr_lwsp_same
                    LIMIT gv_bulk_limit;

                IF arr_lwsp_same.COUNT > 0
                THEN
                    FOR ix IN arr_lwsp_same.FIRST .. arr_lwsp_same.LAST
                    LOOP
                        UPDATE alps_mml_pr_detail
                           SET status = arr_lwsp_same (ix).status,
                               address1 = arr_lwsp_same (ix).address1,
                               address2 = arr_lwsp_same (ix).address2,
                               city = arr_lwsp_same (ix).city,
                               state = arr_lwsp_same (ix).state,
                               zip = arr_lwsp_same (ix).zip,
                               digit_code_key =
                                   arr_lwsp_same (ix).digit_code_key,
                               disco_code = arr_lwsp_same (ix).disco_code,
                               sca_status =
                                   alps.f_digit_key_requestovrd (
                                       sbl_quote_id,
                                       ldc_account,
                                       market_code,
                                       disco_code,
                                       'VALIDATION_SUCCESS',
                                       arr_lwsp_same (ix).digit_code_key),
                               description = NULL,
                               last_vee_completed_dt = NULL
                         WHERE uid_alps_account =
                               arr_lwsp_same (ix).uid_alps_account;
                    END LOOP;
                END IF;

                EXIT WHEN c_same_pr_old%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr_old;

            -- Add new Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr_new;

            LOOP
                FETCH c_same_pr_new
                    BULK COLLECT INTO arr_detl_acct
                    LIMIT gv_bulk_limit;

                --
                IF arr_detl_acct.COUNT > 0
                THEN
                    FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    LOOP
                        arr_detl_acct (ix).uid_alps_account :=
                            alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                        INSERT INTO alps_mml_pr_detail
                             VALUES arr_detl_acct (ix);
                END IF;

                EXIT WHEN c_same_pr_new%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr_new;

            -- Delete Accounts
            BEGIN
                gv_code_ln := $$plsql_line;

                SELECT DISTINCT mstr.sbl_quote_id
                  INTO lv_new_quote_id
                  FROM alps_mml_stg stg, alps_mml_pr_header mstr
                 WHERE     stg.sbl_attach_id = p_attach_id
                       AND stg.status = 'VALIDATION_PENDING'
                       AND stg.prnumber = mstr.prnumber
                       AND stg.sbl_quote_id = mstr.sbl_quote_id
                       AND NVL (stg.lwsp_flag, 'N') = 'Y'
                       AND mstr.active_flg = 'Y'
                       AND NVL (mstr.lwsp_flag, 'N') = 'Y'
                       AND stg.disco_code IN ('COMELEC',
                                              'BECO',
                                              'CMBRDG',
                                              'ATSICE',
                                              'ATSIOE',
                                              'ATSITE',
                                              'DPL',
                                              'DPLDE');
            EXCEPTION
                WHEN OTHERS
                THEN
                    RAISE;
            END;

            -- Remove accounts not included in the current MML
            gv_code_ln := $$plsql_line;

            DELETE FROM
                alps_mml_pr_detail dtl
                  WHERE     dtl.sbl_quote_id = lv_new_quote_id
                        AND disco_code IN ('COMELEC',
                                           'BECO',
                                           'CMBRDG',
                                           'ATSICE',
                                           'ATSIOE',
                                           'ATSITE',
                                           'DPL',
                                           'DPLDE')
                        AND NOT EXISTS
                                (SELECT 1
                                   FROM alps_mml_stg        stg,
                                        alps_mml_pr_header  mstr
                                  WHERE     stg.sbl_attach_id = p_attach_id
                                        AND stg.status = 'VALIDATION_PENDING'
                                        AND NVL (stg.lwsp_flag, 'N') = 'Y'
                                        --AND stg.disco_code = dtl.disco_code
                                        AND stg.ldc_account = dtl.ldc_account
                                        AND stg.market_code = dtl.market_code
                                        AND mstr.sbl_quote_id =
                                            dtl.sbl_quote_id
                                        AND mstr.sbl_quote_id =
                                            stg.sbl_quote_id
                                        AND mstr.prnumber = stg.prnumber
                                        AND mstr.active_flg = 'Y'
                                        AND NVL (mstr.lwsp_flag, 'N') = 'Y'
                                        AND stg.disco_code IN ('COMELEC',
                                                               'BECO',
                                                               'CMBRDG',
                                                               'ATSICE',
                                                               'ATSIOE',
                                                               'ATSITE',
                                                               'DPL',
                                                               'DPLDE'));
        ELSE                                                   -- Brand new PR
            -- Insert PR
            gv_code_ln := $$plsql_line;

            OPEN c_new_pr;

            LOOP
                FETCH c_new_pr
                    BULK COLLECT INTO arr_mstr_acct
                    LIMIT gv_bulk_limit;

                IF arr_mstr_acct.COUNT > 0
                THEN
                    lv_mml_id := alps_mml_mstr_seq.NEXTVAL; -- Generate the account master sequence_id

                    --
                    FOR ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                    LOOP
                        arr_mstr_acct (ix).uid_mml_master := lv_mml_id; -- Assign account master sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                        INSERT INTO alps_mml_pr_header
                             VALUES arr_mstr_acct (ix);
                END IF;

                EXIT WHEN c_new_pr%NOTFOUND;
            END LOOP;

            CLOSE c_new_pr;

            -- Insert PR Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_new_pr_acct;

            LOOP
                FETCH c_new_pr_acct
                    BULK COLLECT INTO arr_detl_acct
                    LIMIT gv_bulk_limit;

                --
                IF arr_detl_acct.COUNT > 0
                THEN
                    FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    LOOP
                        arr_detl_acct (ix).uid_alps_account :=
                            alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                        INSERT INTO alps_mml_pr_detail
                             VALUES arr_detl_acct (ix);
                END IF;

                EXIT WHEN c_new_pr_acct%NOTFOUND;
            END LOOP;

            CLOSE c_new_pr_acct;
        END IF;

        --
        COMMIT;
        arr_old_acct.delete;
        arr_detl_acct.delete;
        arr_mstr_acct.delete;
        arr_lwsp_same.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END setup_lwsp_nstar_accounts;

    PROCEDURE setup_lwsp_basa_accounts (p_attach_id IN VARCHAR2)
    AS
        -- Same PR - PR HEADER
        CURSOR c_same_pr IS
            SELECT DISTINCT uid_mml_master,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            stg.sbl_attach_id,
                            stg.sbl_quote_id,
                            stg.prnumber,
                            stg.revision,
                            stg.customer_id,
                            stg.salerep_id,
                            NVL (stg.lwsp_flag, 'N'),
                            stg.pr_trans_type,
                            'PENDING'        status,            --mstr.status,
                            NULL             description,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            stg.exp_flag     express_flag
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP');

        --  Same PR -  new MML BASA accounts
        CURSOR c_same_pr_basa IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   mstr.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND NVL (mstr.lwsp_flag, 'N') = 'Y'
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.disco_code = stg.disco_code
                                   AND dtl.ldc_account = stg.ldc_account
                                   AND dtl.disco_code IN ('WMECO',
                                                          'CLP',
                                                          'PSNH',
                                                          'AMERENCIPS',
                                                          'AMERENCILCO',
                                                          'AMERENIP'));

        --  Same PR -New MML BA accounts
        CURSOR c_same_pr_ba IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   mstr.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   (SELECT SUBSTR (dtl.ldc_account,
                                   1,
                                   INSTR (dtl.ldc_account, '_') - 1)
                               ldc_account,
                           dtl.sbl_quote_id,
                           market_code,
                           disco_code
                      FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
                     WHERE     hdr.sbl_attach_id = p_attach_id
                           AND dtl.sbl_quote_id = hdr.sbl_quote_id
                           AND hdr.active_flg = 'Y'
                           AND INSTR (dtl.ldc_account, '_') > 1
                           AND dtl.disco_code IN ('WMECO',
                                                  'CLP',
                                                  'PSNH',
                                                  'AMERENCIPS',
                                                  'AMERENCILCO',
                                                  'AMERENIP')) dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   AND INSTR (stg.ldc_account, '_') = 0
                   AND stg.ldc_account = dtl.ldc_account
                   AND stg.market_code = dtl.market_code
                   AND stg.disco_code = dtl.disco_code;

        /*    AND NOT EXISTS (
                                       SELECT 1
                                        FROM  alps_mml_pr_detail dtl
                                        WHERE dtl.sbl_quote_id  = mstr.sbl_quote_id
                                       --AND dtl.ldc_account = stg.ldc_account
                                        --AND INSTR(dtl.ldc_account,'_') = 0
                                        AND dtl.market_code = stg.market_code
                                        AND dtl.disco_code = stg.disco_code
                                        AND  dtl.disco_code IN ('WMECO','CLP')
                                        AND SUBSTR(dtl.ldc_account,1,INSTR(dtl.ldc_account,'_')-1) = stg.ldc_account
                                        )   ;
    */
        --   New PR
        CURSOR c_new_pr IS
            SELECT DISTINCT NULL,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            sbl_attach_id,
                            sbl_quote_id,
                            prnumber,
                            revision,
                            customer_id,
                            salerep_id,
                            NVL (lwsp_flag, 'N'),
                            pr_trans_type,
                            'VALIDATION_SUCCESS',
                            NULL,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            exp_flag     express_flag
              FROM alps_mml_stg
             WHERE     sbl_attach_id = p_attach_id
                   AND status = 'VALIDATION_PENDING'
                   AND NVL (lwsp_flag, 'N') = 'Y'
                   AND disco_code IN ('WMECO',
                                      'CLP',
                                      'PSNH',
                                      'AMERENCIPS',
                                      'AMERENCILCO',
                                      'AMERENIP');

        -- New PR - new accounts
        CURSOR c_new_pr_acct IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   mstr.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP');

        lv_new_quote_id   alps_mml_pr_detail.sbl_quote_id%TYPE;
        lv_mml_id         INTEGER := 0;
        lv_ba_cnt         INTEGER := 0;
        lv_prnumber       alps_mml_pr_header.prnumber%TYPE;
        lv_sbl_quote_id   alps_mml_pr_detail.sbl_quote_id%TYPE;
    BEGIN
        gv_code_proc := 'SETUP_LWSP_BASA_ACCOUNTS';

        IF f_lwsp_same_pr_chk (p_attach_id) = 'YES'
        THEN
            -- Same PR/QIOTE_ID, UPDATE alps
            -- Update PR HEADER
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr;

            LOOP
                FETCH c_same_pr
                    BULK COLLECT INTO arr_mstr_acct
                    LIMIT gv_bulk_limit;

                IF arr_mstr_acct.COUNT > 0
                THEN
                    DBMS_OUTPUT.put_line (' same PR - UPDATE  PR  HEADER');

                    --
                    FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                        UPDATE alps_mml_pr_header
                           SET sbl_attach_id =
                                   arr_mstr_acct (ix).sbl_attach_id,
                               customer_id = arr_mstr_acct (ix).customer_id,
                               salerep_id = arr_mstr_acct (ix).salerep_id,
                               pr_trans_type =
                                   arr_mstr_acct (ix).pr_trans_type,
                               description = arr_mstr_acct (ix).description,
                               status = arr_mstr_acct (ix).status,
                               last_siebel_setup_dt = NULL,
                               last_offer_summary_dt = NULL
                         WHERE uid_mml_master =
                               arr_mstr_acct (ix).uid_mml_master;
                END IF;

                EXIT WHEN c_same_pr%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr;

            -- Update account attributes for matching BASA Accounts
            gv_code_ln := $$plsql_line;

            SELECT dtl.uid_alps_account,
                   DECODE (dtl.status,
                           'VEE_COMPLETE', 'READY_FOR_VEE',
                           'OFFER_SUMMARY_COMPLETE', 'READY_FOR_VEE',
                           dtl.status)    status,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code
              BULK COLLECT INTO arr_lwsp_same
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.disco_code = stg.disco_code
                   AND dtl.ldc_account = stg.ldc_account
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP');

            --AND INSTR(stg.ldc_account,'_') > 0 ;
            --
            IF arr_lwsp_same.COUNT > 0
            THEN
                FOR ix IN arr_lwsp_same.FIRST .. arr_lwsp_same.LAST
                LOOP
                    DBMS_OUTPUT.put_line (
                           ' same PR - UPDATE  PR DETAIL BASA ACCOUNT==> '
                        || arr_lwsp_same (ix).uid_alps_account);

                    UPDATE alps_mml_pr_detail
                       SET status = arr_lwsp_same (ix).status,
                           address1 = arr_lwsp_same (ix).address1,
                           address2 = arr_lwsp_same (ix).address2,
                           city = arr_lwsp_same (ix).city,
                           state = arr_lwsp_same (ix).state,
                           zip = arr_lwsp_same (ix).zip,
                           digit_code_key = arr_lwsp_same (ix).digit_code_key,
                           sca_status =
                               alps.f_digit_key_requestovrd (
                                   sbl_quote_id,
                                   ldc_account,
                                   market_code,
                                   disco_code,
                                   'VALIDATION_SUCCESS',
                                   arr_lwsp_same (ix).digit_code_key),
                           idr_status = 'VALIDATION_SUCCESS',
                           description = NULL,
                           last_vee_completed_dt = NULL
                     WHERE uid_alps_account =
                           arr_lwsp_same (ix).uid_alps_account;
                END LOOP;
            ELSE
                -- No match by BASA, match by BA Accounts and update underlying BASA account attributes
                gv_code_ln := $$plsql_line;

                SELECT dtl.uid_alps_account,
                       DECODE (dtl.status,
                               'VEE_COMPLETE', 'READY_FOR_VEE',
                               'OFFER_SUMMARY_COMPLETE', 'READY_FOR_VEE',
                               dtl.status)    status,
                       stg.address1,
                       stg.address2,
                       stg.city,
                       stg.state,
                       stg.zip,
                       stg.digit_code_key,
                       stg.disco_code
                  BULK COLLECT INTO arr_lwsp_same
                  FROM alps_mml_stg        stg,
                       alps_mml_pr_header  mstr,
                       alps_mml_pr_detail  dtl
                 WHERE     stg.sbl_attach_id = p_attach_id
                       AND stg.sbl_quote_id = mstr.sbl_quote_id
                       AND stg.prnumber = mstr.prnumber
                       AND stg.status = 'VALIDATION_PENDING'
                       AND NVL (stg.lwsp_flag, 'N') = 'Y'
                       AND mstr.active_flg = 'Y'
                       AND dtl.sbl_quote_id = mstr.sbl_quote_id
                       AND dtl.market_code = stg.market_code
                       AND dtl.disco_code = stg.disco_code
                       AND SUBSTR (dtl.ldc_account,
                                   1,
                                   INSTR (dtl.ldc_account, '_') - 1) =
                           stg.ldc_account
                       --AND INSTR(stg.ldc_account,'_')  = 0
                       AND stg.disco_code IN ('WMECO',
                                              'CLP',
                                              'PSNH',
                                              'AMERENCIPS',
                                              'AMERENCILCO',
                                              'AMERENIP');

                --
                IF arr_lwsp_same.COUNT > 0
                THEN
                    FOR ix IN arr_lwsp_same.FIRST .. arr_lwsp_same.LAST
                    LOOP
                        DBMS_OUTPUT.put_line (
                               ' same PR - UPDATE  PR DETAIL BA ACCOUNT==> '
                            || arr_lwsp_same (ix).uid_alps_account);

                        UPDATE alps_mml_pr_detail
                           SET status = arr_lwsp_same (ix).status,
                               address1 = arr_lwsp_same (ix).address1,
                               address2 = arr_lwsp_same (ix).address2,
                               city = arr_lwsp_same (ix).city,
                               state = arr_lwsp_same (ix).state,
                               zip = arr_lwsp_same (ix).zip,
                               digit_code_key =
                                   arr_lwsp_same (ix).digit_code_key,
                               sca_status =
                                   alps.f_digit_key_requestovrd (
                                       sbl_quote_id,
                                       ldc_account,
                                       market_code,
                                       disco_code,
                                       sca_status,
                                       arr_lwsp_same (ix).digit_code_key),
                               description = NULL,
                               last_vee_completed_dt = NULL
                         WHERE uid_alps_account =
                               arr_lwsp_same (ix).uid_alps_account;
                    END LOOP;
                END IF;
            END IF;

            -- New BASA Accounts
            gv_code_ln := $$plsql_line;

            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   mstr.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              BULK COLLECT INTO arr_detl_acct
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND NVL (mstr.lwsp_flag, 'N') = 'Y'
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.disco_code = stg.disco_code
                                   AND dtl.ldc_account = stg.ldc_account
                                   AND dtl.disco_code IN ('WMECO',
                                                          'CLP',
                                                          'PSNH',
                                                          'AMERENCIPS',
                                                          'AMERENCILCO',
                                                          'AMERENIP'));

            --
            IF arr_detl_acct.COUNT > 0
            THEN
                FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                LOOP
                    -- Not match by full ACCOUNT , do not insert until no match by BA PREFIX
                    SELECT NULL,
                           SYSDATE,
                           USER,
                           SYSDATE,
                           USER,
                           mstr.sbl_quote_id,
                           mstr.uid_mml_master,
                           stg.ldc_account,
                           NULL,
                           stg.market_code,
                           stg.disco_code,
                           'VALIDATION_SUCCESS',
                           NULL,
                           stg.address1,
                           stg.address2,
                           stg.city,
                           stg.state,
                           stg.zip,
                           stg.digit_code_key,
                           'VALIDATION',
                           'VALIDATION_SUCCESS',
                           -- 'VALIDATION_SUCCESS',
                           alps.f_digit_key_requestovrd (
                               mstr.sbl_quote_id,
                               stg.ldc_account,
                               stg.market_code,
                               stg.disco_code,
                               'VALIDATION_SUCCESS',
                               stg.digit_code_key),
                           NULL,
                           NULL,
                              stg.market_code
                           || '_'
                           || stg.disco_code
                           || '_'
                           || stg.ldc_account,
                           NULL,
                           NULL
                      BULK COLLECT INTO arr_detl_acct_ba
                      FROM alps_mml_stg        stg,
                           alps_mml_pr_header  mstr,
                           (SELECT DISTINCT
                                   SUBSTR (dtl.ldc_account,
                                           1,
                                           INSTR (dtl.ldc_account, '_') - 1)
                                       ldc_account,
                                   dtl.sbl_quote_id,
                                   market_code,
                                   disco_code
                              FROM alps_mml_pr_detail  dtl,
                                   alps_mml_pr_header  hdr
                             WHERE     hdr.sbl_attach_id = p_attach_id
                                   AND dtl.sbl_quote_id = hdr.sbl_quote_id
                                   AND hdr.active_flg = 'Y'
                                   AND INSTR (dtl.ldc_account, '_') > 1
                                   AND dtl.disco_code IN ('WMECO',
                                                          'CLP',
                                                          'PSNH',
                                                          'AMERENCIPS',
                                                          'AMERENCILCO',
                                                          'AMERENIP')) dtl
                     WHERE     stg.sbl_attach_id = p_attach_id
                           AND stg.sbl_quote_id = mstr.sbl_quote_id
                           AND stg.prnumber = mstr.prnumber
                           AND stg.status = 'VALIDATION_PENDING'
                           AND NVL (stg.lwsp_flag, 'N') = 'Y'
                           AND mstr.active_flg = 'Y'
                           AND stg.disco_code IN ('WMECO',
                                                  'CLP',
                                                  'PSNH',
                                                  'AMERENCIPS',
                                                  'AMERENCILCO',
                                                  'AMERENIP')
                           AND INSTR (stg.ldc_account, '_') = 0
                           AND stg.ldc_account =
                               arr_detl_acct (ix).ldc_account
                           AND stg.market_code =
                               arr_detl_acct (ix).market_code
                           AND stg.disco_code = arr_detl_acct (ix).disco_code;

                    --
                    IF arr_detl_acct_ba.COUNT = 0
                    THEN
                        -- INSERT
                        IF arr_detl_acct.COUNT > 0
                        THEN
                            DBMS_OUTPUT.put_line (
                                ' same PR - INSERT  PR DETAIL BASA ACCOUNT ');
                            --                   FOR ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST LOOP
                            arr_detl_acct (ix).uid_alps_account :=
                                alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                            --                   END LOOP;
                            --
                            gv_code_ln := $$plsql_line;

                            --                  FORALL ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST
                            INSERT INTO alps_mml_pr_detail
                                 VALUES arr_detl_acct (ix);
                        END IF;
                    END IF;
                END LOOP;
            END IF;

            -- New BA Accounts
            /*       gv_code_ln :=  $$plsql_line;
                    OPEN c_same_pr_ba;
                    LOOP
                        FETCH c_same_pr_ba BULK COLLECT INTO arr_detl_acct  LIMIT gv_bulk_limit;
                            --
                       IF arr_detl_acct.count > 0 THEN
                                           dbms_output.put_line(' same PR - INSERT  PR DETAIL BA ACCOUNT ');
                           FOR ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST LOOP
                               arr_detl_acct(ix).uid_alps_account := ALPS_MML_ACCT_SEQ.nextval;      --Assign the account sequence_id
                           END LOOP;
                           --
                            gv_code_ln :=  $$plsql_line;
                            FORALL ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST
                                INSERT INTO ALPS_MML_PR_DETAIL  VALUES arr_detl_acct(ix);

                       END IF;
                       EXIT WHEN c_same_pr_ba%NOTFOUND;
                    END LOOP;
                    CLOSE c_same_pr_ba;
             */
            --
            -- Remove ALPS BA_SA accounts ONLY both BA and BA_SA not included in the MML
            --
            gv_code_ln := $$plsql_line;

            SELECT DISTINCT mstr.prnumber, mstr.sbl_quote_id
              INTO lv_prnumber, lv_sbl_quote_id
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   AND mstr.active_flg = 'Y';

            gv_code_ln := $$plsql_line;

            -- Retrieve ALPS BA_SA accounts
            SELECT dtl.uid_alps_account,
                   dtl.ldc_account,
                   dtl.market_code,
                   dtl.disco_code
              BULK COLLECT INTO arr_del_accts
              FROM alps_mml_pr_header mstr, alps_mml_pr_detail dtl
             WHERE     mstr.prnumber = lv_prnumber
                   AND mstr.sbl_quote_id = lv_sbl_quote_id
                   AND mstr.active_flg = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   AND NVL (mstr.lwsp_flag, 'N') = 'Y';

            --
            IF arr_del_accts.COUNT > 0
            THEN
                gv_code_ln := $$plsql_line;

                FOR ix IN arr_del_accts.FIRST .. arr_del_accts.LAST
                LOOP
                    DBMS_OUTPUT.put_line (
                           ' same PR - ATTEMPT TO DELETE PR DETAIL ACCOUNT==> '
                        || arr_del_accts (ix).ldc_account);

                    -- Check MML for the matching BA
                    SELECT COUNT (1)
                      INTO lv_ba_cnt
                      FROM alps_mml_stg
                     WHERE     sbl_attach_id = p_attach_id
                           AND status = 'VALIDATION_PENDING'
                           AND INSTR (ldc_account, '_') = 0
                           AND ldc_account =
                               SUBSTR (
                                   arr_del_accts (ix).ldc_account,
                                   1,
                                     INSTR (arr_del_accts (ix).ldc_account,
                                            '_')
                                   - 1)
                           AND market_code = arr_del_accts (ix).market_code
                           AND disco_code = arr_del_accts (ix).disco_code;

                    --
                    IF lv_ba_cnt = 0
                    THEN
                        DBMS_OUTPUT.put_line (
                            ' same PR - DELETE PR DETAIL ACCOUNT, NOMATCH BY BA ');

                        -- No macthing BA then matched by BA_SA accounts
                        SELECT COUNT (1)
                          INTO lv_ba_cnt
                          FROM alps_mml_stg
                         WHERE     sbl_attach_id = p_attach_id
                               AND status = 'VALIDATION_PENDING'
                               --AND INSTR(ldc_account,'_') > 0
                               AND ldc_account =
                                   arr_del_accts (ix).ldc_account
                               AND market_code =
                                   arr_del_accts (ix).market_code
                               AND disco_code = arr_del_accts (ix).disco_code;

                        --  Could not find a match by neither BA nor BA_SA account in MML ; this BA_SA ca be remove from ALPS
                        IF lv_ba_cnt = 0
                        THEN
                            DBMS_OUTPUT.put_line (
                                ' same PR - DELETE PR DETAIL ACCOUNT, NOMATCH BY BASA, DELETING ');

                            DELETE FROM
                                alps_mml_pr_detail dtl
                                  WHERE uid_alps_account =
                                        arr_del_accts (ix).uid_alps_account;
                        END IF;
                    END IF;

                    DBMS_OUTPUT.put_line (
                        ' same PR - NO DELETE PR DETAIL ACCOUNT, MATCH BY BA OR BASA ');
                END LOOP;
            END IF;
        ELSE                                                         -- New PR
            gv_code_ln := $$plsql_line;

            OPEN c_new_pr;

            LOOP
                FETCH c_new_pr
                    BULK COLLECT INTO arr_mstr_acct
                    LIMIT gv_bulk_limit;

                IF arr_mstr_acct.COUNT > 0
                THEN
                    lv_mml_id := alps_mml_mstr_seq.NEXTVAL; -- Generate the account master sequence_id

                    --
                    FOR ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                    LOOP
                        arr_mstr_acct (ix).uid_mml_master := lv_mml_id; -- Assign account master sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                        INSERT INTO alps_mml_pr_header
                             VALUES arr_mstr_acct (ix);
                END IF;

                EXIT WHEN c_new_pr%NOTFOUND;
            END LOOP;

            CLOSE c_new_pr;

            -- Insert PR Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_new_pr_acct;

            LOOP
                FETCH c_new_pr_acct
                    BULK COLLECT INTO arr_detl_acct
                    LIMIT gv_bulk_limit;

                --
                IF arr_detl_acct.COUNT > 0
                THEN
                    FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    LOOP
                        arr_detl_acct (ix).uid_alps_account :=
                            alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                        INSERT INTO alps_mml_pr_detail
                             VALUES arr_detl_acct (ix);
                END IF;

                EXIT WHEN c_new_pr_acct%NOTFOUND;
            END LOOP;

            CLOSE c_new_pr_acct;
        END IF;

        --
        COMMIT;
        arr_old_acct.delete;
        arr_detl_acct.delete;
        arr_mstr_acct.delete;
        arr_lwsp_same.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END setup_lwsp_basa_accounts;

    PROCEDURE setup_lwsp_other_accounts (p_attach_id IN VARCHAR2)
    AS
        -- Same PR/QUOTEID
        CURSOR c_same_pr IS
            SELECT DISTINCT uid_mml_master,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            stg.sbl_attach_id,
                            stg.sbl_quote_id,
                            stg.prnumber,
                            stg.revision,
                            stg.customer_id,
                            stg.salerep_id,
                            NVL (stg.lwsp_flag, 'N'),
                            stg.pr_trans_type,
                            'PENDING'        status,
                            NULL             description,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            stg.exp_flag     express_flag
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.active_flg = 'Y';

        --  Same PR -  Existing Accounts
        CURSOR c_same_pr_old IS
            SELECT dtl.uid_alps_account,
                   DECODE (dtl.status,
                           'VEE_COMPLETE', 'READY_FOR_VEE',
                           'OFFER_SUMMARY_COMPLETE', 'READY_FOR_VEE',
                           dtl.status)    status,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND NVL (mstr.lwsp_flag, 'N') = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.ldc_account = stg.ldc_account;

        --  Same PR - New accounts
        CURSOR c_same_pr_new IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   mstr.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.sbl_quote_id = mstr.sbl_quote_id
                   AND stg.prnumber = mstr.prnumber
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.active_flg = 'Y'
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.ldc_account = stg.ldc_account);

        --   New PR
        CURSOR c_new_pr IS
            SELECT DISTINCT NULL,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            sbl_attach_id,
                            sbl_quote_id,
                            prnumber,
                            revision,
                            customer_id,
                            salerep_id,
                            NVL (lwsp_flag, 'N'),
                            pr_trans_type,
                            'PENDING',
                            NULL,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            exp_flag     express_flag
              FROM alps_mml_stg
             WHERE     sbl_attach_id = p_attach_id
                   AND status = 'VALIDATION_PENDING'
                   AND NVL (lwsp_flag, 'N') = 'Y';

        -- New PR accounts
        CURSOR c_new_pr_acct IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   stg.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   NULL,                             --  'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   -- 'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (stg.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND NVL (stg.lwsp_flag, 'N') = 'Y'
                   AND mstr.sbl_attach_id = stg.sbl_attach_id;

        lv_new_quote_id   alps_mml_pr_detail.sbl_quote_id%TYPE;
        lv_mml_id         INTEGER := 0;
    BEGIN
        gv_code_proc := 'SETUP_LWSP_OTHER_ACCOUNTS';

        IF f_lwsp_same_pr_chk (p_attach_id) = 'YES'
        THEN
            DBMS_OUTPUT.put_line ('same PR ');
            -- Update PR HEADER
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr;

            LOOP
                FETCH c_same_pr
                    BULK COLLECT INTO arr_mstr_acct
                    LIMIT gv_bulk_limit;

                IF arr_mstr_acct.COUNT > 0
                THEN
                    --
                    DBMS_OUTPUT.put_line (' same PR - UPDATE  PR  HEADER');

                    FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                        UPDATE alps_mml_pr_header
                           SET sbl_attach_id =
                                   arr_mstr_acct (ix).sbl_attach_id,
                               customer_id = arr_mstr_acct (ix).customer_id,
                               salerep_id = arr_mstr_acct (ix).salerep_id,
                               pr_trans_type =
                                   arr_mstr_acct (ix).pr_trans_type,
                               description = arr_mstr_acct (ix).description,
                               status = arr_mstr_acct (ix).status,
                               last_siebel_setup_dt = NULL,
                               last_offer_summary_dt = NULL
                         WHERE uid_mml_master =
                               arr_mstr_acct (ix).uid_mml_master;
                END IF;

                EXIT WHEN c_same_pr%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr;

            -- Update existing Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr_old;

            LOOP
                FETCH c_same_pr_old
                    BULK COLLECT INTO arr_lwsp_same
                    LIMIT gv_bulk_limit;

                IF arr_lwsp_same.COUNT > 0
                THEN
                    --
                    DBMS_OUTPUT.put_line ('same PR - UPDATE  PR  DETAIL');

                    FOR ix IN arr_lwsp_same.FIRST .. arr_lwsp_same.LAST
                    LOOP
                        UPDATE alps_mml_pr_detail
                           SET status = arr_lwsp_same (ix).status,
                               address1 = arr_lwsp_same (ix).address1,
                               address2 = arr_lwsp_same (ix).address2,
                               city = arr_lwsp_same (ix).city,
                               state = arr_lwsp_same (ix).state,
                               zip = arr_lwsp_same (ix).zip,
                               digit_code_key =
                                   arr_lwsp_same (ix).digit_code_key,
                               disco_code = arr_lwsp_same (ix).disco_code,
                               sca_status =
                                   alps.f_digit_key_requestovrd (
                                       sbl_quote_id,
                                       ldc_account,
                                       market_code,
                                       disco_code,
                                       'VALIDATION_SUCCESS',
                                       arr_lwsp_same (ix).digit_code_key),
                               idr_status = 'VALIDATION_SUCCESS',
                               description = NULL,
                               last_vee_completed_dt = NULL
                         WHERE uid_alps_account =
                               arr_lwsp_same (ix).uid_alps_account;
                    END LOOP;
                END IF;

                EXIT WHEN c_same_pr_old%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr_old;

            -- Add new Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_same_pr_new;

            LOOP
                FETCH c_same_pr_new
                    BULK COLLECT INTO arr_detl_acct
                    LIMIT gv_bulk_limit;

                --
                IF arr_detl_acct.COUNT > 0
                THEN
                    DBMS_OUTPUT.put_line ('same PR - INSERT NEW  PR  DETAIL');

                    FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    LOOP
                        arr_detl_acct (ix).uid_alps_account :=
                            alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                        INSERT INTO alps_mml_pr_detail
                             VALUES arr_detl_acct (ix);
                END IF;

                EXIT WHEN c_same_pr_new%NOTFOUND;
            END LOOP;

            CLOSE c_same_pr_new;

            -- Delete Accounts
            BEGIN
                gv_code_ln := $$plsql_line;

                SELECT DISTINCT mstr.sbl_quote_id
                  INTO lv_new_quote_id
                  FROM alps_mml_stg stg, alps_mml_pr_header mstr
                 WHERE     stg.sbl_attach_id = p_attach_id
                       AND stg.status = 'VALIDATION_PENDING'
                       AND stg.prnumber = mstr.prnumber
                       AND stg.sbl_quote_id = mstr.sbl_quote_id
                       AND NVL (stg.lwsp_flag, 'N') = 'Y'
                       AND mstr.active_flg = 'Y';
            EXCEPTION
                WHEN OTHERS
                THEN
                    RAISE;
            END;

            -- Remove accounts not included in the current MML
            gv_code_ln := $$plsql_line;

            DELETE FROM
                alps_mml_pr_detail dtl
                  WHERE     dtl.sbl_quote_id = lv_new_quote_id
                        AND NOT EXISTS
                                (SELECT 1
                                   FROM alps_mml_stg        stg,
                                        alps_mml_pr_header  mstr
                                  WHERE     stg.sbl_attach_id = p_attach_id
                                        AND stg.status = 'VALIDATION_PENDING'
                                        AND NVL (stg.lwsp_flag, 'N') = 'Y'
                                        AND stg.ldc_account = dtl.ldc_account
                                        AND stg.market_code = dtl.market_code
                                        AND mstr.sbl_quote_id =
                                            dtl.sbl_quote_id
                                        AND mstr.sbl_quote_id =
                                            stg.sbl_quote_id
                                        AND mstr.prnumber = stg.prnumber
                                        AND mstr.active_flg = 'Y'
                                        AND NVL (mstr.lwsp_flag, 'N') = 'Y');
        ELSE                                                   -- Brand new PR
            -- Insert PR
            gv_code_ln := $$plsql_line;

            OPEN c_new_pr;

            LOOP
                FETCH c_new_pr
                    BULK COLLECT INTO arr_mstr_acct
                    LIMIT gv_bulk_limit;

                IF arr_mstr_acct.COUNT > 0
                THEN
                    lv_mml_id := alps_mml_mstr_seq.NEXTVAL; -- Generate the account master sequence_id

                    --
                    FOR ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                    LOOP
                        arr_mstr_acct (ix).uid_mml_master := lv_mml_id; -- Assign account master sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                        INSERT INTO alps_mml_pr_header
                             VALUES arr_mstr_acct (ix);
                END IF;

                EXIT WHEN c_new_pr%NOTFOUND;
            END LOOP;

            CLOSE c_new_pr;

            -- Insert PR Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_new_pr_acct;

            LOOP
                FETCH c_new_pr_acct
                    BULK COLLECT INTO arr_detl_acct
                    LIMIT gv_bulk_limit;

                --
                IF arr_detl_acct.COUNT > 0
                THEN
                    FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    LOOP
                        arr_detl_acct (ix).uid_alps_account :=
                            alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                        INSERT INTO alps_mml_pr_detail
                             VALUES arr_detl_acct (ix);
                END IF;

                EXIT WHEN c_new_pr_acct%NOTFOUND;
            END LOOP;

            CLOSE c_new_pr_acct;
        END IF;

        --
        COMMIT;
        arr_old_acct.delete;
        arr_detl_acct.delete;
        arr_mstr_acct.delete;
        arr_lwsp_same.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END setup_lwsp_other_accounts;

    PROCEDURE revised_acct_status (p_meter_id     IN     VARCHAR2,
                                   p_meter_type   IN     VARCHAR2,
                                   p_status       IN     VARCHAR2,
                                   p_status_out      OUT VARCHAR2,
                                   p_idr_status      OUT VARCHAR2,
                                   p_sca_status      OUT VARCHAR2,
                                   p_pe_dt           OUT DATE,
                                   p_vee_dt          OUT DATE,
                                   p_refresh_dt   IN     VARCHAR2)
    AS
        lv_fresh_data     VARCHAR2 (3);
        lv_pe_dt          DATE;
        lv_vee_dt         DATE;
        lv_recent_load    VARCHAR2 (3) := 'NO';
        vReqTags          VARCHAR2 (3);
        vSubSeqReq        VARCHAR2 (3) := 'NO';
        lv_status         VARCHAR2 (30);
        lv_status_desc    VARCHAR2 (500);
        lv_meter_type     VARCHAR2 (10);
        vMaxStopTime      ALPS_USAGE.STOP_TIME%TYPE;
        vMaxRefreshDt     ALPS_ACCOUNT.LAST_REFRESH_DT%TYPE;
        lv_Renewal        VARCHAR2 (3);
        lv_recent_pe      VARCHAR2 (3);
        lv_recent_vee     VARCHAR2 (3);
        lv_recent_usage   VARCHAR2 (3);
        lv_quote_id       ALPS_MML_PR_HEADER.SBL_QUOTE_ID%TYPE;
        vUsageLimit       INTEGER;

        --
        TYPE vAcctAttrTab IS TABLE OF alps_account_attributes%ROWTYPE
            INDEX BY PLS_INTEGER;

        vAcctAttrArray    vAcctAttrTab;
    BEGIN
        -- Previously VEE accounts need to be evaluate whether to reload data to PE  and RE-VEE
        --  If revised accounts been previously VEE/OFFER SUMMARY  THEN
        --     1.If VEE data is current (<120 days) then set account overall status to VEE_COMPLETE and PR status to SETUP_READY
        --     2. If VEE data is not current (>120 days) then
        --       a. If there fresh DATA then reload data to PE  and RE-VEE a
        --      2. If there is no fresh DATA request data

        --Added for EXPRESS
        DBMS_OUTPUT.put_line ('gv_Express_Flag:' || gv_express_flag);
        DBMS_OUTPUT.put_line ('p_status:' || p_status);

        IF gv_express_flag = 'Y'         /*And p_status In ('EXPRESS_READY')*/
        THEN
            SELECT lookup_num_value1
              INTO vUsageLimit
              FROM alps_mml_lookup
             WHERE     lookup_group = 'DATA_AGING_LIMIT'
                   AND lookup_code = 'RESPONSE_DATA';

            --
            SELECT MAX (STOP_TIME), MAX (last_refresh_dt)
              INTO vMaxStopTime, vMaxRefreshDt
              FROM alps_account a, alps_usage b
             WHERE METER_ID = p_meter_id AND a.uid_account = b.uid_account;

            DBMS_OUTPUT.PUT_LINE ('  vMaxStopTime: ' || vMaxStopTime);
            DBMS_OUTPUT.PUT_LINE ('  vMaxRefreshDt: ' || vMaxRefreshDt);

            -- If recent USAGE has recent START/STOP date  , DO NOT request
            IF vMaxStopTime IS NOT NULL
            THEN
                IF ROUND (SYSDATE - vMaxStopTime) < vUsageLimit
                THEN
                    lv_recent_load := 'YES';
                ELSE
                    lv_recent_load := 'NO';
                END IF;
            ELSE
                lv_recent_load := 'NO';
            END IF;

            DBMS_OUTPUT.put_line ('lv_recent_load:' || lv_recent_load);
            -- APplied to NIMO only. Confirm that  all required atttibutes, usages, Cap tags are received in ALPS.
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

            --
            IF lv_status = 'DATA_PENDING'
            THEN
                lv_recent_load := 'NO';
            END IF;

            DBMS_OUTPUT.put_line (
                'fAfter NIMO check - lv_recent_load:' || lv_recent_load);


            /*If Recent*/
            IF lv_recent_load = 'YES'
            THEN
                IF p_meter_type = 'IDR'
                THEN
                    p_sca_status := 'DATA_RECEIVED';
                ELSE
                    p_idr_status := 'NOT_IDR';
                    p_sca_status := 'DATA_RECEIVED';
                END IF;
            ELSE
                IF p_meter_type = 'IDR'
                THEN
                    p_idr_status := 'REQUEST_ATTEMPT';
                    p_sca_status := 'REQUEST_ATTEMPT';
                ELSE
                    p_idr_status := 'NOT_IDR';
                    p_sca_status := 'REQUEST_ATTEMPT';
                END IF;
            END IF;
        ELSIF p_status IN ('VEE_COMPLETE', 'OFFER_SUMMARY_COMPLETE')
        THEN
            DBMS_OUTPUT.PUT_LINE (' IN VEE_CHECK');
            lv_recent_pe :=
                alps_data_pkg.f_acct_already_pe (p_meter_id,
                                                 p_meter_type,
                                                 lv_pe_dt);
            lv_recent_vee :=
                alps_data_pkg.f_acct_already_vee (p_meter_id, lv_vee_dt);

            IF lv_recent_pe = 'YES' OR lv_recent_vee = 'YES'
            THEN
                p_status_out := 'VEE_COMPLETE';
            ELSE
                -- LPSS check
                lv_Renewal :=
                    ALPS.ALPS_REQUEST_PKG.f_lpss_status (
                        lv_quote_id,
                        SUBSTR (p_meter_id,
                                  INSTR (p_meter_id,
                                         '_',
                                         1,
                                         2)
                                + 1),                                   -- ldc
                        SUBSTR (p_meter_id,
                                1,
                                  INSTR (p_meter_id,
                                         '_',
                                         1,
                                         1)
                                - 1),                                 --market
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
                                          1))));                  --disco_code

                IF lv_Renewal = 'YES'
                THEN
                    DBMS_OUTPUT.PUT_LINE (' IN LPSS_CHECK');

                    IF p_meter_type = 'IDR'
                    THEN
                        p_idr_status := 'INTERNAL_ACTIVE';
                        p_sca_status := 'INTERNAL_ACTIVE';
                    ELSIF p_meter_type = 'SCA'
                    THEN
                        p_idr_status := 'NOT_IDR';
                        p_sca_status := 'INTERNAL_ACTIVE';
                    END IF;
                ELSE
                    DBMS_OUTPUT.PUT_LINE (' IN SCA USAGE CHECK');

                    SELECT lookup_num_value1
                      INTO vUsageLimit
                      FROM alps_mml_lookup
                     WHERE     lookup_group = 'DATA_AGING_LIMIT'
                           AND lookup_code = 'RESPONSE_DATA';

                    -- Check recent scalar usage
                    SELECT MAX (STOP_TIME), MAX (last_refresh_dt)
                      INTO vMaxStopTime, vMaxRefreshDt
                      FROM alps_account a, alps_usage b
                     WHERE     METER_ID = p_meter_id
                           AND a.uid_account = b.uid_account;

                    DBMS_OUTPUT.PUT_LINE ('  vMaxStopTime: ' || vMaxStopTime);
                    DBMS_OUTPUT.PUT_LINE (
                        '  vMaxRefreshDt: ' || vMaxRefreshDt);

                    -- If recent USAGE has recent START/STOP date  , DO NOT request
                    IF vMaxStopTime IS NOT NULL
                    THEN
                        IF ROUND (SYSDATE - vMaxStopTime) < vUsageLimit
                        THEN
                            DBMS_OUTPUT.PUT_LINE ('RECENT USAGE');

                            IF p_meter_type = 'IDR'
                            THEN
                                p_idr_status := 'REQUEST_ATTEMPT';
                                p_sca_status := 'DATA_RECEIVED';
                            ELSIF p_meter_type = 'SCA'
                            THEN
                                p_idr_status := 'NOT_IDR';
                                p_sca_status := 'DATA_RECEIVED';
                            END IF;
                        ELSE
                            DBMS_OUTPUT.PUT_LINE ('STALE USAGE');

                            IF p_meter_type = 'IDR'
                            THEN
                                p_idr_status := 'REQUEST_ATTEMPT';
                                p_sca_status := 'REQUEST_ATTEMPT';
                            ELSIF p_meter_type = 'SCA'
                            THEN
                                p_idr_status := 'NOT_IDR';
                                p_sca_status := 'REQUEST_ATTEMPT';
                            END IF;
                        END IF;
                    ELSE
                        IF p_meter_type = 'IDR'
                        THEN
                            p_idr_status := 'REQUEST_ATTEMPT';
                            p_sca_status := 'REQUEST_ATTEMPT';
                        ELSIF p_meter_type = 'SCA'
                        THEN
                            p_idr_status := 'NOT_IDR';
                            p_sca_status := 'REQUEST_ATTEMPT';
                        END IF;
                    END IF;
                END IF;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            RAISE;
    END;

    PROCEDURE setup_mml_revise_accounts (p_attach_id IN VARCHAR2)
    AS
        CURSOR c_same_pr IS
            SELECT DISTINCT uid_mml_master,
                            SYSDATE,
                            USER,
                            SYSDATE,
                            USER,
                            stg.sbl_attach_id,
                            stg.sbl_quote_id,
                            stg.prnumber,
                            stg.revision,
                            stg.customer_id,
                            stg.salerep_id,
                            NVL (stg.lwsp_flag, 'N'),
                            stg.pr_trans_type,
                            'PENDING'        status,
                            NULL             description,
                            'Y',
                            NULL,
                            NULL,
                            NULL,
                            stg.exp_flag     express_flag
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.active_flg = 'Y';

        --
        --  Existing Accounts
        CURSOR c_old IS
            SELECT dtl.uid_alps_account,
                   dtl.status,
                   dtl.idr_status,
                   dtl.sca_status,
                   dtl.idr_source,
                   dtl.sca_source,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account    meter_id,
                   dtl.updated_dt,
                   dtl.meter_type
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND dtl.uid_mml_master = mstr.uid_mml_master
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.ldc_account = stg.ldc_account;

        --  NEW accounts Not previously included in the previous revision
        CURSOR c_new IS
            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   stg.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   NULL,                              -- 'VALIDATION_SUCCESS',
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   --'VALIDATION_SUCCESS',
                   alps.f_digit_key_requestovrd (mstr.sbl_quote_id,
                                                 stg.ldc_account,
                                                 stg.market_code,
                                                 stg.disco_code,
                                                 'VALIDATION_SUCCESS',
                                                 stg.digit_code_key),
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.uid_mml_master =
                                       mstr.uid_mml_master
                                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.ldc_account = stg.ldc_account);

        lv_new_quote_id      alps_mml_pr_detail.sbl_quote_id%TYPE;
        lv_basa_ind          VARCHAR2 (3);
        lv_nstar_ind         VARCHAR2 (3);
        lv_other_ind         VARCHAR2 (3);

        lv_ba_cnt            INTEGER;
        lv_pe_dt             DATE;
        lv_vee_dt            DATE;
        lv_idr_status        alps_mml_pr_detail.idr_status%TYPE;
        lv_sca_status        alps_mml_pr_detail.sca_status%TYPE;
        lv_desc              ALPS_MML_PR_DETAIL.DESCRIPTION%TYPE;
        lv_status            alps_mml_pr_detail.status%TYPE;
        lv_distinct_status   alps_mml_pr_detail.status%TYPE;
        lv_cnt               INTEGER;
    BEGIN
        gv_code_proc := 'SETUP_MML_REVISE_ACCOUNTS';

        /*Aurora Change*/
        FOR rec IN (SELECT NVL (EXP_FLAG, 'N')     EXP_FLAG
                      FROM alps_mml_stg
                     WHERE sbl_attach_id = p_attach_id AND ROWNUM < 2)
        LOOP
            gv_express_flag := rec.EXP_FLAG;
            DBMS_OUTPUT.put_line ('Setting gv_express_flag:' || rec.EXP_FLAG);
        END LOOP;

        -- Get PR data for new Revision
        gv_code_ln := $$plsql_line;

        SELECT DISTINCT hdr.sbl_quote_id
          INTO lv_new_quote_id
          FROM alps_mml_stg stg, alps_mml_pr_header hdr
         WHERE     stg.sbl_attach_id = p_attach_id
               AND stg.status = 'VALIDATION_PENDING'
               AND stg.prnumber = hdr.prnumber
               AND stg.sbl_quote_id = hdr.sbl_quote_id
               AND active_flg = 'Y';

        --
        SELECT DECODE (COUNT (1), 0, 'NO', 'YES')
          INTO lv_basa_ind
          FROM alps_mml_stg
         WHERE     disco_code IN ('WMECO',
                                  'CLP',
                                  'PSNH',
                                  'AMERENCIPS',
                                  'AMERENCILCO',
                                  'AMERENIP')
               AND status = 'VALIDATION_PENDING'
               AND sbl_attach_id = p_attach_id;

        --
        SELECT DECODE (COUNT (1), 0, 'NO', 'YES')
          INTO lv_nstar_ind
          FROM alps_mml_stg stg
         WHERE     stg.sbl_attach_id = p_attach_id
               AND stg.status = 'VALIDATION_PENDING'
               AND stg.disco_code IN ('COMELEC',
                                      'BECO',
                                      'CMBRDG',
                                      'ATSICE',
                                      'ATSIOE',
                                      'ATSITE',
                                      'DPL',
                                      'DPLDE');


        SELECT DECODE (COUNT (1), 0, 'NO', 'YES')
          INTO lv_other_ind
          FROM alps_mml_stg stg
         WHERE     stg.sbl_attach_id = p_attach_id
               AND stg.disco_code NOT IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE',
                                          'CLP',
                                          'WMECO',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP');

        DBMS_OUTPUT.put_line ('lv_basa_ind ==>  ' || lv_basa_ind);
        DBMS_OUTPUT.put_line ('lv_nstar_ind ==>  ' || lv_nstar_ind);

        -- WMECO and CLP can attached BA or BA_SA accounts in the MML
        IF lv_basa_ind = 'YES'
        THEN
            DBMS_OUTPUT.put_line ('In WMECO/CLP/PSNH ');

            --
            -- update their Attributes in ALPS for any Matching MML BA_SA accounts,
            --
            SELECT dtl.uid_alps_account,
                   dtl.status,
                   dtl.idr_status,
                   --dtl.sca_status,
                   alps.f_digit_key_requestovrd (dtl.sbl_quote_id,
                                                 dtl.ldc_account,
                                                 dtl.market_code,
                                                 dtl.disco_code,
                                                 dtl.sca_status,
                                                 stg.digit_code_key)
                       sca_status,
                   dtl.idr_source,
                   dtl.sca_source,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   dtl.updated_dt,
                   dtl.meter_type
              BULK COLLECT INTO arr_old_acct
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND dtl.uid_mml_master = mstr.uid_mml_master
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.ldc_account = stg.ldc_account
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   AND INSTR (stg.ldc_account, '_') > 0;

            --
            IF arr_old_acct.COUNT > 0
            THEN
                DBMS_OUTPUT.put_line (
                    'matched ba_sa accounts ==> ' || arr_old_acct.COUNT);

                FOR ix IN arr_old_acct.FIRST .. arr_old_acct.LAST
                LOOP
                    revised_acct_status (arr_old_acct (ix).meter_id,
                                         arr_old_acct (ix).meter_type,
                                         arr_old_acct (ix).status,
                                         lv_status,
                                         lv_idr_status,
                                         lv_sca_status,
                                         lv_pe_dt,
                                         lv_vee_dt,
                                         arr_old_acct (ix).last_refresh_dt);

                    -- Update account attributes
                    gv_code_ln := $$plsql_line;

                    UPDATE alps_mml_pr_detail
                       SET status = NVL (lv_status, arr_old_acct (ix).status),
                           idr_status =
                               NVL (lv_idr_status,
                                    arr_old_acct (ix).idr_status),
                           sca_status =
                               DECODE (
                                   arr_old_acct (ix).sca_status,
                                   'REQUEST_OVERRIDE', 'REQUEST_OVERRIDE',
                                   NVL (lv_sca_status,
                                        arr_old_acct (ix).sca_status)),
                           idr_source = arr_old_acct (ix).idr_source,
                           sca_source = arr_old_acct (ix).sca_source,
                           address1 = arr_old_acct (ix).address1,
                           address2 = arr_old_acct (ix).address2,
                           city = arr_old_acct (ix).city,
                           state = arr_old_acct (ix).state,
                           zip = arr_old_acct (ix).zip,
                           digit_code_key = arr_old_acct (ix).digit_code_key,
                           disco_code = arr_old_acct (ix).disco_code,
                           meter_id = arr_old_acct (ix).meter_id,
                           last_pe_setup_dt = lv_pe_dt,
                           last_vee_completed_dt = lv_vee_dt,
                           description = lv_desc
                     WHERE uid_alps_account =
                           arr_old_acct (ix).uid_alps_account;

                    DBMS_OUTPUT.put_line (
                           'BASA BA_SA Line:'
                        || $$plsql_line
                        || 'Update Completed:'
                        || arr_old_acct (ix).uid_alps_account);
                END LOOP;
            END IF;

            IF 1 = 1
            THEN
                --If not match by BA_SA then Matched BA accounts.
                SELECT dtl.uid_alps_account,
                       dtl.status,
                       --  DECODE(dtl.status,'VEE_COMPLETE','READY_FOR_VEE','OFFER_SUMMARY_COMPLETE','READY_FOR_VEE',dtl.status) status,
                       dtl.idr_status,
                       --dtl.sca_status,
                       alps.f_digit_key_requestovrd (dtl.sbl_quote_id,
                                                     dtl.ldc_account,
                                                     dtl.market_code,
                                                     dtl.disco_code,
                                                     dtl.sca_status,
                                                     stg.digit_code_key)
                           sca_status,
                       dtl.idr_source,
                       dtl.sca_source,
                       stg.address1,
                       stg.address2,
                       stg.city,
                       stg.state,
                       stg.zip,
                       stg.digit_code_key,
                       stg.disco_code,
                          dtl.market_code
                       || '_'
                       || dtl.disco_code
                       || '_'
                       || dtl.ldc_account,
                       dtl.updated_dt,
                       dtl.meter_type
                  BULK COLLECT INTO arr_old_acct
                  FROM alps_mml_stg        stg,
                       alps_mml_pr_header  mstr,
                       alps_mml_pr_detail  dtl
                 WHERE     stg.sbl_attach_id = p_attach_id
                       AND stg.status = 'VALIDATION_PENDING'
                       AND mstr.sbl_quote_id = stg.sbl_quote_id
                       AND mstr.prnumber = stg.prnumber
                       AND mstr.active_flg = 'Y'
                       AND dtl.uid_mml_master = mstr.uid_mml_master
                       AND dtl.sbl_quote_id = mstr.sbl_quote_id
                       AND dtl.market_code = stg.market_code
                       AND stg.disco_code IN ('WMECO',
                                              'CLP',
                                              'PSNH',
                                              'AMERENCIPS',
                                              'AMERENCILCO',
                                              'AMERENIP')
                       AND stg.ldc_account =
                           DECODE (
                               INSTR (dtl.ldc_account, '_'),
                               0, dtl.ldc_account,
                               SUBSTR (dtl.ldc_account,
                                       1,
                                       INSTR (dtl.ldc_account, '_') - 1))
                       --Only Pickup BAs that don't have scenario of BA records and BASA records
                       AND NOT EXISTS
                               (SELECT 1
                                  FROM alps_mml_stg
                                 WHERE     ldc_account LIKE
                                               stg.ldc_account || '_%'
                                       AND sbl_attach_id = stg.sbl_attach_id);

                --
                IF arr_old_acct.COUNT > 0
                THEN
                    FOR ix IN arr_old_acct.FIRST .. arr_old_acct.LAST
                    LOOP
                        revised_acct_status (
                            arr_old_acct (ix).meter_id,
                            arr_old_acct (ix).meter_type,
                            arr_old_acct (ix).status,
                            lv_status,
                            lv_idr_status,
                            lv_sca_status,
                            lv_pe_dt,
                            lv_vee_dt,
                            arr_old_acct (ix).last_refresh_dt);
                        -- Update account attributes
                        gv_code_ln := $$plsql_line;

                        UPDATE alps_mml_pr_detail
                           SET status =
                                   NVL (lv_status, arr_old_acct (ix).status),
                               idr_status =
                                   NVL (lv_idr_status,
                                        arr_old_acct (ix).idr_status),
                               sca_status =
                                   DECODE (
                                       arr_old_acct (ix).sca_status,
                                       'REQUEST_OVERRIDE', 'REQUEST_OVERRIDE',
                                       NVL (lv_sca_status,
                                            arr_old_acct (ix).sca_status)),
                               idr_source = arr_old_acct (ix).idr_source,
                               sca_source = arr_old_acct (ix).sca_source,
                               address1 = arr_old_acct (ix).address1,
                               address2 = arr_old_acct (ix).address2,
                               city = arr_old_acct (ix).city,
                               state = arr_old_acct (ix).state,
                               zip = arr_old_acct (ix).zip,
                               digit_code_key =
                                   arr_old_acct (ix).digit_code_key,
                               disco_code = arr_old_acct (ix).disco_code,
                               meter_id = arr_old_acct (ix).meter_id,
                               last_pe_setup_dt = lv_pe_dt,
                               last_vee_completed_dt = lv_vee_dt,
                               description = lv_desc
                         WHERE uid_alps_account =
                               arr_old_acct (ix).uid_alps_account;

                        DBMS_OUTPUT.put_line (
                               'BASA BA Only Line:'
                            || $$plsql_line
                            || 'Update Completed:'
                            || arr_old_acct (ix).uid_alps_account);
                    END LOOP;
                END IF;
            END IF;

            --
            -- Insert new MML BA-SA accounts into ALPS
            --
            gv_code_ln := $$plsql_line;

            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   stg.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   NULL,
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   'VALIDATION_SUCCESS',
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              BULK COLLECT INTO arr_detl_acct
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP')
                   --AND INSTR(stg.ldc_account,'_') > 0
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.uid_mml_master =
                                       mstr.uid_mml_master
                                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.disco_code = stg.disco_code
                                   AND dtl.disco_code IN ('WMECO',
                                                          'CLP',
                                                          'PSNH',
                                                          'AMERENCIPS',
                                                          'AMERENCILCO',
                                                          'AMERENIP')
                                   AND dtl.ldc_account = stg.ldc_account);

            DBMS_OUTPUT.put_line (
                ' NWE -arr_detl_acct.count ' || arr_detl_acct.COUNT);

            --
            IF arr_detl_acct.COUNT > 0
            THEN
                FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                LOOP
                    DBMS_OUTPUT.put_line (
                           ' NEW - LDC_ACCOUNT ==>  '
                        || arr_detl_acct (ix).ldc_account);

                    -- Not match by full ACCOUNT , do not insert until no match by BA PREFIX
                    SELECT COUNT (1)
                      INTO lv_cnt
                      FROM alps_mml_pr_detail dtl, alps_mml_pr_header hdr
                     WHERE     hdr.sbl_attach_id = p_attach_id
                           AND dtl.sbl_quote_id = hdr.sbl_quote_id
                           AND hdr.active_flg = 'Y'
                           AND INSTR (dtl.ldc_account, '_') > 1
                           AND dtl.disco_code IN ('WMECO',
                                                  'CLP',
                                                  'PSNH',
                                                  'AMERENCIPS',
                                                  'AMERENCILCO',
                                                  'AMERENIP')
                           AND SUBSTR (dtl.ldc_account,
                                       1,
                                       INSTR (dtl.ldc_account, '_') - 1) =
                               arr_detl_acct (ix).ldc_account
                           AND market_code = arr_detl_acct (ix).market_code
                           AND disco_code = arr_detl_acct (ix).disco_code;

                    DBMS_OUTPUT.put_line (' EXISTING - lv_cnt ' || lv_cnt);

                    --
                    IF lv_cnt = 0
                    THEN
                        -- INSERT
                        IF arr_detl_acct.COUNT > 0
                        THEN
                            DBMS_OUTPUT.put_line (
                                ' same PR - INSERT  PR DETAIL BASA ACCOUNT ');
                            --                   FOR ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST LOOP
                            arr_detl_acct (ix).uid_alps_account :=
                                alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                            --                   END LOOP;
                            --
                            gv_code_ln := $$plsql_line;

                            --                  FORALL ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST
                            INSERT INTO alps_mml_pr_detail
                                 VALUES arr_detl_acct (ix);
                        END IF;
                    END IF;
                END LOOP;
            END IF;

            /*      IF arr_detl_acct.count > 0 THEN
                    DBMS_OUTPUT.PUT_LINE('if NEW ba_sa accounts founded then INSERT==> ' ||arr_detl_acct.count  );
                      IF arr_detl_acct.count > 0 THEN
                   -- Not match by full ACCOUNT , do not insert until no match by BA PREFIX
                          SELECT
                           NULL,
                           SYSDATE,
                           USER,
                           SYSDATE,
                           USER,
                           mstr.sbl_quote_id,
                           mstr.uid_mml_master,
                           stg.ldc_account,
                           NULL,
                           stg.market_code,
                           stg.disco_code,
                           'VALIDATION_SUCCESS',
                           NULL,
                           stg.address1,
                           stg.address2,
                           stg.city,
                           stg.state,
                           stg.zip,
                           stg.digit_code_key,
                           'VALIDATION',
                          'VALIDATION_SUCCESS',
                         'VALIDATION_SUCCESS',
                           NULL,
                           NULL,
                           stg.market_code||'_'||stg.disco_code||'_'||stg.ldc_account,
                           NULL,
                           NULL
                       BULK COLLECT INTO arr_detl_acct_ba
                       FROM alps_mml_stg stg,
                                alps_mml_pr_header mstr,
                   ( SELECT SUBSTR(dtl.ldc_account,1,INSTR(dtl.ldc_account,'_')-1) ldc_account,
                                   dtl.sbl_quote_id,
                                   market_code,
                                   disco_code
                       FROM  alps_mml_pr_detail dtl, alps_mml_pr_header hdr
                       WHERE HDR.sbl_attach_id = p_attach_id
                       AND dtl.sbl_quote_id  = hdr.sbl_quote_id
                       AND hdr.active_flg = 'Y'
                       AND INSTR(dtl.ldc_account,'_') > 1
                       AND  dtl.disco_code IN ('WMECO','CLP')
                                                   )   dtl
                       WHERE stg.sbl_attach_id = p_attach_id
                       AND  stg.sbl_quote_id = mstr.sbl_quote_id
                       AND  stg.prnumber = mstr.prnumber
                       AND stg.status  = 'VALIDATION_PENDING'
                       AND mstr.active_flg = 'Y'
                      AND  stg.disco_code IN ('WMECO','CLP')
                      AND INSTR(stg.ldc_account,'_') = 0
                      AND stg.ldc_account =  dtl.ldc_account
                      AND stg.market_code = dtl.market_code
                      AND stg.disco_code =  dtl.disco_code;
                      --
                          DBMS_OUTPUT.PUT_LINE(' arr_detl_acct_ba.COUNT ==> ' ||arr_detl_acct_ba.count  );

                      IF arr_detl_acct_ba.count = 0 THEN
                          -- INSERT
                                              dbms_output.put_line(' same PR - INSERT  PR DETAIL BASA ACCOUNT ');
                              FOR ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST LOOP
                                  arr_detl_acct(ix).uid_alps_account := ALPS_MML_ACCT_SEQ.nextval;      --Assign the account sequence_id
                              END LOOP;
                              --
                               gv_code_ln :=  $$plsql_line;
                               FORALL ix IN arr_detl_acct.FIRST..arr_detl_acct.LAST
                                   INSERT INTO ALPS_MML_PR_DETAIL  VALUES arr_detl_acct(ix);
                          END IF;

                      END IF;
                  END IF;
           */
            --
            -- Remove ALPS BA_SA accounts ONLY both BA and BA_SA not included in the MML
            --
            gv_code_ln := $$plsql_line;

            -- Retrieve ALPS BA_SA accounts
            SELECT dtl.uid_alps_account,
                   dtl.ldc_account,
                   dtl.market_code,
                   dtl.disco_code
              BULK COLLECT INTO arr_del_accts
              FROM alps_mml_pr_header mstr, alps_mml_pr_detail dtl
             WHERE     mstr.sbl_quote_id = lv_new_quote_id
                   AND mstr.active_flg = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.disco_code IN ('WMECO',
                                          'CLP',
                                          'PSNH',
                                          'AMERENCIPS',
                                          'AMERENCILCO',
                                          'AMERENIP');

            --
            IF arr_del_accts.COUNT > 0
            THEN
                gv_code_ln := $$plsql_line;

                FOR ix IN arr_del_accts.FIRST .. arr_del_accts.LAST
                LOOP
                    -- Check MML for the matching BA
                    SELECT COUNT (1)
                      INTO lv_ba_cnt
                      FROM alps_mml_stg
                     WHERE     sbl_attach_id = p_attach_id
                           AND status = 'VALIDATION_PENDING'
                           AND disco_code IN ('WMECO',
                                              'CLP',
                                              'PSNH',
                                              'AMERENCIPS',
                                              'AMERENCILCO',
                                              'AMERENIP')
                           AND INSTR (ldc_account, '_') = 0
                           AND ldc_account =
                               SUBSTR (
                                   arr_del_accts (ix).ldc_account,
                                   1,
                                     INSTR (arr_del_accts (ix).ldc_account,
                                            '_')
                                   - 1)
                           AND market_code = arr_del_accts (ix).market_code
                           AND disco_code = arr_del_accts (ix).disco_code;

                    --
                    IF lv_ba_cnt = 0
                    THEN
                        -- No macthing BA then matched by BA_SA accounts
                        SELECT COUNT (1)
                          INTO lv_ba_cnt
                          FROM alps_mml_stg
                         WHERE     sbl_attach_id = p_attach_id
                               AND status = 'VALIDATION_PENDING'
                               AND disco_code IN ('WMECO',
                                                  'CLP',
                                                  'PSNH',
                                                  'AMERENCIPS',
                                                  'AMERENCILCO',
                                                  'AMERENIP')
                               --AND INSTR(ldc_account,'_') > 0
                               AND ldc_account =
                                   arr_del_accts (ix).ldc_account
                               AND market_code =
                                   arr_del_accts (ix).market_code
                               AND disco_code = arr_del_accts (ix).disco_code;

                        --  Could not find a match by neither BA nor BA_SA account in MML ; this BA_SA ca be remove from ALPS
                        IF lv_ba_cnt = 0
                        THEN
                            DELETE FROM
                                alps_mml_pr_detail dtl
                                  WHERE uid_alps_account =
                                        arr_del_accts (ix).uid_alps_account;
                        END IF;
                    END IF;
                END LOOP;
            END IF;
        END IF;

        IF lv_nstar_ind = 'YES'
        THEN -- NSTAR where for a same account,  MML disco not matched with ALPS disco
            SELECT dtl.uid_alps_account,
                   dtl.status,
                   --    DECODE(dtl.status,'VEE_COMPLETE','READY_FOR_VEE','OFFER_SUMMARY_COMPLETE','READY_FOR_VEE',dtl.status) status,
                   dtl.idr_status,
                   dtl.sca_status,
                   dtl.idr_source,
                   dtl.sca_source,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code,
                      dtl.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || dtl.ldc_account,
                   dtl.updated_dt,
                   dtl.meter_type
              BULK COLLECT INTO arr_old_acct
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.ldc_account = stg.ldc_account
                   AND stg.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE');

            --
            IF arr_old_acct.COUNT > 0
            THEN
                DBMS_OUTPUT.put_line (
                    'matched mrket, ldc_account  ==> ' || arr_old_acct.COUNT);

                FOR ix IN arr_old_acct.FIRST .. arr_old_acct.LAST
                LOOP
                    revised_acct_status (arr_old_acct (ix).meter_id,
                                         arr_old_acct (ix).meter_type,
                                         arr_old_acct (ix).status,
                                         lv_status,
                                         lv_idr_status,
                                         lv_sca_status,
                                         lv_pe_dt,
                                         lv_vee_dt,
                                         arr_old_acct (ix).last_refresh_dt);

                    -- Update account attributes
                    gv_code_ln := $$plsql_line;

                    UPDATE alps_mml_pr_detail
                       SET status = NVL (lv_status, arr_old_acct (ix).status),
                           idr_status =
                               NVL (lv_idr_status,
                                    arr_old_acct (ix).idr_status),
                           sca_status =
                               NVL (lv_sca_status,
                                    arr_old_acct (ix).sca_status),
                           idr_source = arr_old_acct (ix).idr_source,
                           sca_source = arr_old_acct (ix).sca_source,
                           address1 = arr_old_acct (ix).address1,
                           address2 = arr_old_acct (ix).address2,
                           city = arr_old_acct (ix).city,
                           state = arr_old_acct (ix).state,
                           zip = arr_old_acct (ix).zip,
                           digit_code_key = arr_old_acct (ix).digit_code_key,
                           --  disco_code = arr_old_acct (ix).disco_code,
                           --                      meter_id = arr_old_acct (ix).meter_id,
                           last_pe_setup_dt = lv_pe_dt,
                           last_vee_completed_dt = lv_vee_dt,
                           description = lv_desc
                     WHERE uid_alps_account =
                           arr_old_acct (ix).uid_alps_account;

                    DBMS_OUTPUT.put_line (
                           'NStar Utilities Line:'
                        || $$plsql_line
                        || 'Update Completed:'
                        || arr_old_acct (ix).uid_alps_account);
                END LOOP;
            END IF;

            --
            -- Insert new accounts into ALPS
            --
            gv_code_ln := $$plsql_line;

            SELECT NULL,
                   SYSDATE,
                   USER,
                   SYSDATE,
                   USER,
                   stg.sbl_quote_id,
                   mstr.uid_mml_master,
                   stg.ldc_account,
                   NULL,
                   stg.market_code,
                   stg.disco_code,
                   NULL,
                   NULL,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   'VALIDATION',
                   'VALIDATION_SUCCESS',
                   'VALIDATION_SUCCESS',
                   NULL,
                   NULL,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account,
                   NULL,
                   NULL
              BULK COLLECT INTO arr_detl_acct
              FROM alps_mml_stg stg, alps_mml_pr_header mstr
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND stg.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE')
                   AND NOT EXISTS
                           (SELECT 1
                              FROM alps_mml_pr_detail dtl
                             WHERE     dtl.uid_mml_master =
                                       mstr.uid_mml_master
                                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                                   AND dtl.market_code = stg.market_code
                                   AND dtl.ldc_account = stg.ldc_account);

            --
            IF arr_detl_acct.COUNT > 0
            THEN
                DBMS_OUTPUT.put_line (
                       'if NEW accounts founded then INSERT==> '
                    || arr_detl_acct.COUNT);
                gv_code_ln := $$plsql_line;

                FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                LOOP
                    arr_detl_acct (ix).uid_alps_account :=
                        alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                END LOOP;

                --
                gv_code_ln := $$plsql_line;

                FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    INSERT INTO alps_mml_pr_detail
                         VALUES arr_detl_acct (ix);
            END IF;

            --
            -- Remove ALPS accounts NOT litsted in MML
            --
            gv_code_ln := $$plsql_line;

            SELECT dtl.uid_alps_account,
                   dtl.ldc_account,
                   dtl.market_code,
                   dtl.disco_code
              BULK COLLECT INTO arr_del_accts
              FROM alps_mml_pr_header mstr, alps_mml_pr_detail dtl
             WHERE     mstr.sbl_quote_id = lv_new_quote_id
                   AND mstr.active_flg = 'Y'
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.disco_code IN ('COMELEC',
                                          'BECO',
                                          'CMBRDG',
                                          'ATSICE',
                                          'ATSIOE',
                                          'ATSITE',
                                          'DPL',
                                          'DPLDE');

            --
            IF arr_del_accts.COUNT > 0
            THEN
                gv_code_ln := $$plsql_line;

                FOR ix IN arr_del_accts.FIRST .. arr_del_accts.LAST
                LOOP
                    -- Check MML for the matching market/disco combination
                    SELECT COUNT (1)
                      INTO lv_ba_cnt
                      FROM alps_mml_stg
                     WHERE     sbl_attach_id = p_attach_id
                           AND status = 'VALIDATION_PENDING'
                           AND disco_code IN ('COMELEC',
                                              'BECO',
                                              'CMBRDG',
                                              'ATSICE',
                                              'ATSIOE',
                                              'ATSITE',
                                              'DPL',
                                              'DPLDE')
                           AND ldc_account = arr_del_accts (ix).ldc_account
                           AND market_code = arr_del_accts (ix).market_code;

                    --
                    IF lv_ba_cnt = 0
                    THEN
                        DELETE FROM
                            alps_mml_pr_detail dtl
                              WHERE uid_alps_account =
                                    arr_del_accts (ix).uid_alps_account;
                    END IF;
                END LOOP;
            END IF;
        END IF;

        IF lv_other_ind = 'YES'
        THEN
            -- The rest of the market
            -- Update existing Accounts
            gv_code_ln := $$plsql_line;

            SELECT dtl.uid_alps_account,
                   dtl.status,
                   --      DECODE(dtl.status,'VEE_COMPLETE', 'READY_FOR_VEE') ,'OFFER_SUMMARY_COMPLETE','READY_FOR_VEE',dtl.status) status,
                   dtl.idr_status,
                   --dtl.sca_status,
                   alps.f_digit_key_requestovrd (dtl.sbl_quote_id,
                                                 dtl.ldc_account,
                                                 dtl.market_code,
                                                 dtl.disco_code,
                                                 dtl.sca_status,
                                                 stg.digit_code_key)
                       sca_status,
                   dtl.idr_source,
                   dtl.sca_source,
                   stg.address1,
                   stg.address2,
                   stg.city,
                   stg.state,
                   stg.zip,
                   stg.digit_code_key,
                   stg.disco_code,
                      stg.market_code
                   || '_'
                   || stg.disco_code
                   || '_'
                   || stg.ldc_account
                       meter_id,
                   dtl.updated_dt,
                   dtl.meter_type
              BULK COLLECT INTO arr_old_acct
              FROM alps_mml_stg        stg,
                   alps_mml_pr_header  mstr,
                   alps_mml_pr_detail  dtl
             WHERE     stg.sbl_attach_id = p_attach_id
                   AND stg.status = 'VALIDATION_PENDING'
                   AND mstr.sbl_quote_id = stg.sbl_quote_id
                   AND mstr.prnumber = stg.prnumber
                   AND mstr.active_flg = 'Y'
                   AND dtl.uid_mml_master = mstr.uid_mml_master
                   AND dtl.sbl_quote_id = mstr.sbl_quote_id
                   AND dtl.market_code = stg.market_code
                   AND dtl.ldc_account = stg.ldc_account
                   AND stg.disco_code NOT IN ('COMELEC',
                                              'BECO',
                                              'CMBRDG',
                                              'ATSICE',
                                              'ATSIOE',
                                              'ATSITE',
                                              'DPL',
                                              'DPLDE',
                                              'CLP',
                                              'WMECO',
                                              'PSNH',
                                              'AMERENCIPS',
                                              'AMERENCILCO',
                                              'AMERENIP');

            --

            IF arr_old_acct.COUNT > 0
            THEN
                gv_code_ln := $$plsql_line;

                FOR ix IN arr_old_acct.FIRST .. arr_old_acct.LAST
                LOOP
                    DBMS_OUTPUT.put_line (
                           'BEFORE revised_acct_status - meter :'
                        || arr_old_acct (ix).meter_id
                        || '; Update Completed:'
                        || arr_old_acct (ix).uid_alps_account
                        || '; meter type : '
                        || arr_old_acct (ix).meter_type);

                    revised_acct_status (arr_old_acct (ix).meter_id,
                                         arr_old_acct (ix).meter_type,
                                         arr_old_acct (ix).status,
                                         lv_status,
                                         lv_idr_status,
                                         lv_sca_status,
                                         lv_pe_dt,
                                         lv_vee_dt,
                                         arr_old_acct (ix).last_refresh_dt);



                    DBMS_OUTPUT.put_line (
                           'AAFTER revised_acct_status;  '
                        || 'lv_idr_status :'
                        || lv_idr_status
                        || '; lv_sca_status :'
                        || lv_sca_status
                        || ';lv_status :'
                        || NVL (lv_status, arr_old_acct (ix).status));

                    DBMS_OUTPUT.put_line (
                           'AFTER  revised_acct_status - meter :'
                        || arr_old_acct (ix).meter_id
                        || '; Update Completed:'
                        || arr_old_acct (ix).uid_alps_account
                        || '; meter type : '
                        || arr_old_acct (ix).meter_type);

                    DBMS_OUTPUT.put_line ('Values before update to MML:');

                    FOR rec
                        IN (SELECT status, sca_status, idr_status
                              FROM alps_mml_pr_detail
                             WHERE uid_alps_account =
                                   arr_old_acct (ix).uid_alps_account)
                    LOOP
                        DBMS_OUTPUT.put_line (
                               'status:'
                            || rec.status
                            || '; sca_status:'
                            || rec.sca_status
                            || ';idr_status'
                            || rec.idr_status);
                    END LOOP;


                    DBMS_OUTPUT.put_line (
                           'Statuses being updated: lv_status:'
                        || lv_status
                        || '; lv_sca_status:'
                        || lv_sca_status
                        || ';lv_idr_status'
                        || lv_idr_status);

                    --
                    UPDATE alps_mml_pr_detail
                  Set status = NVL (lv_status, arr_old_acct (ix).status),
                      idr_status =
                         NVL (lv_idr_status, arr_old_acct (ix).idr_status),
                      sca_status =
                         DECODE (
                            arr_old_acct (ix).sca_status,
                            'REQUEST_OVERRIDE', 'REQUEST_OVERRIDE',
                            NVL (lv_sca_status, arr_old_acct (ix).sca_status)),
                           idr_source = arr_old_acct (ix).idr_source,
                           sca_source = arr_old_acct (ix).sca_source,
                           address1 = arr_old_acct (ix).address1,
                           address2 = arr_old_acct (ix).address2,
                           city = arr_old_acct (ix).city,
                           state = arr_old_acct (ix).state,
                           zip = arr_old_acct (ix).zip,
                           digit_code_key = arr_old_acct (ix).digit_code_key,
                           disco_code = arr_old_acct (ix).disco_code,
                           meter_id = arr_old_acct (ix).meter_id,
                           last_pe_setup_dt = lv_pe_dt,
                           last_vee_completed_dt = lv_vee_dt,
                      description = NULL
                Where uid_alps_account = arr_old_acct (ix).uid_alps_account;

                    --
                    UPDATE alps_mml_pr_detail
                       SET status =
                               NVL (
                                   DECODE (
                                       lv_sca_status,
                                       'DATA_NEEDS_EXP_VEE', 'EXP_VEE',
                                       (SELECT DISTINCT status
                                          FROM alps_status_matrix
                                         WHERE     idr_status = lv_idr_status
                                               AND sca_status = lv_sca_status
                                               AND applicability =
                                                   'MML_DETAIL'
                                               AND ROWNUM < 2
                                               AND active_flag = 'Y')),
                                   arr_old_acct (ix).status)
                     WHERE uid_alps_account =
                           arr_old_acct (ix).uid_alps_account;

                    DBMS_OUTPUT.put_line (
                           'ROW UPDATED ==> '
                        || SQL%ROWCOUNT
                        || ' FOR UID ==> '
                        || arr_old_acct (ix).uid_alps_account);

                    DBMS_OUTPUT.put_line (
                           'lv_idr_status:'
                        || NVL (lv_idr_status, arr_old_acct (ix).idr_status)
                        || '; lv_sca_status:'
                        || NVL (lv_sca_status, arr_old_acct (ix).sca_status)
                        || ';lv_status:'
                        || NVL (lv_status, arr_old_acct (ix).status));


                    DBMS_OUTPUT.put_line ('Values after update to MML:');

                    FOR rec
                        IN (SELECT status, sca_status, idr_status
                              FROM alps_mml_pr_detail
                             WHERE uid_alps_account =
                                   arr_old_acct (ix).uid_alps_account)
                    LOOP
                        DBMS_OUTPUT.put_line (
                               'status:'
                            || rec.status
                            || '; sca_status:'
                            || rec.sca_status
                            || ';idr_status'
                            || rec.idr_status);
                    END LOOP;
                END LOOP;
            END IF;

            -- Add new Accounts
            gv_code_ln := $$plsql_line;

            OPEN c_new;

            LOOP
                FETCH c_new
                    BULK COLLECT INTO arr_detl_acct
                    LIMIT gv_bulk_limit;

                --
                IF arr_detl_acct.COUNT > 0
                THEN
                    FOR ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                    LOOP
                        arr_detl_acct (ix).uid_alps_account :=
                            alps_mml_acct_seq.NEXTVAL; --Assign the account sequence_id
                    END LOOP;

                    --
                    gv_code_ln := $$plsql_line;

                    FORALL ix IN arr_detl_acct.FIRST .. arr_detl_acct.LAST
                        INSERT INTO alps_mml_pr_detail
                             VALUES arr_detl_acct (ix);
                END IF;

                EXIT WHEN c_new%NOTFOUND;
            END LOOP;

            CLOSE c_new;

            -- Remove accounts not included in the new revision MML
            gv_code_ln := $$plsql_line;

            DELETE FROM
                alps_mml_pr_detail dtl
                  WHERE     dtl.sbl_quote_id = lv_new_quote_id
                        AND NOT EXISTS
                                (SELECT 1
                                   FROM alps_mml_stg        stg,
                                        alps_mml_pr_header  mstr
                                  WHERE     stg.sbl_quote_id =
                                            lv_new_quote_id
                                        AND stg.disco_code = dtl.disco_code
                                        AND stg.ldc_account = dtl.ldc_account
                                        AND stg.market_code = dtl.market_code
                                        AND stg.status = 'VALIDATION_PENDING'
                                        AND mstr.sbl_quote_id =
                                            stg.sbl_quote_id
                                        AND mstr.prnumber = stg.prnumber
                                        AND mstr.active_flg = 'Y');
        END IF;

        -- Update PR HEADER
        gv_code_ln := $$plsql_line;

        OPEN c_same_pr;

        LOOP
            FETCH c_same_pr
                BULK COLLECT INTO arr_mstr_acct
                LIMIT gv_bulk_limit;

            IF arr_mstr_acct.COUNT > 0
            THEN
                -- Begin codes added per SR-1-341430906 : if all underlying accounts are VEE_COMPLETE then update PR status accordingly
                /*
                 FOR ix IN arr_mstr_acct.FIRST..arr_mstr_acct.LAST
                    LOOP
                       SELECT COUNT(DISTINCT status)
                       INTO lv_cnt
                       FROM alps_mml_pr_detail
                       WHERE sbl_quote_id = arr_mstr_acct(ix).sbl_quote_id;
                        --
                        IF lv_cnt = 1 THEN
                            gv_code_ln :=  $$plsql_line;
                            SELECT DISTINCT status
                            INTO lv_distinct_status
                            FROM alps_mml_pr_detail
                            WHERE sbl_quote_id = arr_mstr_acct(ix).sbl_quote_id
                            AND ROWNUM < 2;

                          -- Update PR status to SETUP_READY if all underlying PR account are VEE_COMPLETE else  PENDING
                            IF   lv_distinct_status = 'VEE_COMPLETE' THEN
                                 arr_mstr_acct(ix).status :=  'SETUP_READY';
                            END IF;
                        END IF;

                    END LOOP;
                    */
                -- Ended codes added per SR-1-341430906

                DBMS_OUTPUT.put_line (' same PR - UPDATE  PR  HEADER');

                FORALL ix IN arr_mstr_acct.FIRST .. arr_mstr_acct.LAST
                    UPDATE alps_mml_pr_header
                       SET sbl_attach_id = arr_mstr_acct (ix).sbl_attach_id,
                           customer_id = arr_mstr_acct (ix).customer_id,
                           salerep_id = arr_mstr_acct (ix).salerep_id,
                           pr_trans_type = arr_mstr_acct (ix).pr_trans_type,
                           description = arr_mstr_acct (ix).description,
                           status = arr_mstr_acct (ix).status
                     --   last_siebel_setup_dt = NULL,
                     --   last_offer_summary_dt = NULL
                     WHERE uid_mml_master = arr_mstr_acct (ix).uid_mml_master;
            END IF;

            EXIT WHEN c_same_pr%NOTFOUND;
        END LOOP;

        CLOSE c_same_pr;

        --
        COMMIT;
        arr_old_acct.delete;
        arr_detl_acct.delete;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line (
                   SQLERRM
                || ' at line '
                || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ());
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END setup_mml_revise_accounts;

    FUNCTION f_request_data_is_old (p_quote_id   IN VARCHAR2,
                                    p_meter_id   IN VARCHAR2)
        RETURN VARCHAR2
    AS
        lv_cnt   VARCHAR2 (3);
    BEGIN
        gv_code_proc := 'F_REQUEST_DATA_IS_OLD';
        gv_code_ln := $$plsql_line;

        SELECT DECODE (COUNT (1), 0, 'NO', 'YES')
          INTO lv_cnt
          FROM alps_mml_pr_detail dtl
         WHERE     dtl.meter_id = p_meter_id
               AND dtl.sbl_quote_id = p_quote_id
               AND (SYSDATE - dtl.updated_dt) >
                   (SELECT lookup_num_value1
                      FROM alps_mml_lookup
                     WHERE     lookup_group = 'DATA_AGING_LIMIT'
                           AND lookup_code = 'DATA_TOO_OLD');

        /*   (SELECT sbl_quote_id
                                               FROM alps_mml_pr_header
                                               WHERE prnumber = p_prnumber
                                               AND revision  =  (SELECT MAX(revision)
                                                                        FROM alps_mml_pr_header
                                                                        WHERE prnumber = p_prnumber
                                                                        AND sbl_quote_id <>  p_quote_id
                                                                        AND active_flg = 'N') )
                                                                        */
        --
        RETURN (lv_cnt);
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
    END;

    FUNCTION f_vee_data_is_old (p_quote_id   IN VARCHAR2,
                                p_meter_id   IN VARCHAR2)
        RETURN VARCHAR2
    AS
        lv_cnt   VARCHAR2 (3);
    BEGIN
        gv_code_proc := 'F_VEE_DATA_IS_OLD';

        --If current Quote alredy existed in PR Header then it a PR revision else it a new PR
        gv_code_ln := $$plsql_line;

        SELECT DECODE (COUNT (1), 0, 'NO', 'YES')
          INTO lv_cnt
          FROM alps_mml_pr_detail dtl
         WHERE     dtl.meter_id = p_meter_id
               AND dtl.sbl_quote_id = p_quote_id
               -- AND status IN ('VEE_COMPLETE','OFFER_SUMMARY_COMPLETE', 'OFFER_SUMMARY_FAILED','SUBMITTED')
               AND (SYSDATE - last_vee_completed_dt) >
                   (SELECT lookup_num_value1
                      FROM alps_mml_lookup
                     WHERE     lookup_group = 'DATA_AGING_LIMIT'
                           AND lookup_code = 'VEE_DATA');

        --
        RETURN (lv_cnt);
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
    END;

    FUNCTION f_pr_new_revision (p_attach_id IN VARCHAR2)
        RETURN VARCHAR2
    AS
        lv_sbl_quote_id   alps_mml_stg.sbl_quote_id%TYPE;
        lv_prnumber       alps_mml_stg.prnumber%TYPE;
        lv_retval         VARCHAR2 (3);
    BEGIN
        gv_code_proc := 'F_PR_NEW_REVISION';

        -- get the pending MML PR and quote
        BEGIN
            gv_code_ln := $$plsql_line;

            SELECT DISTINCT prnumber, sbl_quote_id
              INTO lv_prnumber, lv_sbl_quote_id
              FROM alps_mml_stg
             WHERE     sbl_attach_id = p_attach_id
                   AND status = 'VALIDATION_PENDING';
        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
                RAISE;
        END;

        --If current Quote alredy existed in PR Header then it a PR revision else it a new PR
        gv_code_ln := $$plsql_line;

        SELECT DECODE (COUNT (1), 0, 'NO', 'YES') --  DECODE(MAX(revision),'1','NO','YES')
          INTO lv_retval
          FROM alps_mml_pr_header
         WHERE prnumber = lv_prnumber  --   AND sbl_quote_id = lv_sbl_quote_id
                                      AND active_flg = 'Y';

        --
        RETURN (lv_retval);
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
    END;

    FUNCTION f_lwsp_same_pr_chk (p_attach_id IN VARCHAR2)
        RETURN VARCHAR2
    AS
        lv_revcnt         VARCHAR2 (3);
        lv_sbl_quote_id   alps_mml_stg.sbl_quote_id%TYPE;
        lv_prnumber       alps_mml_stg.prnumber%TYPE;
    BEGIN
        gv_code_proc := 'F_LWSP_PR_MML_CHK';
        gv_code_ln := $$plsql_line;

        -- Check to see if same PR being re-use for new MML
        SELECT DECODE (COUNT (1), 0, 'NO', 'YES')
          INTO lv_revcnt
          FROM alps_mml_pr_header hdr
         WHERE     NVL (hdr.lwsp_flag, 'N') = 'Y'
               AND hdr.active_flg = 'Y'
               AND EXISTS
                       (SELECT 1
                          FROM alps_mml_stg stg
                         WHERE     stg.sbl_attach_id = p_attach_id
                               AND stg.status = 'VALIDATION_PENDING'
                               AND NVL (stg.lwsp_flag, 'N') = 'Y'
                               AND stg.prnumber = hdr.prnumber --      AND stg.sbl_quote_id = hdr.sbl_quote_id
                                                              );

        --
        RETURN (lv_revcnt);
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Function: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END;

    FUNCTION requests_exceed_threshold (p_attach_id IN VARCHAR2)
        RETURN VARCHAR2
    AS
        lv_return   VARCHAR2 (1);
    BEGIN
        gv_code_proc := 'REQUESTS_EXCEED_THRESHOLD';

        gv_code_ln := $$plsql_line;

        SELECT CASE
                   WHEN stg_count > threshold THEN 'Y'
                   WHEN stg_count <= threshold THEN 'N'
               END
          INTO lv_return
          FROM (SELECT lookup_num_value1     threshold
                  FROM alps_mml_lookup lkup
                 WHERE     lookup_group = 'SYSTEM_VALIDATION'
                       AND lookup_code = 'MAX_REQUESTED_ACCOUNTS'),
               (SELECT COUNT (1)     stg_count
                  FROM alps_mml_stg stg
                 WHERE     sbl_attach_id = p_attach_id
                       AND stg.status = 'VALIDATION_PENDING');

        RETURN (lv_return);
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Function: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END;

    PROCEDURE pre_cleanup (p_attach_id IN VARCHAR2)
    AS
        lv_sbl_quote_id   alps_mml_stg.sbl_quote_id%TYPE;
    BEGIN
        gv_code_proc := 'PRE_CLEANUP';
        --
        gv_code_ln := $$plsql_line;

        SELECT DISTINCT sbl_quote_id
          INTO lv_sbl_quote_id
          FROM alps_mml_stg
         WHERE sbl_attach_id = p_attach_id AND status = 'VALIDATION_PENDING';

        --
        gv_code_ln := $$plsql_line;

        DELETE FROM alps_mml_validation_exceptions
              WHERE sbl_quote_id = lv_sbl_quote_id;

        /*Express*/
        /*Delete express accounts/usage records*/
        FOR rec IN (SELECT sbl_quote_id
                      FROM alps_mml_stg
                     WHERE sbl_attach_id = p_attach_id AND ROWNUM < 2)
        LOOP
            DELETE FROM
                exp_quote_usage
                  WHERE uid_account IN
                            (SELECT uid_account
                               FROM exp_quote_acct
                              WHERE exp_quote_id = rec.sbl_quote_id);

            DELETE FROM exp_quote_acct
                  WHERE exp_quote_id = rec.sbl_quote_id;
        END LOOP;

        --
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END pre_cleanup;

    PROCEDURE setup_meter_type (p_attach_id IN VARCHAR2)
    AS
    BEGIN
        gv_code_proc := 'SETUP_METER_TYPE';
        --
        gv_code_ln := $$plsql_line;

        FOR c_data
            IN (SELECT dtl.market_code,
                       dtl.disco_code,
                       dtl.uid_alps_account,
                       DECODE (resp.idr_meter,
                               'Y', 'IDR',
                               'N', 'SCA',
                               NULL, 'UNKNOWN')    idr_meter
                  FROM alps_mml_pr_header       hdr,
                       alps_mml_pr_detail       dtl,
                       alps_account_repository  resp
                 WHERE     hdr.sbl_attach_id = p_attach_id
                       AND hdr.uid_mml_master = dtl.uid_mml_master
                       AND dtl.market_code = resp.market_code(+)
                       AND dtl.disco_code = resp.disco_code(+)
                       AND dtl.ldc_account = resp.ldc_account(+))
        LOOP
            UPDATE alps_mml_pr_detail
               SET meter_type = c_data.idr_meter,
                   idr_status =
                       DECODE (c_data.idr_meter,
                               'SCA', 'NOT_IDR',
                               idr_status)
             WHERE uid_alps_account = c_data.uid_alps_account;
        END LOOP;

        --
        FOR rec
            IN (SELECT lk.lookup_str_value2 meter_type, dtl.uid_alps_account
                  FROM alps_mml_pr_header  hdr,
                       alps_mml_pr_detail  dtl,
                       (SELECT lookup_code,
                               lookup_str_value2,
                               (SUBSTR (
                                    lookup_str_value1,
                                    INSTR (lookup_str_value1, ':', -1) + 1))
                                   strprefix,
                               (SUBSTR (lookup_str_value1,
                                        1,
                                        INSTR (lookup_str_value1, ':') - 1))
                                   strlen
                          FROM alps_mml_lookup
                         WHERE lookup_group = 'DISCO_METER_TYPE') lk
                 WHERE     hdr.sbl_attach_id = p_attach_id
                       AND dtl.disco_code = lk.lookup_code
                       AND SUBSTR (dtl.ldc_account, 1, lk.strlen) =
                           lk.strprefix
                       AND hdr.uid_mml_master = dtl.uid_mml_master)
        LOOP
            UPDATE alps_mml_pr_detail
               SET meter_type = rec.meter_type,
                   idr_status =
                       DECODE (rec.meter_type, 'SCA', 'NOT_IDR', idr_status)
             WHERE uid_alps_account = rec.uid_alps_account;
        END LOOP;

        --
        COMMIT;
    EXCEPTION
        WHEN OTHERS
        THEN
            --PROCESS_LOGGING(gv_log_source_id,  'Procedure: '||gv_code_proc||',  line: '||gv_code_ln ,NULL,NULL,NULL,NULL, SQLCODE, SUBSTR(SQLERRM,1,200),gv_log_stage,gv_log_source_typ );
            ROLLBACK;
            RAISE;
    END setup_meter_type;

    PROCEDURE mml_validations (p_attach_id    IN     VARCHAR2,
                               p_out_status      OUT VARCHAR2,
                               p_out_desc        OUT VARCHAR2)
    AS
        lv_lwsp_flag     alps_mml_pr_header.lwsp_flag%TYPE;
        lv_description   alps_mml_stg.description%TYPE;
        lv_customer_id   alps_mml_stg.customer_id%TYPE;
        verror           VARCHAR2 (2000);
    BEGIN
        /*Express*/
        FOR rec IN (SELECT NVL (stg.exp_flag, 'N')     express_flag
                      FROM alps_mml_stg stg
                     WHERE stg.sbl_attach_id = p_attach_id AND ROWNUM < 2)
        LOOP
            gv_express_flag := rec.express_flag;
        END LOOP;


        gv_code_proc := 'PROCESS_ALL_RULES';
        --
        gv_code_ln := $$plsql_line;

        SELECT COUNT (1)
          INTO gv_stg_cnt
          FROM alps_mml_stg
         WHERE sbl_attach_id = p_attach_id AND status = 'VALIDATION_PENDING';

        --
        IF gv_stg_cnt = 0
        THEN
            RAISE missing_accounts;
        ELSE
            -- Cleanup
            pre_cleanup (p_attach_id);

            --get the Quote_id
            gv_code_ln := $$plsql_line;

            SELECT DISTINCT 
                   customer_id,
                   sbl_quote_id, 
                   NVL (lwsp_flag, 'N'), 
                   UPPER (description)
              INTO lv_customer_id,
                   gv_log_source_id, 
                   lv_lwsp_flag, 
                   lv_description
              FROM alps_mml_stg
             WHERE     sbl_attach_id = p_attach_id
                   AND status = 'VALIDATION_PENDING';

            --
            DBMS_OUTPUT.put_line (
                   'lv_lwsp_flag ==> '
                || lv_lwsp_flag
                || ', lv_description ==> '
                || lv_description);

            --
            IF lv_description != 'PROXY'
            THEN -- skip system rules validation if description has the word IGNORE
                -- MML rules validation
                process_all_rules (p_attach_id, 'MML');

                -- Check Exception LOG and fail the Validation if any exception founded.
                gv_code_ln := $$plsql_line;

                IF mml_exception_count (p_attach_id) > 0
                THEN
                    RAISE failed_validation;
                END IF;

                --  SYSTEM rules validation
                gv_code_ln := $$plsql_line;

                IF requests_exceed_threshold (p_attach_id) = 'N'
                THEN
                    process_all_rules (p_attach_id, 'SYSTEM');
                ELSE
                    RAISE exceed_account_threshold;
                END IF;

                -- Check Exception LOG and fail the alidation if any exception founded.
                gv_code_ln := $$plsql_line;

                IF mml_exception_count (p_attach_id) > 0
                THEN
                    RAISE failed_validation;
                END IF;
            END IF;

            DBMS_OUTPUT.put_line ('validation OK begin setup account');
            -- For all Validate accounts, setup MML PR master and detail accounts
            gv_code_ln := $$plsql_line;

            --Insert Data into EXP_QUOTE_ACCT
            FOR rec
                IN (SELECT sbl_attach_id, sbl_quote_id
                      FROM alps_mml_stg
                     WHERE     sbl_attach_id = p_attach_id
                           AND NVL (exp_flag, 'N') = 'Y'
                           AND ROWNUM < 2)
            LOOP
                INSERT INTO exp_quote_acct (uid_account,
                                            exp_quote_id,
                                            ldc_account,
                                            market_code,
                                            disco_code,
                                            meter_type,
                                            uid_mml_account,
                                            status,
                                            description,
                                            retain_attrusage_flg)
                    SELECT alps_generic_seq.NEXTVAL,
                           sbl_quote_id,
                           CASE
                               WHEN PR_TRANS_TYPE <> 'Amendment - Add'
                               THEN
                                   ldc_account
                               ELSE
                                   CASE
                                       WHEN INSTR (ldc_account, '_') = 0
                                       THEN
                                           ldc_account
                                       ELSE
                                           SUBSTR (
                                               ldc_account,
                                               1,
                                               INSTR (ldc_account, '_') - 1)
                                   END
                           END    ldc_account,
                           market_code,
                           disco_code,
                           meter_type,
                           uid_mml_account,
                           status,
                           description,
                           'N'
                      FROM alps_mml_stg
                     WHERE     sbl_quote_id = rec.sbl_quote_id
                           AND sbl_attach_id = rec.sbl_attach_id;
            END LOOP;



            IF lv_lwsp_flag = 'Y'
            THEN
                DBMS_OUTPUT.put_line ('setup LWSP');
                setup_lwsp_accounts (p_attach_id);
            ELSE
                IF f_pr_new_revision (p_attach_id) = 'YES'
                THEN
                    DBMS_OUTPUT.put_line (
                        'setup Regular accoutn with revision');
                    -- This is a revision to a existing PR . Revise ALPS_MML_PR_DETAILS. Add /remove accounts based on newly submitted MML list
                    setup_mml_revise_accounts (p_attach_id);
                ELSE
                    DBMS_OUTPUT.put_line ('setup new Regular account');
                    --  PR initial SETUP  for LWSP and NON-LWSP
                    setup_mml_accounts (p_attach_id);

                    -- Process Off Ramp PR
                    process_offramp_pr (p_attach_id,
                                        p_out_status,
                                        p_out_desc);

                END IF;
            END IF;

            -- Update status for all accounts in STAGING table
            gv_code_ln := $$plsql_line;

            IF lv_description != 'PROXY'
            THEN
                set_staging_account (p_attach_id, 'VALIDATION_SUCCESS');
            ELSE
                set_staging_account (p_attach_id, 'VALIDATION_BYPASS');
            END IF;

            -- Determine LDC 's meter type
            setup_meter_type (p_attach_id);

            -- Added by Ann on 12/10/2014 to clean up LWSP override flags
            IF lv_lwsp_flag = 'N' AND NVL (gv_express_flag, 'N') <> 'Y'
            THEN
                --Remove LWSP Flag in ALPS
                UPDATE alps_account
                   SET lwsp_flag = 'N'
                 WHERE meter_id IN (SELECT meter_id
                                      FROM alps_mml_pr_detail
                                     WHERE sbl_quote_id = gv_log_source_id);

                FOR vacct
                    IN (SELECT b.uidaccount, b.accountid
                          FROM alps_mml_pr_detail     a,
                               account@TPPE           b,
                               acctoverridehist@TPPE  c
                         WHERE     a.meter_id = b.accountid
                               AND b.uidaccount = c.uidaccount
                               AND c.overridecode = 'FT_ACCOUNT'
                               AND a.sbl_quote_id = gv_log_source_id
                               AND a.meter_type = 'IDR')
                LOOP
                    DELETE FROM
                        lschannelcutdata@TPPE
                          WHERE uidchannelcut IN
                                    (SELECT uidchannelcut
                                       FROM lschannelcutheader@TPPE
                                      WHERE     channel = 3
                                            AND recorder = vacct.accountid);

                    DELETE FROM lschannelcutheader@TPPE
                          WHERE channel = 3 AND recorder = vacct.accountid;

                    DELETE FROM
                        acctoverridehist@TPPE
                          WHERE     uidaccount = vacct.uidaccount
                                AND overridecode = 'VEE_COMPLETE_OVRD';
                END LOOP;

                DELETE FROM
                    acctoverridehist@TPPE
                      WHERE     uidaccount IN
                                    (SELECT uidaccount
                                       FROM account@TPPE
                                      WHERE accountid IN
                                                (SELECT meter_id
                                                   FROM alps_mml_pr_detail
                                                  WHERE sbl_quote_id =
                                                        gv_log_source_id))
                            AND overridecode IN
                                    ('FT_ACCOUNT',
                                     'FT_FUEL_SOURCE',
                                     'FT_SEGMENT_CODE');

                COMMIT;
            ELSIF lv_lwsp_flag = 'Y'
            THEN
                --Set the FT Tags on LWSP Accounts, if any exists in PE
                BEGIN
                    FOR rec
                        IN (SELECT gv_log_source_id                sbl_quote_id,
                                   b.business_profile              ft_segment_code,
                                   b.heat_source                   ft_fuel_source,
                                   NVL (c.lookup_str_value1, 1)    ft_account
                              FROM v_alps_sbl_get_prdata  b,
                                   (SELECT *
                                      FROM alps_mml_lookup
                                     WHERE lookup_group =
                                           'ALPS_LWSP_FT_ACCOUNT') c
                             WHERE     b.channel_type_cd = c.lookup_code(+)
                                   AND b.quote_id = gv_log_source_id)
                    LOOP
                        FOR cpeinfo
                            IN (SELECT DISTINCT    market_code
                                                || '_'
                                                || disco_code
                                                || '_'
                                                || ldc_account                meter_id,
                                                t2.uidaccount,
                                                (SELECT COUNT (1)     COUNT
                                                   FROM acctoverridehist@TPPE
                                                  WHERE     overridecode IN
                                                                ('FT_ACCOUNT',
                                                                 'FT_FUEL_SOURCE',
                                                                 'FT_SEGMENT_CODE')
                                                        AND uidaccount =
                                                            t2.uidaccount)    ft_count
                                  FROM alps_mml_pr_detail t1, account@TPPE t2
                                 WHERE        t1.market_code
                                           || '_'
                                           || disco_code
                                           || '_'
                                           || ldc_account =
                                           t2.accountid
                                       AND t1.sbl_quote_id = rec.sbl_quote_id)
                        LOOP
                            --Update ALPS Account
                            UPDATE alps_account
                               SET lwsp_flag = 'Y'
                             WHERE     lwsp_flag <> 'Y'
                                   AND meter_id = cpeinfo.meter_id;


                            DBMS_OUTPUT.put_line (
                                   'ALPS Meter Updated as LWSP for Meter Id:'
                                || cpeinfo.meter_id);

                            IF     cpeinfo.uidaccount > 0
                               AND cpeinfo.ft_count < 3
                            THEN
                                --Delete from ACCTOVERRIDEHIST
                                DELETE FROM
                                    acctoverridehist@TPPE
                                      WHERE     overridecode IN
                                                    ('FT_ACCOUNT',
                                                     'FT_FUEL_SOURCE',
                                                     'FT_SEGMENT_CODE')
                                            AND uidaccount =
                                                cpeinfo.uidaccount;

                                DBMS_OUTPUT.put_line (
                                       'Delete Records Effected:'
                                    || SQL%ROWCOUNT);
                            END IF;

                            IF cpeinfo.ft_count < 3
                            THEN
                                INSERT INTO acctoverridehist@TPPE (
                                                uidaccount,
                                                overridecode,
                                                starttime,
                                                val,
                                                strval)
                                    (SELECT cpeinfo.uidaccount,
                                            'FT_ACCOUNT',
                                            TO_DATE ('01-JAN-1999',
                                                     'DD-MON-YYYY'),
                                            TO_NUMBER (rec.ft_account),
                                            NULL
                                       FROM DUAL
                                     UNION
                                     SELECT cpeinfo.uidaccount,
                                            'FT_FUEL_SOURCE',
                                            TO_DATE ('01-JAN-1999',
                                                     'DD-MON-YYYY'),
                                            NULL,
                                            rec.ft_fuel_source
                                       FROM DUAL
                                     UNION
                                     SELECT cpeinfo.uidaccount,
                                            'FT_SEGMENT_CODE',
                                            TO_DATE ('01-JAN-1999',
                                                     'DD-MON-YYYY'),
                                            NULL,
                                            rec.ft_segment_code
                                       FROM DUAL);
                            END IF;

                            DBMS_OUTPUT.put_line (
                                'Insert Records Effected:' || SQL%ROWCOUNT);
                        END LOOP;
                    END LOOP;

                    COMMIT;
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        DBMS_OUTPUT.put_line (SQLERRM);
                        verror :=
                               SQLERRM
                            || '-'
                            || DBMS_UTILITY.format_error_backtrace ();
                END;
            END IF;

            --Update All previous Versions to Inactive
            UPDATE alps_mml_pr_header
               SET active_flg = 'N'
             WHERE uid_mml_master IN
                       (SELECT DISTINCT a.uid_mml_master
                          FROM alps_mml_pr_header a, alps_mml_pr_header b
                         WHERE     a.prnumber = b.prnumber
                               AND b.sbl_quote_id = gv_log_source_id
                        MINUS
                        SELECT MAX (a.uid_mml_master)
                          FROM alps_mml_pr_header a, alps_mml_pr_header b
                         WHERE     a.prnumber = b.prnumber
                               AND b.sbl_quote_id = gv_log_source_id);

            IF gv_express_flag = 'Y'
            THEN
                /*Delete Data from EXP_QUOTE_ACCT*/
                DELETE FROM exp_quote_acct
                      WHERE exp_quote_id = gv_log_source_id;

                /*Update meter Type for the Meters that already exists*/
                FOR recDetails
                    IN (SELECT DISTINCT
                               a.meter_id, a.sbl_quote_id, B.METER_TYPE
                          FROM alps_mml_pr_detail a, alps_account b
                         WHERE     a.meter_id = b.meter_id(+)
                               AND a.sbl_quote_id = gv_log_source_id)
                LOOP
                    --update alps_account and set the correct customer id for Express deal
                    UPDATE alps_account
                       SET customer_id = recDetails.sbl_quote_id
                     WHERE meter_id = recDetails.meter_id;

                    UPDATE alps_mml_pr_detail
                       SET meter_type =
                               NVL (recDetails.METER_TYPE, meter_type)
                     WHERE meter_id = recDetails.meter_id;
                END LOOP;


                /*Insert Data into EXP_QUOTE_ACCT*/
                FOR rec
                    IN (SELECT sbl_attach_id, sbl_quote_id
                          FROM alps_mml_stg
                         WHERE     sbl_attach_id = p_attach_id
                               AND NVL (exp_flag, 'N') = 'Y'
                               AND ROWNUM < 2)
                LOOP
                    INSERT INTO exp_quote_acct (uid_account,
                                                exp_quote_id,
                                                ldc_account,
                                                market_code,
                                                disco_code,
                                                meter_type,
                                                uid_mml_account,
                                                status,
                                                description,
                                                retain_attrusage_flg)
                        SELECT alps_generic_seq.NEXTVAL,
                               sbl_quote_id,
                               ldc_account,
                               market_code,
                               disco_code,
                               meter_type,
                               uid_alps_account,
                               status,
                               description,
                               'N'
                          FROM alps_mml_pr_detail
                         WHERE sbl_quote_id = rec.sbl_quote_id;
                END LOOP;

                BEGIN
                    FOR rec
                        IN (SELECT *
                              FROM (  SELECT DISTINCT
                                             a.sbl_quote_id,
                                             a.status
                                                 header_status,
                                             COUNT (
                                                 DISTINCT
                                                     NVL (b.status, 'DUMMY'))
                                                 statuscount,
                                             MAX (NVL (b.status, 'DUMMY'))
                                                 status
                                        FROM alps_mml_pr_header a,
                                             alps_mml_pr_detail b
                                       WHERE     a.sbl_quote_id =
                                                 b.sbl_quote_id
                                             AND a.active_flg = 'Y'
                                             AND a.express_flag = 'Y'
                                             AND a.sbl_quote_id =
                                                 gv_log_source_id
                                             AND a.status <>
                                                 'READY_FOR_PRICING'
                                    GROUP BY a.sbl_quote_id, a.status)
                             WHERE     statuscount = 1
                                   AND status = 'EXPRESS_READY')
                    LOOP
                        p_out_status := NULL;
                        p_out_desc := NULL;

                        BEGIN
                            EXP_PKG.p_preparegetpricing (rec.sbl_quote_id,
                                                         p_out_status,
                                                         p_out_desc);
                        EXCEPTION
                            WHEN OTHERS
                            THEN
                                NULL;
                                p_out_status := 'ERROR';
                                p_out_desc := SQLERRM;
                        END;

                        IF p_out_status = 'SUCCESS'
                        THEN
                            UPDATE alps_mml_pr_header
                               SET status = 'READY_FOR_PRICING',
                                   DESCRIPTION = NULL
                             WHERE sbl_quote_id = rec.sbl_quote_id;
                        ELSE
                            UPDATE alps_mml_pr_header
                               SET status = 'PENDING',
                                   DESCRIPTION = p_out_desc
                             WHERE sbl_quote_id = rec.sbl_quote_id;
                        END IF;
                    END LOOP;
                EXCEPTION
                    WHEN OTHERS
                    THEN
                        NULL;
                        DBMS_OUTPUT.put_line (
                            'Error Updating the Header:' || SQLERRM);
                END;
            END IF;

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
                              FROM ALPS_ACCOUNT A, 
                                   ALPS_MML_PR_DETAIL B
                             WHERE     B.SBL_QUOTE_ID = gv_log_source_id
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
                    UPDATE ALPS_ACCOUNT
                       SET ADDRESS1 = REC.SRC_ADDRESS1,
                           ADDRESS2 = REC.SRC_ADDRESS2,
                           CITY = REC.SRC_CITY,
                           STATE = REC.SRC_STATE,
                           ZIP = REC.SRC_ZIP,
                           CUSTOMER_ID = lv_customer_id
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
                             WHERE     B.SBL_QUOTE_ID = gv_log_source_id
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

            --Sync CUSTOMER ID - 70455 
            BEGIN
                FOR REC IN (SELECT DISTINCT A.UID_ACCOUNT
                                  FROM ALPS_ACCOUNT A, 
                                       ALPS_MML_PR_DETAIL B
                                 WHERE     B.SBL_QUOTE_ID = gv_log_source_id
                                       AND A.METER_ID = B.METER_ID
                                       AND CUSTOMER_ID IS NULL
                             )
                LOOP
                    UPDATE ALPS_ACCOUNT
                       SET CUSTOMER_ID = lv_customer_id
                     WHERE UID_ACCOUNT = REC.UID_ACCOUNT;
                END LOOP;

                COMMIT;
            END;
            -- Pass back SUCCESS message to calling app.
            p_out_status := 'SUCCESS';
            p_out_desc := NULL;
        END IF;
    EXCEPTION
        WHEN loa_missing
        THEN
            SELECT boilertext1, 'ERROR'
              INTO p_out_desc, p_out_status
              FROM alps_error_msg_template
             WHERE msg_group = 'OTHER' AND msg_type = 'LOA_ERROR';

            --
            ROLLBACK;
        WHEN exceed_account_threshold
        THEN
            SELECT boilertext1, 'ERROR'
              INTO p_out_desc, p_out_status
              FROM alps_error_msg_template
             WHERE msg_group = 'OTHER' AND msg_type = 'LIMIT_ERROR';

            ROLLBACK;
        WHEN missing_accounts
        THEN
            -- Pass back ERROR message to calling app.
            SELECT boilertext1, 'ERROR'
              INTO p_out_desc, p_out_status
              FROM alps_error_msg_template
             WHERE msg_group = 'OTHER' AND msg_type = 'STG_ERROR';

            --
            ROLLBACK;
        WHEN failed_validation
        THEN
            --Update status in STAGING table
            process_mml_exceptions (p_attach_id);

            -- Pass back ERROR message to calling app.
            SELECT DISTINCT
                   DECODE (
                       rule_catg,
                       'MML', (SELECT boilertext1
                                 FROM alps_error_msg_template
                                WHERE     msg_group = 'OTHER'
                                      AND msg_type = 'MML_ERROR'),
                       (SELECT boilertext1
                          FROM alps_error_msg_template
                         WHERE     msg_group = 'OTHER'
                               AND msg_type = 'SYSTEM_ERROR')),
                   'ERROR'
              INTO p_out_desc, p_out_status
              FROM alps_mml_validation_exceptions
             WHERE sbl_attach_id = p_attach_id;

            --
            ROLLBACK;
        WHEN OTHERS
        THEN
            SELECT boilertext2, 'ERROR'
              INTO p_out_desc, p_out_status
              FROM alps_error_msg_template
             WHERE msg_group = 'OTHER' AND msg_type = 'ORA_ERROR';

            -- Set up the error message to be return to calling App.
            p_out_desc :=
                   p_out_desc
                || CHR (10)
                || SUBSTR (SQLERRM, 1, 1000)
                || CHR (10)
                || DBMS_UTILITY.format_error_backtrace ();

            -- log the error in ALPS
            process_logging (
                gv_log_source_id,
                'Procedure: ' || gv_code_proc || ',  line: ' || gv_code_ln,
                NULL,
                NULL,
                NULL,
                NULL,
                SQLCODE,
                SUBSTR (SQLERRM, 1, 200),
                gv_log_stage,
                gv_log_source_typ);
            ROLLBACK;
    END mml_validations;
END alps_mml_pkg;
////
