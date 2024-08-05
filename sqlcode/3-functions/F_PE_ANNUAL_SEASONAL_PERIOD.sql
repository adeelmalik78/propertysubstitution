create or replace FUNCTION ${schema.name}.F_PE_ANNUAL_SEASONAL_PERIOD(p_market IN VARCHAR2, p_indate  IN DATE , p_date_type IN VARCHAR2)
RETURN VARCHAR2 AS
/**************************************************************/
/*    Purpose : Get the annual tag Start/End Dates for market with seasoal tag dates
/**************************************************************/
    vReturn  DATE;
    v_annual_tag_date   DATE;

BEGIN
       DBMS_OUTPUT.PUT_LINE( 'p_indate ==> ' ||p_indate);
              DBMS_OUTPUT.PUT_LINE( 'p_market ==> ' ||p_market);

   IF p_market = 'MISO'
   THEN
       -- Fetch the CPY tag date
       SELECT TO_DATE(lookup_str_value1||TO_CHAR(p_indate,'YYYY'),'MMDDYYYY')
       INTO v_annual_tag_date
      --  from ALPS_MML_LOOKUP@tpint
       from ALPS_MML_LOOKUP${PE}
       WHERE lookup_group = 'MARKET_ANNUAL_TAG'
       AND lookup_code = p_market;
       --

       IF p_indate < v_annual_tag_date
       THEN
         SELECT ADD_MONTHS(v_annual_tag_date, -12)
         INTO v_annual_tag_date
         FROM dual;
        --------
		-- BUG #69730 Resolve annual seasonal period.
		--------
       ELSIF p_indate >=  ADD_MONTHS(v_annual_tag_date, 12)
        THEN
            SELECT ADD_MONTHS(v_annual_tag_date, 12)
            INTO v_annual_tag_date
         FROM dual;
        ----------
       ELSIF p_indate >= v_annual_tag_date
       THEN
         SELECT v_annual_tag_date
         INTO v_annual_tag_date
         FROM dual;
       END IF;
       --

       IF p_date_type = 'STOP'
       THEN
         v_annual_tag_date := ADD_MONTHS (v_annual_tag_date, 12) - (1 / (24 * 60 * 60));
       END IF;
       --
       RETURN TO_CHAR(v_annual_tag_date,'YYYY-MM-DD HH24:MI:SS');

   END IF;
EXCEPTION
 when others then
    RETURN TO_CHAR(p_indate,'YYYY-MM-DD HH24:MI:SS');

END;
/
