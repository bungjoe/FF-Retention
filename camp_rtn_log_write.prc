create or replace procedure camp_rtn_log_write is
      PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
      BEGIN
          AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
          DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
          AP_PUBLIC.CORE_LOG_PKG.pEnd;
      END;
      PROCEDURE pTruncate( acTable VARCHAR2) IS
      BEGIN
          AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
          EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
          AP_PUBLIC.CORE_LOG_PKG.pEnd ;
      END;
begin
      AP_PUBLIC.CORE_LOG_PKG.pInit( 'AP_CRM', 'CAMP_RTN_LOG_WRITE');

			AP_PUBLIC.CORE_LOG_PKG.pStart('Insert log_camp_rtn_contracts');
			insert /*+ APPEND */ into ap_crm.log_camp_rtn_contracts
			select trunc(sysdate)log_Date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from camp_rtn_contracts t;
			AP_PUBLIC.CORE_LOG_PKG.pEnd;
			commit;
			pstats('log_camp_rtn_contracts');

      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert log_camp_rtn_last_comm');
			insert /*+ APPEND */ into log_camp_rtn_last_comm
			select trunc(sysdate)log_Date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from camp_rtn_last_comm t;
			AP_PUBLIC.CORE_LOG_PKG.pEnd;
			commit;
			pstats('log_camp_rtn_last_comm');

      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert log_camp_rtn_client_identity');
			insert /*+ APPEND */ into log_camp_rtn_client_identity
			select trunc(sysdate)log_Date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from camp_rtn_client_identity t;
			AP_PUBLIC.CORE_LOG_PKG.pEnd;
			commit;
			pstats('log_camp_rtn_client_identity');

      AP_PUBLIC.CORE_LOG_PKG.pStart('Insert log_camp_rtn_final_call_list');
			insert /*+ APPEND */ into log_camp_rtn_final_call_list
			select trunc(sysdate)log_Date, to_char(sysdate,'hh24:mi:ss')time_inserted, t.* from camp_rtn_final_call_list t;
			commit;
			AP_PUBLIC.CORE_LOG_PKG.pEnd;
			commit;
			pstats('log_camp_rtn_final_call_list');
	    AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

