CREATE OR REPLACE PROCEDURE CAMP_RTN_GEN_CALL_LIST IS
  PROCEDURE pStats( acTable VARCHAR2, anPerc NUMBER DEFAULT 0.01) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Stat:'||upper(acTable) );
    DBMS_STATS.Gather_Table_Stats( OwnName => 'AP_CRM', TabName => upper(acTable),Estimate_Percent => anPerc );
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
  END ;
  PROCEDURE pTruncate( acTable VARCHAR2) IS
  BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pStart( 'Trunc:'||upper(acTable) );
    EXECUTE IMMEDIATE 'TRUNCATE TABLE AP_CRM.'||upper(acTable) ;
    AP_PUBLIC.CORE_LOG_PKG.pEnd ;
  END ;

BEGIN
    AP_PUBLIC.CORE_LOG_PKG.pInit('AP_CRM', 'CAMP_RTN_GEN_CALL_LIST'); 
		
		ptruncate('GTT_CAMP_PTS_MD');
		AP_PUBLIC.CORE_LOG_PKG.PSTART('Retrieve MyDream');
		INSERT /*+APPEND*/ INTO GTT_CAMP_PTS_MD
		SELECT CUID, LOAN_PURPOSE_ID, LOAN_PURPOSE_DESC
		FROM AP_BICC.STGV_MOB_MD_CUSTOMER
		 WHERE CUID IN 
		 (
					 SELECT NVL(TO_NUMBER(CUID), -999999) FROM camp_rtn_contracts
		 ) and status_active = 1;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		COMMIT;
		pstats('GTT_CAMP_PTS_MD');
		
/*		if (trunc(sysdate) < to_Date('08/13/2018','mm/dd/yyyy')) then 
				AP_PUBLIC.CORE_LOG_PKG.PSTART('Delete call list for today if any.');
				DELETE FROM AP_CRM.FINAL_RETENTION_CALL_LIST_C1 WHERE DTIME_DATA_GENERATED = TRUNC(SYSDATE);
				AP_PUBLIC.CORE_LOG_PKG.pEnd;
				COMMIT;
				pstats('FINAL_RETENTION_CALL_LIST');

				AP_PUBLIC.CORE_LOG_PKG.PSTART('Insert final call list');
				insert into FINAL_RETENTION_CALL_LIST
				with con as
				(
						SELECT * FROM camp_rtn_contracts where nvl(days_approved,1) >= 2 and nvl(days_approved, 1) < 14 and contract_type = 'Paperless'
						union 
						SELECT * FROM camp_rtn_contracts where nvl(days_approved,1) >= 3 and nvl(days_approved, 1) < 14 and contract_type = 'Regular'
						union 
						SELECT * FROM camp_rtn_contracts where nvl(days_approved, 1) < 14 and contract_type = 'Mobile App'
				)
				select trunc(sysdate)dtime_data_generated, cli.id_cuid, con.text_contract_number contract_id, 
							 case when lower(cli.name_gender) = 'male' then 'Bpk. ' || cli.name_first
										when lower(cli.name_gender) = 'female' then 'Ibu. ' || cli.name_first
							 end name_first, cli.name_last, 
							 case when con.alto_dtime_created is not null then con.alto_credit_amount else coalesce(con.credit_amount, con.offr_max_instalment) end max_credit_amount,
							 case when con.alto_dtime_created is not null then con.alto_max_instalment else coalesce(con.instalment, con.offr_max_instalment) end  max_installment,
							 cli.name_mother mother_maiden_name,
							 cli.name_birth_place place_of_birth,
							 cli.date_birth,
							 cli.id_ktp,
							 cli.expiry_date_ktp,
							 cli.mobile1,
							 cli.mobile2,
							 cli.email email_address,
							 cli.full_address,
							 cli.name_town,
							 cli.name_subdistrict,
							 cli.name_zip code_zip,
							 cli.name_district,
							 comm.text_note info1,
							 case when comm.dt_cancel_request is not null then con.contract_type || ' - Request Cancel' 
										when comm.date_promise is not null then con.contract_type || ' - Promise Date = ' || to_char(comm.date_promise,'dd-MON-yyyy')
										when con.contract_type = 'Mobile App' and skp_credit_substatus in (15, 9) then con.contract_type || ' - Alternate Offer' || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
										when con.contract_type = 'Mobile App' and con.credit_status in ('In Preprocess','In Process') then con.contract_type || ' - Follow Up 1st BOD' || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
							 else con.contract_type || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
							 end info2,
							 case when trunc(con.dtime_approval) <> to_date('01/01/3000','mm/dd/yyyy') then trunc(con.dtime_approval) + 14 end cancel_date,
							 case when trunc(con.dtime_approval) <> to_date('01/01/3000','mm/dd/yyyy') then trunc(con.dtime_approval) end date_approved,
							 case when tm_zone.code_timezone = 'DFT' then 'WIB' else tm_zone.code_timezone end tzone
				from con
				left join camp_rtn_last_comm comm on con.skp_credit_case = comm.skp_credit_case
				left join camp_rtn_client_identity cli on con.skp_client = cli.skp_client
				left join GTT_CAMP_PTS_MD md on cli.id_cuid = md.cuid
				left join (select TEXT_CONTRACT_NUMBER ,CODE_TIMEZONE from AP_BICC.F_CONTRACT_TIMEZONE_AD) Tm_zone on Tm_zone.TEXT_CONTRACT_NUMBER= con.text_contract_number
				where 1=1 
					and nvl(comm.action,'Call') = 'Call';
        AP_PUBLIC.CORE_LOG_PKG.pEnd;  
        commit;
        pstats('FINAL_RETENTION_CALL_LIST_C1');					
		else
        AP_PUBLIC.CORE_LOG_PKG.PSTART('Delete call list for today if any.');
        DELETE FROM AP_CRM.FINAL_RETENTION_CALL_LIST WHERE DTIME_DATA_GENERATED = TRUNC(SYSDATE);
        AP_PUBLIC.CORE_LOG_PKG.pEnd;
        COMMIT;
        pstats('FINAL_RETENTION_CALL_LIST');
				
        AP_PUBLIC.CORE_LOG_PKG.PSTART('Insert final call list');			
			  insert into FINAL_RETENTION_CALL_LIST
        with con as
        (
            SELECT * FROM camp_rtn_contracts where nvl(days_approved,1) >= 2 and nvl(days_approved, 1) < 14 and contract_type = 'Paperless'
            union 
            SELECT * FROM camp_rtn_contracts where nvl(days_approved,1) >= 3 and nvl(days_approved, 1) < 14 and contract_type = 'Regular'
            union 
            SELECT * FROM camp_rtn_contracts where nvl(days_approved, 1) < 14 and contract_type = 'Mobile App'
        )
        select trunc(sysdate)dtime_data_generated, cli.id_cuid, con.text_contract_number contract_id, 
               case when lower(cli.name_gender) = 'male' then 'Bpk. ' || cli.name_first
                    when lower(cli.name_gender) = 'female' then 'Ibu. ' || cli.name_first
               end name_first, cli.name_last, 
               case when con.alto_dtime_created is not null then con.alto_credit_amount else coalesce(con.credit_amount, con.offr_max_instalment) end max_credit_amount,
               case when con.alto_dtime_created is not null then con.alto_max_instalment else coalesce(con.instalment, con.offr_max_instalment) end  max_installment,
               cli.name_mother mother_maiden_name,
               cli.name_birth_place place_of_birth,
               cli.date_birth,
               cli.id_ktp,
               cli.expiry_date_ktp,
               cli.mobile1,
               cli.mobile2,
               cli.email email_address,
               cli.full_address,
               cli.name_town,
               cli.name_subdistrict,
               cli.name_zip code_zip,
               cli.name_district,
               comm.text_note info1,
               case when comm.dt_cancel_request is not null then con.contract_type || ' - Request Cancel' 
                    when comm.date_promise is not null then con.contract_type || ' - Promise Date = ' || to_char(comm.date_promise,'dd-MON-yyyy')
                    when con.contract_type = 'Mobile App' and skp_credit_substatus in (15, 9) then con.contract_type || ' - Alternate Offer' || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
                    when con.contract_type = 'Mobile App' and con.credit_status in ('In Preprocess','In Process') then con.contract_type || ' - Follow Up 1st BOD' || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
               else con.contract_type || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
               end info2,
               case when trunc(con.dtime_approval) <> to_date('01/01/3000','mm/dd/yyyy') then to_char(trunc(con.dtime_approval) + 14,'mm/dd/yyyy') end cancel_date,
               case when trunc(con.dtime_approval) <> to_date('01/01/3000','mm/dd/yyyy') then to_char(trunc(con.dtime_approval),'mm/dd/yyyy') end date_approved,
               case when tm_zone.code_timezone = 'DFT' then 'WIB' else tm_zone.code_timezone end tzone
        from con
        left join camp_rtn_last_comm comm on con.skp_credit_case = comm.skp_credit_case
        left join camp_rtn_client_identity cli on con.skp_client = cli.skp_client
        left join GTT_CAMP_PTS_MD md on cli.id_cuid = md.cuid
        left join (select TEXT_CONTRACT_NUMBER ,CODE_TIMEZONE from AP_BICC.F_CONTRACT_TIMEZONE_AD) Tm_zone on Tm_zone.TEXT_CONTRACT_NUMBER= con.text_contract_number
        where 1=1 
          and nvl(comm.action,'Call') = 'Call';
				AP_PUBLIC.CORE_LOG_PKG.pEnd;	
				commit;
				pstats('FINAL_RETENTION_CALL_LIST');
		end if;*/
		
		ptruncate('camp_rtn_final_call_list');
		AP_PUBLIC.CORE_LOG_PKG.PSTART('Insert final call list to camp_rtn_final_call_list');
		insert into camp_rtn_final_call_list
    with con as
		(
				/* Approved MobileApp contracts between D+1 to D+12 */
				SELECT * FROM camp_rtn_contracts where contract_type = 'Mobile App' and credit_status = 'Approved' and nvl(days_approved, 1) < 14
				union
				/* MobileApp Alternate offers, date created between D+1 to D+12 */
				select * from camp_rtn_contracts where contract_type = 'Mobile App' and skp_credit_substatus in (9, 15) and (trunc(sysdate) - trunc(dtime_app_created)) < 14
				union
				/* MobileApp 1stBOD data, date created between D+1 to D+12 */
				select * from camp_rtn_contracts where contract_type = 'Mobile App' and skp_credit_substatus not in (6, 9, 15) and credit_status <> 'Approved' and (trunc(sysdate) - trunc(dtime_app_created)) < 14
				union
				/* Paperless 1stBOD data, date created between D+2 to D+12 */
				select * from camp_rtn_contracts where contract_type = 'Paperless' and skp_credit_substatus not in (9, 15) and credit_status <> 'Approved' and (trunc(sysdate) - trunc(dtime_app_created)) between 2 and 13
				union
				/* Paperless Alternate offers, date created between D+1 to D+12 */
				select * from camp_rtn_contracts where contract_type = 'Paperless' and skp_credit_substatus in (6, 9, 15) and (trunc(sysdate) - trunc(dtime_app_created)) < 14
				union
				/* Approved Paperless contracts between D+1 to D+12 */
				select * from camp_rtn_contracts where contract_type = 'Paperless' and credit_status = 'Approved' and nvl(days_approved,1) >= 2 and nvl(days_approved, 1) < 14    
				union 
				/* Approved Regular contracts between D+1 to D+12 */
			  SELECT * FROM camp_rtn_contracts where contract_type = 'Regular' and credit_status = 'Approved' and nvl(days_approved,1) >= 3 and nvl(days_approved, 1) < 14
		)
		select distinct 
		       --case when comm.dt_cancel_request is not null then 'Cancel' else 'Non-Cancel' end pts_call_source,
					 'MPF_PTS' pts_call_source,
           cli.id_cuid CUID,
           bs.text_contract_number contract_id,
           case when cli.name_gender = 'Male' then 'Bpk. ' ||cli.name_first || ' ' || cli.name_last
                when cli.name_gender = 'Female' then 'Ibu. ' ||cli.name_first || ' ' || cli.name_last
           end pts_cust_name,
           comm.text_note pts_liscom,
           comm.date_promise pts_promise_date,
           cli.last_cz_page_date pts_clz_visit,
           case when trunc(bs.dtime_approval) <> to_date('01/01/3000','mm/dd/yyyy') then trunc(bs.dtime_approval) + 14 end pts_auto_cancel_date,
           case when bs.alto_dtime_created is not null then bs.alto_credit_amount else coalesce(bs.credit_amount, bs.offr_max_instalment) end pts_max_credit_amount,
           case when bs.alto_max_instalment is not null then bs.alto_max_instalment else coalesce(bs.offr_min_instalment, bs.instalment) end pts_min_instalment,
           bs.offr_max_tenor pts_max_tenor,
           initcap(pup.name_loan_purpose) pts_purpose,
           cli.mobile1 pts_mobile1,
           cli.mobile2 pts_mobile2,
           cli.mobile3 pts_mobile3,
           cli.mobile4 pts_mobile4,
           case when comm.date_promise is not null then bs.contract_type
                when bs.contract_type = 'Mobile App' and skp_credit_substatus in (15, 9) then bs.contract_type || ' - Alternate Offer' || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
                when bs.contract_type = 'Mobile App' and bs.credit_status in ('In Preprocess','In Process') then bs.contract_type || ' - Follow Up 1st BOD' || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
           else bs.contract_type || case when md.cuid is not null then ', MyDream = ' || md.loan_purpose_desc end
           end pts_info1,
           '-' pts_info2,
           '-' pts_info3,
           '-' pts_info4,
           '-' pts_info5,
           '-' pts_info6,
           '-' pts_info7,
           '-' pts_info8,
           '-' pts_info9,
           '-' pts_info10,
           case when tm_zone.code_timezone = 'DFT' then 'WIB' else tm_zone.code_timezone end tzone,
           'FF PTS' campaign_type,
           row_number() over (order by bs.dtime_app_created asc) nums,
					 trunc(sysdate)
    from con bs
    left join owner_Dwh.Dc_Contract dcc on bs.skp_credit_case = dcc.skp_credit_case
    left join ap_bicc.clt_loan_purpose pup on trim(dcc.text_loan_purpose) = pup.code_loan_purpose
    left join camp_rtn_client_identity cli on bs.skp_client = cli.skp_client
    left join camp_rtn_last_comm comm on bs.skp_credit_case = comm.skp_credit_case
    left join GTT_CAMP_PTS_MD md on cli.id_cuid = md.cuid
    left join (select TEXT_CONTRACT_NUMBER ,CODE_TIMEZONE from AP_BICC.F_CONTRACT_TIMEZONE_AD) Tm_zone on Tm_zone.TEXT_CONTRACT_NUMBER = bs.text_contract_number;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;	
		commit;
		pstats('camp_rtn_final_call_list');
		AP_PUBLIC.CORE_LOG_PKG.pFinish;
end; 
