create or replace procedure CAMP_RTN_GET_CLIENT is
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
    AP_PUBLIC.CORE_LOG_PKG.pInit('AP_CRM', 'CAMP_RTN_GET_CLIENT');

		pTruncate('gtt_camp_pts_client');
    AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_camp_pts_client');
		insert /*+ APPEND */ into gtt_camp_pts_client
		WITH prep as
		(
			 select /*+ MATERIALIZE */ fac.skp_client, FAC.SKP_CREDIT_CASE, FAC.NAME_FIRST, FAC.NAME_LAST, FAC.NAME_MOTHER, FAC.DATE_BIRTH
			 fROM OWNER_DWH.F_APPLICATION_CLIENT_TT FAC 
			 where FAC.SKP_CLIENT_ROLE = '11' and fac.skp_credit_case in (select skp_credit_case from camp_rtn_contracts)
		)
		SELECT /*+ FULL(FAC) USE_HASH(FAC DC FABT) */
				 DC.SKP_CLIENT, DC.ID_CUID, null text_contract_number, FAC.SKP_CREDIT_CASE, FAC.NAME_FIRST, FAC.NAME_LAST, 
				 FAC.NAME_MOTHER, FAC.DATE_BIRTH, DC.NAME_BIRTH_PLACE, null
		fROM prep FAC
		LEFT JOIN OWNER_DWH.DC_CLIENT DC ON DC.SKP_CLIENT=FAC.SKP_CLIENT;
		--LEFT JOIN OWNER_DWH.F_APPLICATION_BASE_TT FABT ON FAC.SKP_CREDIT_CASE = FABT.SKP_CREDIT_CASE;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit; 
		pstats('gtt_camp_pts_client');

		pTruncate('gtt_camp_pts_ktp');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_camp_pts_ktp');
		insert /*+ APPEND */ into gtt_camp_pts_ktp
		SELECT /*+ PARALLEL(4) */ FCD.SKP_CLIENT, FCD.TEXT_DOCUMENT_NUMBER ID_KTP, FCD.DTIME_VALID_TO EXPIRY_DATE_KTP
		FROM OWNER_DWH.F_CLIENT_DOCUMENT_AD FCD
		WHERE SKP_CLIENT_DOCUMENT_TYPE=9 AND CODE_STATUS='a' and fcd.skp_client in (select skp_client from gtt_camp_pts_client);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit; 
		pstats('gtt_camp_pts_ktp');

		pTruncate('gtt_camp_pts_address');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_camp_pts_address');
		insert /*+ APPEND */ into gtt_camp_pts_address
		SELECT /*+ PARALLEL(4) USE_HASH(DCC ADT TAD) */ DCC.TEXT_CONTRACT_NUMBER, TAD.SKP_CREDIT_CASE,
		CASE WHEN UPPER(TAD.NAME_STREET) LIKE '%JL%'
		THEN TAD.NAME_STREET ELSE 'Jl '
		||TAD.NAME_STREET END
		||' no. '
		||CASE WHEN TAD.TEXT_STREET_NUMBER = '0' THEN TAD.TEXT_FLOOR_NUMBER
		ELSE TAD.TEXT_STREET_NUMBER END
		|| CASE WHEN TAD.TEXT_BLOCK_NUMBER <> '0' THEN ', RT'||TAD.TEXT_BLOCK_NUMBER ELSE '' END
		|| CASE WHEN TAD.TEXT_BUILDING_NUMBER <> '0' THEN ', RW'||TAD.TEXT_BUILDING_NUMBER ELSE '' END
		||', '
		||TAD.NAME_TOWN
		||', '
		||TAD.NAME_SUBDISTRICT
		||' - '
		||TAD.NAME_ZIP FULL_ADDRESS,TAD.NAME_TOWN,TAD.NAME_SUBDISTRICT,TAD.NAME_ZIP,TAD.NAME_DISTRICT
		FROM OWNER_DWH.F_CREDIT_CASE_REQUEST_ADDR_TT TAD
		JOIN OWNER_DWH.CL_ADDRESS_TYPE ADT ON TAD.SKP_ADDRESS_TYPE = ADT.SKP_ADDRESS_TYPE
		JOIN OWNER_DWH.DC_CREDIT_CASE DCC ON DCC.SKP_CREDIT_CASE = TAD.SKP_CREDIT_CASE
		WHERE ADT.CODE_ADDRESS_TYPE IN ('CONTACT') and tad.skp_credit_case in (select nvl(skp_credit_case,-999999) from gtt_camp_pts_client);
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit; 
		pstats('gtt_camp_pts_address');

		pTruncate('gtt_camp_pts_contact');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_camp_pts_contact');
		insert /*+ APPEND */ into gtt_camp_pts_contact
		SELECT /*+ PARALLEL(4) */ CC.SKP_CREDIT_CASE,
		MAX(CASE WHEN CT.CODE_CONTACT_TYPE = 'PRIMARY_MOBILE' THEN CC.TEXT_CONTACT ELSE NULL END) AS TEXT_MOBILE_PHONE,
		MAX(CASE WHEN CT.CODE_CONTACT_TYPE = 'SECONDARY_MOBILE' THEN CC.TEXT_CONTACT ELSE NULL END) AS TEXT_MOBILE2_PHONE,
		MAX(CASE WHEN CT.CODE_CONTACT_TYPE = 'PRIMARY_EMAIL' THEN CC.TEXT_CONTACT ELSE NULL END) AS EMAIL
		FROM OWNER_DWH.F_CREDIT_CASE_REQUEST_CONT_TT CC
		JOIN OWNER_DWH.CL_CONTACT_TYPE CT ON CC.SKP_CONTACT_TYPE = CT.SKP_CONTACT_TYPE
		WHERE CT.CODE_STATUS= 'a' AND CC.SKP_CONTACT_RELATION_TYPE = '4'
		and skp_credit_case in ( select skp_credit_case from gtt_camp_pts_client )
		GROUP BY CC.SKP_CREDIT_CASE;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit; 
		pstats('gtt_camp_pts_contact');

		pTruncate('camp_rtn_client_identity');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into client identity data');
		insert into camp_rtn_client_identity 
		with cli as
		(
		 select /*+ MATERIALIZE */ cln.*, dcl.name_gender from gtt_camp_pts_client cln
		 left join owner_Dwh.dc_client dcl on cln.skp_client = dcl.skp_client
		),
		cz as
		(
		   select /*+ MATERIALIZE */ contract_number, description, created_by, date_end from ap_bicc.stg_oa_activity_report
			 where (activity_id like 'CUST_%' and activity_id not in ('CUST_LOGIN','CUST_LOGOUT'))
			 and (contract_number, ID) in
			 (
			 		select contract_number, max(ID)ID_COL from ap_bicc.stg_oa_activity_report
					where contract_number in (select text_contract_number from camp_rtn_contracts)
						and (activity_id like 'CUST_%' and activity_id not in ('CUST_LOGIN','CUST_LOGOUT'))
					group by contract_number	
			 )
		)
		select bs.skp_client, bs.skp_credit_case, cli.id_cuid, cli.name_first, cli.name_last, cli.name_gender, cli.name_mother, cli.name_birth_place, 
				 cli.date_birth, ktp.id_ktp, ktp.expiry_date_ktp, ard.full_address, ard.name_town, ard.name_district, ard.name_subdistrict, 
				 ard.name_zip, con.text_mobile_phone mobile1, con.text_mobile2_phone mobile2, null mobile3, null mobile4, con.email, cz.description, cz.date_end
		from camp_rtn_contracts bs
		left join cli on bs.skp_client = cli.skp_client
		left join cz on bs.text_contract_number = cz.contract_number
		left join gtt_camp_pts_ktp ktp on cli.skp_client = ktp.skp_client
		left join gtt_camp_pts_address ard on cli.skp_credit_case = ard.skp_credit_case
		left join gtt_camp_pts_contact con on cli.skp_credit_case = con.skp_credit_case;
		AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit; 
		pstats('camp_rtn_client_identity');
		AP_PUBLIC.CORE_LOG_PKG.pFinish;
end;
/

