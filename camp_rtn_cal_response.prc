CREATE OR REPLACE PROCEDURE CAMP_RTN_CAL_RESPONSE IS
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
	  AP_PUBLIC.CORE_LOG_PKG.pInit('AP_CRM', 'CAMP_RTN_CAL_RESPONSE');  
		
    ptruncate('gtt_cmp_rtn_02_commlist');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_cmp_rtn_02_commlist');
    insert /*+ APPEND */ into gtt_cmp_rtn_02_commlist
    select /*+ PARALLEL(5) USE_HASH (fcr ccl ccls cct ccs css csf cst crt) */ 
           fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
           fcr.skp_communication_channel, fcr.code_channel, ccls.name_communication_channel,
           fcr.skp_communication_type, fcr.code_type_code, cct.NAME_COMMUNICATION_TYPE,
           fcr.skp_communication_subtype, fcr.code_subtype, ccs.NAME_COMMUNICATION_SUBTYPE,
           fcr.skp_comm_subtype_specif, css.CODE_COMM_SUBTYPE_SPECIF, css.NAME_COMM_SUBTYPE_SPECIF,
           fcr.skp_comm_subtype_sub_specif, csf.CODE_COMM_SUBTYPE_SUB_SPECIF, csf.NAME_COMM_SUBTYPE_SUB_SPECIF,
           fcr.skp_communication_status, fcr.code_status, cst.name_communication_status,
           fcr.skp_communication_result_type, fcr.code_result_type, crt.NAME_COMMUNICATION_RESULT_TYPE,
           fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
    FROM ap_crm.camp_comm_rec_ob fcr
    inner join camp_cfg_comm_list ccl on ccl.name_campaign in ('Retention','FF General') /* and nvl(ccl.logic_group,'-') in ('Complaint','Exclusion') */ and ccl.active = 'Y'
       and nvl(fcr.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(fcr.skp_communication_channel,-1) else ccl.skp_communication_channel end
       and nvl(fcr.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(fcr.skp_communication_type,-1) else ccl.skp_communication_type end
       and nvl(fcr.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(fcr.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
       and nvl(fcr.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(fcr.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
       and nvl(fcr.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(fcr.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
       and nvl(fcr.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(fcr.skp_communication_status,-1) else ccl.skp_communication_status end
       and nvl(fcr.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(fcr.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
    left join owner_dwh.cl_communication_channel ccls on fcr.skp_communication_channel = ccls.skp_communication_channel
    left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
    left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
    left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
    left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
    left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
    left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
    where fcr.date_call >= trunc(sysdate-30) and fcr.skp_credit_case in (select skp_credit_case from camp_rtn_contracts)
    union 
    select /*+ PARALLEL(5) USE_HASH (fcr ccl ccls cct ccs css csf cst crt) */
           fcr.date_call, fcr.dtime_inserted, fcr.skf_communication_record, fcr.skp_client, fcr.skp_credit_case,
           fcr.skp_communication_channel, fcr.code_channel, ccls.name_communication_channel,
           fcr.skp_communication_type, fcr.code_type_code, cct.NAME_COMMUNICATION_TYPE,
           fcr.skp_communication_subtype, fcr.code_subtype, ccs.NAME_COMMUNICATION_SUBTYPE,
           fcr.skp_comm_subtype_specif, css.CODE_COMM_SUBTYPE_SPECIF, css.NAME_COMM_SUBTYPE_SPECIF,
           fcr.skp_comm_subtype_sub_specif, csf.CODE_COMM_SUBTYPE_SUB_SPECIF, csf.NAME_COMM_SUBTYPE_SUB_SPECIF,
           fcr.skp_communication_status, fcr.code_status, cst.name_communication_status,
           fcr.skp_communication_result_type, fcr.code_result_type, crt.NAME_COMMUNICATION_RESULT_TYPE,
           fcr.text_note, fcr.text_contact, fcr.employee_number, fcr.common_name
    FROM ap_crm.camp_comm_rec_ib fcr
    inner join camp_cfg_comm_list ccl on ccl.name_campaign in ('Retention','FF General') /* and nvl(ccl.logic_group,'-') in ('Complaint','Exclusion') */ and ccl.active = 'Y'
       and nvl(fcr.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(fcr.skp_communication_channel,-1) else ccl.skp_communication_channel end
       and nvl(fcr.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(fcr.skp_communication_type,-1) else ccl.skp_communication_type end
       and nvl(fcr.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(fcr.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
       and nvl(fcr.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(fcr.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
       and nvl(fcr.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(fcr.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
       and nvl(fcr.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(fcr.skp_communication_status,-1) else ccl.skp_communication_status end
       and nvl(fcr.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(fcr.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
    left join owner_dwh.cl_communication_channel ccls on fcr.skp_communication_channel = ccls.skp_communication_channel
    left join owner_dwh.cl_communication_type cct on fcr.skp_communication_type = cct.SKP_COMMUNICATION_TYPE
    left join owner_dwh.cl_communication_subtype ccs on fcr.skp_communication_subtype = ccs.skp_communication_subtype
    left join owner_dwh.cl_comm_subtype_specif css on fcr.skp_comm_subtype_specif = css.SKP_COMM_SUBTYPE_SPECIF
    left join owner_dwh.cl_comm_subtype_sub_specif csf on fcr.skp_comm_subtype_sub_specif = csf.SKP_COMM_SUBTYPE_SUB_SPECIF
    left join owner_dwh.cl_communication_status cst on fcr.skp_communication_status = cst.skp_communication_status
    left join owner_Dwh.cl_communication_result_type crt on fcr.skp_communication_result_type = crt.skp_communication_result_type
    where fcr.date_call >= trunc(sysdate-30) and fcr.skp_credit_case in (select skp_credit_case from camp_rtn_contracts);
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
    pstats('gtt_cmp_rtn_02_commlist');  
    
    ptruncate('gtt_cmp_rtn_comm_parts');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into gtt_cmp_rtn_comm_parts');
    insert /*+ APPEND */ into gtt_cmp_rtn_comm_parts
    select skf_communication_record,
           coalesce
           (
               max(decode(code_comm_result_part, 'CALL_ON', text_value)), 
               max(decode(code_comm_result_part, 'PRMS_DT_MPF', text_value)), 
               max(decode(code_comm_result_part, 'DATETIME', text_value)), 
               max(decode(code_comm_result_part, 'PUSH_TOSIGN', text_value))
           )DATE_PROMISE,
           max(decode(code_comm_result_part, 'PHONE', text_value)) CB_PHONE
    from camp_comm_res_part
    where skf_communication_record in (select skf_communication_record from ap_crm.gtt_cmp_rtn_02_commlist)
    group by skf_communication_record;
    AP_PUBLIC.CORE_LOG_PKG.pEnd;
		commit;
		pstats('gtt_cmp_rtn_comm_parts');  
    
    ptruncate('camp_rtn_last_comm');
		AP_PUBLIC.CORE_LOG_PKG.pStart('Insert into camp_rtn_last_comm');
    insert into camp_rtn_last_comm
    with comm as
     (
         select /*+ MATERIALIZE */ * from gtt_cmp_rtn_02_commlist
         where (skp_client, skp_credit_case, dtime_inserted) in
         (
             select skp_client, skp_credit_case, max(dtime_inserted)dtime_inserted from gtt_cmp_rtn_02_commlist
             group by skp_client, skp_credit_case
         )
     ),
     cancl as
     (
            select /*+ MATERIALIZE */  comm.skp_client, comm.skp_credit_case, min(date_call)dt_cancel_req
            from gtt_cmp_rtn_02_commlist comm
            inner join camp_cfg_comm_list ccl on ccl.name_campaign in ('Retention') and ccl.logic_group = 'Cancel' and ccl.active = 'Y'
               and nvl(comm.skp_communication_channel,-1) = case when ccl.skp_communication_channel is null then nvl(comm.skp_communication_channel,-1) else ccl.skp_communication_channel end
               and nvl(comm.skp_communication_type,-1) = case when ccl.skp_communication_type is null then nvl(comm.skp_communication_type,-1) else ccl.skp_communication_type end
               and nvl(comm.skp_communication_subtype,-1) = case when ccl.skp_communication_subtype is null then nvl(comm.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
               and nvl(comm.skp_comm_subtype_specif,-1) = case when ccl.skp_comm_subtype_specif is null then nvl(comm.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
               and nvl(comm.skp_comm_subtype_sub_specif,-1) = case when ccl.skp_comm_subtype_sub_specif is null then nvl(comm.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
               and nvl(comm.skp_communication_status,-1) = case when ccl.skp_communication_status is null then nvl(comm.skp_communication_status,-1) else ccl.skp_communication_status end
               and nvl(comm.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(comm.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
            group by comm.skp_client, comm.skp_credit_case  
     ),
     contact as
     (
         select /*+ MATERIALIZE */ con.skp_credit_case, con.text_contact, cct.NAME_CONTACT_TYPE from owner_Dwh.f_Credit_Case_Request_Cont_Tt con
         left join owner_dwh.cl_contact_type cct on con.skp_contact_type = cct.SKP_CONTACT_TYPE
         where skp_credit_case in (select skp_credit_case from comm)
           and con.skp_contact_type = 2     
     ),
     comp as
     (
         select /*+ MATERIALIZE */ comm.date_call, comm.skf_communication_record, comm.skp_client, comm.skp_credit_case,
                 comm.name_communication_channel, comm.name_communication_type, comm.name_communication_subtype, comm.name_comm_subtype_specif, comm.name_comm_subtype_sub_specif, comm.name_communication_status, comm.name_communication_result_type,
                 comm.text_note, comm.text_contact,
                 max(ccl.logic_group) sub_campaign,
                 case when substr(max(nvl(ccl.action,'1.Call')),3) = 'Call' then
                          case when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) < trunc(sysdate) then 'Call'
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) >= trunc(sysdate) then 'Do not call'
                          else substr(max(ccl.action),3) end
                      when substr(max(nvl(ccl.action,'1.Call')),3) = 'Delay' then
                          case when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) < trunc(sysdate) then 'Call' 
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'N' and crp.date_promise is not null and to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy') + max(ccl.delay_days) >= trunc(sysdate) then 'Do not call'
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'Y' and comm.date_call + max(ccl.delay_days) < trunc(sysdate) then 'Call'
                               when substr(max(nvl(ccl.ignore_callback,'2.N')),3) = 'Y' and comm.date_call + max(ccl.delay_days) >= trunc(sysdate) then 'Do not call'
                          else substr(max(ccl.action),3) end         
                 else 'Do not call' end action,
                to_Date(substr(crp.date_promise,0,10),'dd/mm/yyyy')date_promise
         from comm 
         left join camp_cfg_comm_list ccl on ccl.name_campaign in ('Retention','FF General') and ccl.active = 'Y'
           and nvl(comm.skp_communication_channel,-1)     = case when ccl.skp_communication_channel is null then nvl(comm.skp_communication_channel,-1) else ccl.skp_communication_channel end
           and nvl(comm.skp_communication_type,-1)        = case when ccl.skp_communication_type is null then nvl(comm.skp_communication_type,-1) else ccl.skp_communication_type end
           and nvl(comm.skp_communication_subtype,-1)     = case when ccl.skp_communication_subtype is null then nvl(comm.skp_communication_subtype,-1) else ccl.skp_communication_subtype end
           and nvl(comm.skp_comm_subtype_specif,-1)       = case when ccl.skp_comm_subtype_specif is null then nvl(comm.skp_comm_subtype_specif,-1) else  ccl.skp_comm_subtype_specif end
           and nvl(comm.skp_comm_subtype_sub_specif,-1)   = case when ccl.skp_comm_subtype_sub_specif is null then nvl(comm.skp_comm_subtype_sub_specif,-1) else ccl.skp_comm_subtype_sub_specif end
           and nvl(comm.skp_communication_status,-1)      = case when ccl.skp_communication_status is null then nvl(comm.skp_communication_status,-1) else ccl.skp_communication_status end
           and nvl(comm.skp_communication_result_type,-1) = case when ccl.skp_communication_result_type is null then nvl(comm.skp_communication_result_type,-1) else ccl.skp_communication_result_type end
         left join gtt_cmp_rtn_comm_parts crp on comm.skf_communication_record = crp.skf_communication_record
         group by comm.date_call, comm.skf_communication_record, comm.skp_client, comm.skp_credit_case,
                     comm.name_communication_channel, comm.name_communication_type, comm.name_communication_subtype, comm.name_comm_subtype_specif, 
                     comm.name_comm_subtype_sub_specif, comm.name_communication_status, comm.name_communication_result_type,
                     comm.text_note, comm.text_contact, crp.date_promise
     )
     select distinct comp.date_call, comp.skf_communication_record, comp.skp_client, comp.skp_credit_case,
                 comp.name_communication_channel, comp.name_communication_type, comp.name_communication_subtype, comp.name_comm_subtype_specif, comp.name_comm_subtype_sub_specif, comp.name_communication_status, comp.name_communication_result_type,
                 comp.text_note, 
                 contact.text_contact,
                 comp.sub_campaign,
                 comp.action,
                 comp.date_promise,
                 cancl.dt_cancel_req
      from comp
      left join contact on comp.skp_credit_case = contact.skp_credit_case
      left join cancl on comp.skp_credit_case = cancl.skp_credit_case;
      AP_PUBLIC.CORE_LOG_PKG.pEnd;
		  commit;
		  pstats('camp_rtn_last_comm');
      AP_PUBLIC.CORE_LOG_PKG.pFinish;
END;
/

