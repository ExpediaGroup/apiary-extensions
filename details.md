# Partition in glue

```
[jfaulkner@ip-10-18-234-152 ~]$ aws glue get-partition --database-name onprem_conversation --table-name hadoop_dm_cust_ops_call_bkg_detail --partition-values BKG,2013-01-01 | jq
{
  "Partition": {
    "Values": [
      "BKG",
      "2013-01-01"
    ],
    "DatabaseName": "onprem_conversation",
    "TableName": "hadoop_dm_cust_ops_call_bkg_detail",
    "CreationTime": "2025-06-02T11:39:07+00:00",
    "LastAccessTime": "1970-01-01T00:00:00+00:00",
    "StorageDescriptor": {
      "Columns": [
        {
          "Name": "cust_ops_call_id",
          "Type": "string"
        },
        {
          "Name": "cust_ops_call_seg_seq_nbr",
          "Type": "int"
        },
        {
          "Name": "trans_date_key",
          "Type": "int"
        },
        {
          "Name": "trans_agnt_key",
          "Type": "int"
        },
        {
          "Name": "agnt_skill_target_id",
          "Type": "int"
        },
        {
          "Name": "router_call_day_id",
          "Type": "int"
        },
        {
          "Name": "router_call_id",
          "Type": "int"
        },
        {
          "Name": "src_call_start_datetm",
          "Type": "string"
        },
        {
          "Name": "src_call_end_datetm",
          "Type": "string"
        },
        {
          "Name": "aest_call_start_date_key",
          "Type": "int"
        },
        {
          "Name": "gmt_call_start_date_key",
          "Type": "int"
        },
        {
          "Name": "pst_call_start_date_key",
          "Type": "int"
        },
        {
          "Name": "pst_call_end_date_key",
          "Type": "int"
        },
        {
          "Name": "pst_call_start_tm_key",
          "Type": "int"
        },
        {
          "Name": "pst_call_end_tm_key",
          "Type": "int"
        },
        {
          "Name": "agnt_periph_nbr",
          "Type": "string"
        },
        {
          "Name": "call_itin_nbr",
          "Type": "string"
        },
        {
          "Name": "inbnd_dialed_nbr",
          "Type": "string"
        },
        {
          "Name": "outbnd_dialed_nbr",
          "Type": "string"
        },
        {
          "Name": "ani_nbr",
          "Type": "string"
        },
        {
          "Name": "cust_ops_ngcc_agnt_sessn_id",
          "Type": "string"
        },
        {
          "Name": "cust_ops_ngcc_media_id",
          "Type": "string"
        },
        {
          "Name": "orignl_ani_nbr",
          "Type": "string"
        },
        {
          "Name": "orignl_cust_ops_ngcc_cntct_id",
          "Type": "string"
        },
        {
          "Name": "vq_inbnd_call_id",
          "Type": "string"
        },
        {
          "Name": "vq_return_call_id",
          "Type": "string"
        },
        {
          "Name": "cust_ops_agnt_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_agnt_id",
          "Type": "int"
        },
        {
          "Name": "call_agnt_frst_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_middl_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_last_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_job_title_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_hire_date",
          "Type": "string"
        },
        {
          "Name": "call_agnt_hire_tenr_day",
          "Type": "int"
        },
        {
          "Name": "call_agnt_termnatn_date",
          "Type": "string"
        },
        {
          "Name": "call_agnt_vndr_loc_id",
          "Type": "int"
        },
        {
          "Name": "call_agnt_vndr_loc_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_vndr_id",
          "Type": "smallint"
        },
        {
          "Name": "call_agnt_vndr_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_role_id",
          "Type": "smallint"
        },
        {
          "Name": "call_agnt_role_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_role_tenr_day",
          "Type": "int"
        },
        {
          "Name": "call_agnt_prim_cust_ops_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "call_agnt_prim_cust_ops_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_profcncy_id",
          "Type": "smallint"
        },
        {
          "Name": "call_agnt_profcncy_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_mgr_frst_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_mgr_last_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_full_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_mgr_full_name",
          "Type": "string"
        },
        {
          "Name": "call_agnt_typ_id",
          "Type": "int"
        },
        {
          "Name": "call_agnt_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_key",
          "Type": "int"
        },
        {
          "Name": "call_business_partnr_id",
          "Type": "int"
        },
        {
          "Name": "call_business_partnr_sys_name",
          "Type": "string"
        },
        {
          "Name": "call_expe_business_partnr_id",
          "Type": "int"
        },
        {
          "Name": "call_ian_business_partnr_id",
          "Type": "int"
        },
        {
          "Name": "call_as400_business_partnr_src_code",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_tpid",
          "Type": "int"
        },
        {
          "Name": "call_business_partnr_tpid_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_svc_brand_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_svc_short_cntry_code",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_svc_cntry_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_svc_super_regn_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_svc_super_regn_desc",
          "Type": "string"
        },
        {
          "Name": "call_actv_business_partnr_ind",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_acct_mgr_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_b2b_billng_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_co_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_home_url",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_med_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_mgmt_unit_code",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_mgmt_unit_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_oper_regn_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_rpt_co_code",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_rpt_co_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_seg_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_site_platform_name",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_start_date",
          "Type": "string"
        },
        {
          "Name": "call_business_partnr_website_domain_name",
          "Type": "string"
        },
        {
          "Name": "call_parnt_business_partnr_id",
          "Type": "int"
        },
        {
          "Name": "call_parnt_business_partnr_name",
          "Type": "string"
        },
        {
          "Name": "call_tpid_key",
          "Type": "int"
        },
        {
          "Name": "call_tpid",
          "Type": "int"
        },
        {
          "Name": "call_tpid_name",
          "Type": "string"
        },
        {
          "Name": "call_tpid_cntry_code",
          "Type": "string"
        },
        {
          "Name": "call_tpid_cntry_name",
          "Type": "string"
        },
        {
          "Name": "call_tpid_hwire_pos_code",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_key",
          "Type": "smallint"
        },
        {
          "Name": "call_mgmt_unit_code",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_1_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_2_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_3_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_4_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_5_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_6_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_7_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_8_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_9_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_10_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_11_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_12_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_13_name",
          "Type": "string"
        },
        {
          "Name": "call_mgmt_unit_lvl_14_name",
          "Type": "string"
        },
        {
          "Name": "call_lang_key",
          "Type": "int"
        },
        {
          "Name": "call_lang_code",
          "Type": "string"
        },
        {
          "Name": "call_lang_name",
          "Type": "string"
        },
        {
          "Name": "call_product_cat_key",
          "Type": "smallint"
        },
        {
          "Name": "call_product_cat_name",
          "Type": "string"
        },
        {
          "Name": "call_service_type_name",
          "Type": "string"
        },
        {
          "Name": "call_service_category_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_call_typ_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_call_typ_id",
          "Type": "int"
        },
        {
          "Name": "call_typ_code_list",
          "Type": "string"
        },
        {
          "Name": "call_typ_desc",
          "Type": "string"
        },
        {
          "Name": "call_typ_caller_need_id",
          "Type": "smallint"
        },
        {
          "Name": "call_typ_caller_need_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_caller_need_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_cust_ops_product_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "call_typ_cust_ops_product_typ_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_cust_ops_product_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_cust_trans_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "call_typ_cust_trans_typ_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_cust_trans_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_call_seg_grp_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_call_seg_grp_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_phon_site_chnnl_plcmnt_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_phon_site_chnnl_plcmnt_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_route_typ_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_staff_grp_code",
          "Type": "string"
        },
        {
          "Name": "cust_ops_skill_grp_key",
          "Type": "int"
        },
        {
          "Name": "skill_grp_id",
          "Type": "int"
        },
        {
          "Name": "skill_grp_code_list",
          "Type": "string"
        },
        {
          "Name": "skill_grp_desc",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_caller_need_id",
          "Type": "smallint"
        },
        {
          "Name": "base_skill_grp_caller_need_code",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_caller_need_name",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_cust_ops_product_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "base_skill_grp_cust_ops_product_typ_code",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_cust_ops_product_typ_name",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_cust_trans_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "base_skill_grp_cust_trans_typ_code",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_cust_trans_typ_name",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_call_seg_grp_code",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_call_seg_grp_name",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_desc",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_code_list",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_phon_site_chnnl_plcmnt_code",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_phon_site_chnnl_plcmnt_name",
          "Type": "string"
        },
        {
          "Name": "base_skill_grp_staff_grp_code",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_call_seg_grp_code",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_call_seg_grp_name",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_caller_need_code",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_caller_need_name",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_cust_ops_product_typ_code",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_cust_ops_product_typ_name",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_cust_trans_typ_code",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_cust_trans_typ_name",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_lang_code",
          "Type": "string"
        },
        {
          "Name": "frcast_grp_lang_name",
          "Type": "string"
        },
        {
          "Name": "skill_grp_profcncy_code",
          "Type": "string"
        },
        {
          "Name": "cust_ops_icrs_call_periph_dispostn_typ_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_icrs_periph_call_typ_id",
          "Type": "int"
        },
        {
          "Name": "cust_ops_icrs_call_dispostn_typ_id",
          "Type": "int"
        },
        {
          "Name": "periph_call_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_dispostn_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_dispostn_name",
          "Type": "string"
        },
        {
          "Name": "call_sys_err",
          "Type": "string"
        },
        {
          "Name": "call_sys_gb",
          "Type": "string"
        },
        {
          "Name": "call_sys_partval",
          "Type": "string"
        },
        {
          "Name": "call_sys_ref_id",
          "Type": "string"
        },
        {
          "Name": "call_sys_lodg_property_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_ops_brand_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_bkg_windw_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_caller_need_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_caller_need_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_intl_dom_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_intl_dom_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_ops_product_typ_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_ops_product_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_trans_typ_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_trans_typ_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_phon_site_chnnl_plcmnt_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_phon_site_chnnl_plcmnt_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_ops_pos_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_ops_pos_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_cust_ops_sub_brand_name",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_trvl_duratn_code",
          "Type": "string"
        },
        {
          "Name": "call_typ_var_trvl_duratn_name",
          "Type": "string"
        },
        {
          "Name": "call_exprnce_var_intfc_fails",
          "Type": "string"
        },
        {
          "Name": "call_exprnce_var_lang_list",
          "Type": "string"
        },
        {
          "Name": "call_sys_var_media_publctn_id",
          "Type": "string"
        },
        {
          "Name": "call_exprnce_var_persna",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_key",
          "Type": "int"
        },
        {
          "Name": "target_seg_call_typ_id",
          "Type": "int"
        },
        {
          "Name": "target_seg_call_typ_code_list",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_desc",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_caller_need_id",
          "Type": "smallint"
        },
        {
          "Name": "target_seg_call_typ_caller_need_code",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_caller_need_name",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_cust_ops_product_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "target_seg_call_typ_cust_ops_product_typ_code",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_cust_ops_product_typ_name",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_cust_trans_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "target_seg_call_typ_cust_trans_typ_code",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_cust_trans_typ_name",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_call_seg_grp_code",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_call_seg_grp_name",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_phon_site_chnnl_plcmnt_code",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_phon_site_chnnl_plcmnt_name",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_route_typ_code",
          "Type": "string"
        },
        {
          "Name": "target_seg_call_typ_staff_grp_code",
          "Type": "string"
        },
        {
          "Name": "call_seg_state_ind",
          "Type": "string"
        },
        {
          "Name": "vq_call_ind",
          "Type": "string"
        },
        {
          "Name": "vq_call_state_ind",
          "Type": "string"
        },
        {
          "Name": "cust_ops_phon_assign_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_phon_assign_id",
          "Type": "int"
        },
        {
          "Name": "cust_ops_phon_id",
          "Type": "int"
        },
        {
          "Name": "phon_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_phon_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "cust_ops_phon_typ_name",
          "Type": "string"
        },
        {
          "Name": "phon_carrier_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_carrier_name",
          "Type": "string"
        },
        {
          "Name": "phon_cntry_prfx_nbr",
          "Type": "string"
        },
        {
          "Name": "phon_cntry_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_phon_regn_id",
          "Type": "smallint"
        },
        {
          "Name": "cust_ops_phon_regn_name",
          "Type": "string"
        },
        {
          "Name": "phon_vanity_desc",
          "Type": "string"
        },
        {
          "Name": "phon_local_nbr",
          "Type": "string"
        },
        {
          "Name": "intl_phon_nbr",
          "Type": "string"
        },
        {
          "Name": "rcf_phon_nbr",
          "Type": "string"
        },
        {
          "Name": "rcf_phon_carrier_id",
          "Type": "smallint"
        },
        {
          "Name": "rcf_phon_carrier_name",
          "Type": "string"
        },
        {
          "Name": "phon_acquir_date",
          "Type": "string"
        },
        {
          "Name": "phon_retire_date",
          "Type": "string"
        },
        {
          "Name": "phon_carrier_start_datetm",
          "Type": "string"
        },
        {
          "Name": "phon_carrier_end_datetm",
          "Type": "string"
        },
        {
          "Name": "phon_carrier_acct_nbr",
          "Type": "string"
        },
        {
          "Name": "phon_assign_format_phon_txt",
          "Type": "string"
        },
        {
          "Name": "phon_assign_business_partnr_key",
          "Type": "int"
        },
        {
          "Name": "phon_assign_start_datetm",
          "Type": "string"
        },
        {
          "Name": "phon_assign_end_datetm",
          "Type": "string"
        },
        {
          "Name": "phon_assign_parnt_phon_id",
          "Type": "int"
        },
        {
          "Name": "phon_assign_cust_ops_branding_cat_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_cust_ops_branding_cat_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cust_ops_brand_lvl_1_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_cust_ops_brand_lvl_1_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cust_ops_brand_lvl_2_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_cust_ops_brand_lvl_2_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cust_ops_brand_lvl_3_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_cust_ops_brand_lvl_3_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cust_ops_pos_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_cust_ops_pos_code",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cust_ops_pos_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_iso_lang_code",
          "Type": "string"
        },
        {
          "Name": "phon_assign_iso_lang_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cust_ops_product_typ_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_cust_ops_product_typ_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_mktg_chnnl_name_1",
          "Type": "string"
        },
        {
          "Name": "phon_assign_mktg_chnnl_name_2",
          "Type": "string"
        },
        {
          "Name": "phon_assign_mktg_cmpgn_lvl_1_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_mktg_cmpgn_lvl_1_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_mktg_cmpgn_lvl_2_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_mktg_cmpgn_lvl_2_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_media_typ_lvl_1_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_media_typ_lvl_1_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_media_typ_lvl_2_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_media_typ_lvl_2_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_media_typ_lvl_3_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_media_typ_lvl_3_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_publctn_typ_lvl_1_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_publctn_typ_lvl_1_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_publctn_typ_lvl_2_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_publctn_typ_lvl_2_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_site_plcmnt_lvl_1_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_site_plcmnt_lvl_1_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_site_plcmnt_lvl_2_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_site_plcmnt_lvl_2_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_site_chnnl_plcmnt_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_site_chnnl_plcmnt_code",
          "Type": "string"
        },
        {
          "Name": "phon_assign_site_chnnl_plcmnt_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_mktg_effrt_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_srch_cat_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_srch_cat_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_srch_term_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_holder_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_holder_frst_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_holder_last_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_creative_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_creative_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_call_to_actn_id",
          "Type": "smallint"
        },
        {
          "Name": "phon_assign_call_to_actn_name",
          "Type": "string"
        },
        {
          "Name": "phon_assign_ivr_exprnce_id",
          "Type": "int"
        },
        {
          "Name": "phon_assign_ivr_exprnce_code_list",
          "Type": "string"
        },
        {
          "Name": "phon_assign_ivr_exprnce_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_hrs_of_operatn_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_cost_per_minute_desc",
          "Type": "string"
        },
        {
          "Name": "phon_assign_prk_until_datetm",
          "Type": "string"
        },
        {
          "Name": "phon_assign_rpt_dnp_ind",
          "Type": "string"
        },
        {
          "Name": "cust_ops_call_src_tm_zone_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_call_src_tm_zone_id",
          "Type": "int"
        },
        {
          "Name": "cust_ops_call_src_tm_zone_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_call_duratn_windw_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_ngcc_cntct_dirctn_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_ngcc_dirctn_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_cntct_dirctn_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_ngcc_cntct_disconnect_typ_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_ngcc_cntct_disconnect_reasn_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_cntct_disconnect_typ_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_ngcc_dispostn_typ_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_cntct_dispostn_typ_name",
          "Type": "string"
        },
        {
          "Name": "agnt_handled_ind",
          "Type": "string"
        },
        {
          "Name": "cust_disconnect_ind",
          "Type": "string"
        },
        {
          "Name": "handled_wthn_sla_ind",
          "Type": "string"
        },
        {
          "Name": "ivr_handled_ind",
          "Type": "string"
        },
        {
          "Name": "outbnd_ind",
          "Type": "string"
        },
        {
          "Name": "cust_ops_ngcc_comptncy_grp_key",
          "Type": "int"
        },
        {
          "Name": "cust_ops_ngcc_comptncy_grp_id",
          "Type": "int"
        },
        {
          "Name": "agnt_ngcc_comptncy_grp_name",
          "Type": "string"
        },
        {
          "Name": "short_call_ind",
          "Type": "string"
        },
        {
          "Name": "zero_talk_tm_ind",
          "Type": "string"
        },
        {
          "Name": "agnt_disconnect_cnt",
          "Type": "int"
        },
        {
          "Name": "attch_outbnd_cnt",
          "Type": "int"
        },
        {
          "Name": "handle_cnt",
          "Type": "int"
        },
        {
          "Name": "unattch_outbnd_cnt",
          "Type": "int"
        },
        {
          "Name": "aftr_call_wrk_second_cnt",
          "Type": "int"
        },
        {
          "Name": "attch_outbnd_second_cnt",
          "Type": "int"
        },
        {
          "Name": "hold_second_cnt",
          "Type": "int"
        },
        {
          "Name": "talk_second_cnt",
          "Type": "int"
        },
        {
          "Name": "totl_handle_second_cnt",
          "Type": "int"
        },
        {
          "Name": "unattch_outbnd_second_cnt",
          "Type": "int"
        },
        {
          "Name": "hold_call_ind",
          "Type": "string"
        },
        {
          "Name": "system_disconnect_cnt",
          "Type": "int"
        },
        {
          "Name": "abandon_second_cnt",
          "Type": "int"
        },
        {
          "Name": "netwrk_abandon_second_cnt",
          "Type": "int"
        },
        {
          "Name": "trnsfr_abandon_second_cnt",
          "Type": "int"
        },
        {
          "Name": "totl_offr_cnt",
          "Type": "int"
        },
        {
          "Name": "inbnd_queue_offr_cnt",
          "Type": "int"
        },
        {
          "Name": "transfr_queue_offr_cnt",
          "Type": "int"
        },
        {
          "Name": "answr_second_cnt",
          "Type": "int"
        },
        {
          "Name": "netwrk_answr_second_cnt",
          "Type": "int"
        },
        {
          "Name": "trnsfr_answr_second_cnt",
          "Type": "int"
        },
        {
          "Name": "answr_20_second_call_ind",
          "Type": "string"
        },
        {
          "Name": "answr_30_second_call_ind",
          "Type": "string"
        },
        {
          "Name": "answr_60_second_call_ind",
          "Type": "string"
        },
        {
          "Name": "answr_120_second_call_ind",
          "Type": "string"
        },
        {
          "Name": "agnt_conect_attmpt_cnt",
          "Type": "int"
        },
        {
          "Name": "agnt_trnsfr_cnt",
          "Type": "int"
        },
        {
          "Name": "arrv_cnt",
          "Type": "int"
        },
        {
          "Name": "block_call_cnt",
          "Type": "int"
        },
        {
          "Name": "cntct_cnt",
          "Type": "int"
        },
        {
          "Name": "hold_abandon_cnt",
          "Type": "int"
        },
        {
          "Name": "inbnd_cnt",
          "Type": "int"
        },
        {
          "Name": "ivr_second_cnt",
          "Type": "int"
        },
        {
          "Name": "othr_err_cnt",
          "Type": "int"
        },
        {
          "Name": "queue_abandon_cnt",
          "Type": "int"
        },
        {
          "Name": "retry_agnt_cnt",
          "Type": "int"
        },
        {
          "Name": "sys_terminate_cnt",
          "Type": "int"
        },
        {
          "Name": "sys_trnsfr_cnt",
          "Type": "int"
        },
        {
          "Name": "transfr_init_cnt",
          "Type": "int"
        },
        {
          "Name": "arrv_second_cnt",
          "Type": "int"
        },
        {
          "Name": "duratn_second_cnt",
          "Type": "int"
        },
        {
          "Name": "hang_up_second_cnt",
          "Type": "int"
        },
        {
          "Name": "ivr_queue_second_cnt",
          "Type": "int"
        },
        {
          "Name": "ring_second_cnt",
          "Type": "int"
        },
        {
          "Name": "totl_wrap_up_second_cnt",
          "Type": "int"
        },
        {
          "Name": "terminate_wrap_up_second_cnt",
          "Type": "int"
        },
        {
          "Name": "transfr_queue_second_cnt",
          "Type": "int"
        },
        {
          "Name": "agnt_disconnect_short_call_cnt",
          "Type": "int"
        },
        {
          "Name": "short_call_cnt",
          "Type": "int"
        },
        {
          "Name": "zero_talk_cnt",
          "Type": "int"
        },
        {
          "Name": "hold_call_cnt",
          "Type": "int"
        },
        {
          "Name": "answr_20_second_call_cnt",
          "Type": "int"
        },
        {
          "Name": "answr_30_second_call_cnt",
          "Type": "int"
        },
        {
          "Name": "answr_60_second_call_cnt",
          "Type": "int"
        },
        {
          "Name": "answr_120_second_call_cnt",
          "Type": "int"
        },
        {
          "Name": "tier1_to_tier2_cnt",
          "Type": "int"
        },
        {
          "Name": "ivr_terminate_cnt",
          "Type": "int"
        },
        {
          "Name": "netwrk_handle_call_cnt",
          "Type": "int"
        },
        {
          "Name": "netwrk_totl_handle_tm",
          "Type": "int"
        },
        {
          "Name": "netwrk_handle_talk_tm",
          "Type": "int"
        },
        {
          "Name": "netwrk_handle_hold_tm",
          "Type": "int"
        },
        {
          "Name": "netwrk_aftr_call_wrk_tm",
          "Type": "int"
        },
        {
          "Name": "trnsfr_handle_call_cnt",
          "Type": "int"
        },
        {
          "Name": "trnsfr_totl_handle_tm",
          "Type": "int"
        },
        {
          "Name": "trnsfr_handle_talk_tm",
          "Type": "int"
        },
        {
          "Name": "trnsfr_handle_hold_tm",
          "Type": "int"
        },
        {
          "Name": "trnsfr_aftr_call_wrk_tm",
          "Type": "int"
        },
        {
          "Name": "agnt_disconnect_ind",
          "Type": "string"
        },
        {
          "Name": "ivr_delay_second_cnt",
          "Type": "int"
        },
        {
          "Name": "gmt_trans_date_key",
          "Type": "int"
        },
        {
          "Name": "pst_trans_date_key",
          "Type": "int"
        },
        {
          "Name": "prim_purch_trvl_acct_key",
          "Type": "int"
        },
        {
          "Name": "gross_trans_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_bkg_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "gross_purch_price_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "gross_purch_cost_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "gross_cncl_price_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "gross_cncl_cost_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "totl_cost_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "margn_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "gross_purch_ordr_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_purch_trans_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_cncl_ordr_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_cncl_trans_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_ordr_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_agncy_trans_cnt",
          "Type": "int"
        },
        {
          "Name": "gross_merch_trans_cnt",
          "Type": "int"
        },
        {
          "Name": "cust_ops_est_bk_rev_amt_usd",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "itin_detail",
          "Type": "array<struct<SRC_SYS_ID:int,TPID:int,TRL:int,PRODUCT_CAT_KEY:int,PRODUCT_LN_NAME:string,RESPONDENT_ID:string,BUSINESS_PARTNR_KEY:int,ITIN_NBR:string,ORDER_NBR:bigint,ITIN_BK_AGNT_KEY:int,ITIN_BK_AGNT_HIRE_TENR_DAY:int,ITIN_BK_AGNT_ROLE_TENR_DAY:int,BK_AGNT_KEY:int,BK_DATE_KEY:int,AB_TST_GRP_ID:int,BEGIN_USE_DATE_KEY:int,BEGIN_USE_DATE:string,BK_DATE:string,BK_DATETM:string,BKG_IND_KEY:int,PKG_BKG_IND_KEY:int,BKG_PRODUCT_LN_COMPONENT_KEY:int,BKG_WINDW_KEY:int,COST_CURRN_KEY:int,COUPN_KEY:int,CUST_OPS_BKG_IND_KEY:int,END_USE_DATE_KEY:int,END_USE_DATE:string,LGL_ENTITY_KEY:int,MKTG_CODE_KEY:int,ORACLE_GL_PRODUCT_KEY:int,PRICE_CURRN_KEY:int,PRODUCT_LN_KEY:int,PST_TRANS_DATE_KEY:int,PST_TRANS_DATE:string,PST_TRANS_TM_KEY:int,AIR_TRIP_TYP_NAME:string,SAT_NIGHT_STAY_IND:string,AIR_BKG_IND_KEY:int,AIR_SETTLMNT_AGNT_TYP_KEY:int,PLATNG_CARRIER_KEY:int,PNR_REC_LOCATOR_CODE:string,TCKT_AIR_FARE_TYP_KEY:int,TCKT_ROUTE_KEY:int,TOUR_OPERATR_KEY:int,AGNT_ASST_IND:string,BKG_WINDW_RNG_NAME:string,CAR_CAT_NAME:string,CAR_TYP_NAME:string,CREDT_CARD_TYP_KEY:int,CAR_BASE_PRICE_PERIOD_KEY:int,CAR_BKG_IND_KEY:int,CAR_CLASS_KEY:int,CAR_DROP_OFF_LOC_KEY:int,CAR_PICK_UP_LOC_KEY:int,CAR_SPCL_EQUIP_GRP_KEY:int,CAR_VNDR_AGRMNT_KEY:int,CAR_VNDR_KEY:int,CRUIS_ADJ_REASN_KEY:int,CRUIS_CABN_TYP_KEY:int,CRUIS_RSDNC_STATE_PROVNC_KEY:int,CRUIS_SUB_DEST_KEY:int,DISEMBRK_PORT_KEY:int,EMBRK_PORT_KEY:int,SHIP_KEY:int,AGNT_TOUCH_IND:string,OFFRNG_ITM_KEY:int,DEST_SVC_SRCH_LOC_KEY:int,DEST_REGN_KEY:int,INS_OFFRNG_CAT_KEY:int,INS_OFFRNG_CAT_NAME:string,LENGTH_OF_STAY_RNG_NAME:string,EEM_PROPERTY_IND:string,EXPE_HALF_STAR_RTG:decimal(19,4),GDS_PROPERTY_IND:string,LODG_PROPERTY_NAME:string,MERCH_PROPERTY_IND:string,OPAQUE_PROPERTY_IND:string,PROPERTY_BRAND_NAME:string,PROPERTY_CNTRCT_MODEL_NAME:string,PROPERTY_CNTRY_NAME:string,PROPERTY_PARNT_CHAIN_NAME:string,PROPERTY_MKT_NAME:string,BKG_REFRL_SRC_KEY:int,DISTR_KEY:int,DOM_INTL_BKG_ITM_IND_KEY:int,LENGTH_OF_STAY_KEY:int,LODG_PROPERTY_KEY:int,LODG_RATE_PLN_KEY:int,LODG_RATE_RULE_KEY:int,ORDER_CONF_NBR:string,PRICE_STRUCT_KEY:int,TPID_CURRN_KEY:int,TRANS_TYP_KEY:int,TRVL_DURATN_KEY:int,OFFRNG_ITM_NAME:string,OFFRNG_NAME:string,PKG_BEGIN_USE_DATE:string,PKG_CAR_VNDR_1_KEY:int,PKG_CAR_VNDR_2_KEY:int,PKG_END_USE_DATE:string,PKG_LODG_PROPERTY_1_KEY:int,PKG_LODG_PROPERTY_2_KEY:int,PRICE_MODEL_NAME:string,FLEX_MOR_IND:string,PKG_TYP_NAME:string,TRANS_CAT_NAME:string,TRANS_TYP_DESC:string,TRANS_TYP_ID:int,TRANS_TYP_NAME:string,TRANS_USE_PERIOD_NAME:string,COST_CURRN_NAME:string,DISEMBRK_PORT_NAME:string,EMBRK_PORT_NAME:string,PLATNG_CARRIER_NAME:string,TCKT_DEST_AIRPT_CODE:string,TCKT_DEST_AIRPT_CNTRY_CODE:string,TCKT_DEST_AIRPT_CNTRY_NAME:string,TCKT_ORIGN_AIRPT_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_NAME:string,TCKT_ROUTE_NAME:string,BUSINESS_MODEL_NAME:string,BUSINESS_MODEL_SUBTYP_NAME:string,PKG_IND:string,BKG_ID:int,BKG_ITM_ID:int,BKG_SYS_OF_REC_ID:int,BKG_SYS_OF_REC_NAME:string,ITIN_CREATE_DATETM:string,ORDER_ID:bigint,ORDER_LN_SEQ_NBR:int,PROPERTY_LOCAL_BK_DATE_KEY:int,PROPERTY_LOCAL_BK_TM_KEY:int,PST_ITIN_CREATE_DATE:string,PST_ITIN_CREATE_DATE_KEY:int,PST_ITIN_CREATE_TM_KEY:int,TRANS_TM_KEY:int,PKG_ID:bigint,PKG_TRANS_TYP_KEY:int,BK_LANG_KEY:int,BK_LANG_CODE:string,BK_LANG_NAME:string,BK_GROSS_TRANS_CNT:int,BK_GROSS_BKG_AMT_USD:decimal(19,4),BK_GROSS_PURCH_PRICE_AMT_USD:decimal(19,4),BK_GROSS_PURCH_COST_AMT_USD:decimal(19,4),BK_GROSS_CNCL_PRICE_AMT_USD:decimal(19,4),BK_GROSS_CNCL_COST_AMT_USD:decimal(19,4),BK_TOTL_COST_AMT_USD:decimal(19,4),BK_MARGN_AMT_USD:decimal(19,4),BK_GROSS_PURCH_ORDR_CNT:int,BK_GROSS_PURCH_TRANS_CNT:int,BK_GROSS_CNCL_ORDR_CNT:int,BK_GROSS_CNCL_TRANS_CNT:int,BK_GROSS_ORDR_CNT:int,BK_GROSS_AGNCY_TRANS_CNT:int,BK_GROSS_MERCH_TRANS_CNT:int,BK_CUST_OPS_EST_BK_REV_AMT_USD:decimal(19,4),COUPN_PRICE_AMT_USD:decimal(19,4),EST_COST_OF_SALE_AMT_USD:decimal(19,4),EST_GROSS_PROFIT_AMT_USD:decimal(19,4),EST_NET_REV_AMT_USD:decimal(19,4),EST_VAR_COST_OF_SALE_AMT_USD:decimal(19,4),EST_VAR_GROSS_PROFIT_AMT_USD:decimal(19,4),FRNT_END_CMSN_AMT_USD:decimal(19,4),OTHR_COST_ADJ_AMT_USD:decimal(19,4),OTHR_DEST_SVC_TCKT_CNT:int,OTHR_FEE_COST_AMT_USD:decimal(19,4),OTHR_FEE_PRICE_AMT_USD:decimal(19,4),ADULT_CNT:int,AGNT_ASST_PURCH_FEE_AMT_USD:decimal(19,4),AGNT_TOUCH_CNT:int,BASE_COST_AMT_USD:decimal(19,4),BASE_PRICE_AMT_USD:decimal(19,4),AGNT_ASST_EXCH_FEE_AMT_USD:decimal(19,4),AGNT_ASST_REFUND_FEE_AMT_USD:decimal(19,4),AGNT_ASST_VOID_FEE_AMT_USD:decimal(19,4),BKG_FEE_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_COST_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_PRICE_AMT_USD:decimal(19,4),DELIVERY_FEE_COST_AMT_USD:decimal(19,4),DELIVERY_FEE_PRICE_AMT_USD:decimal(19,4),EXCH_PNLTY_COST_AMT_USD:decimal(19,4),EXCH_PNLTY_PRICE_AMT_USD:decimal(19,4),LAP_INFANT_CNT:int,PAPR_TCKT_FEE_AMT_USD:decimal(19,4),UNUSED_TCKT_COST_AMT_USD:decimal(19,4),UNUSED_TCKT_PRICE_AMT_USD:decimal(19,4),AIR_TRANS_SEG_CNT:int,AIR_TRANS_TCKT_CNT:int,PEAK_RATE_COST_ADJ_AMT_USD:decimal(19,4),PEAK_RATE_PRICE_ADJ_AMT_USD:decimal(19,4),OTHR_PRICE_ADJ_AMT_USD:decimal(19,4),PURE_MARGN_AMT_USD:decimal(19,4),RENTL_DAY_CNT:int,VAR_PRICE_ADJ_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_COST_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_CMSN_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_LODG_CMSN_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_COST_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_COST_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_CMSN_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_COST_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_PRICE_AMT_USD:decimal(19,4),PORT_CHRG_COST_AMT_USD:decimal(19,4),PORT_CHRG_PRICE_AMT_USD:decimal(19,4),SENIOR_CNT:int,OTHR_TAX_COST_AMT_USD:decimal(19,4),OTHR_TAX_PRICE_AMT_USD:decimal(19,4),NET_RETAIL_RATE_AMT_USD:decimal(19,4),MARKUP_AMT_USD:decimal(19,4),ADULT_DEST_SVC_TCKT_CNT:int,CHILD_DEST_SVC_TCKT_CNT:int,DEST_SVC_BKG_ITM_CNT:int,TOTL_DEST_SVC_TCKT_CNT:int,ADULT_INS_ITM_CNT:int,CHILD_INS_ITM_CNT:int,OTHR_INS_ITM_CNT:int,TOTL_INS_ITM_CNT:int,CNCL_PNLTY_WAIVR_PRICE_ADJ_AMT_USD:decimal(19,4),DYN_RATE_RULE_COST_AMT_USD:decimal(19,4),DYN_RATE_RULE_PRICE_AMT_USD:decimal(19,4),EMP_DISC_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),EXTRA_PERSN_COST_AMT_USD:decimal(19,4),EXTRA_PERSN_PRICE_AMT_USD:decimal(19,4),GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),GENRIC_COUPN_PRICE_AMT_USD:decimal(19,4),INFANT_CNT:int,LODG_PKG_SAVE_AMT_USD:decimal(19,4),LOYLTY_POINT_PRICE_ADJ_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_COST_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),MARGN_SALES_TAX_COST_AMT_USD:decimal(19,4),MARGN_SALES_TAX_PRICE_AMT_USD:decimal(19,4),NET_SVC_FEE_PRICE_AMT_USD:decimal(19,4),OCCUP_TAX_COST_AMT_USD:decimal(19,4),OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),RATE_PLN_RESTR_COST_AMT_USD:decimal(19,4),RATE_PLN_RESTR_PRICE_AMT_USD:decimal(19,4),REBATE_PRICE_AMT_USD:decimal(19,4),REFUND_PRICE_ADJ_AMT_USD:decimal(19,4),RM_NIGHT_CNT:int,SALES_TAX_COST_AMT_USD:decimal(19,4),SALES_TAX_PRICE_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_COST_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_PRICE_AMT_USD:decimal(19,4),STNDAL_HTL_PRICE_MOD_AMT_USD:decimal(19,4),SUPPL_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_PRICE_ADJ_AMT_USD:decimal(19,4),SVC_CHRG_COST_AMT_USD:decimal(19,4),SVC_CHRG_PRICE_AMT_USD:decimal(19,4),SVC_FEE_PRICE_AMT_USD:decimal(19,4),TCM_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_CMSN_AMT_USD:decimal(19,4),TOTL_COST_ADJ_AMT_USD:decimal(19,4),TOTL_FEE_COST_AMT_USD:decimal(19,4),TOTL_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_COST_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_PRICE_AMT_USD:decimal(19,4),TOTL_PERSN_CNT:int,TOTL_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_TAX_COST_AMT_USD:decimal(19,4),TOTL_TAX_PRICE_AMT_USD:decimal(19,4),VAR_MARGN_COST_ADJ_USD:decimal(19,4),CNCL_CHG_FEE_PRICE_AMT_USD:decimal(19,4),CHILD_CNT:int,TRANS_DATETM:string,AIR_PKG_SAVE_AMT_USD:decimal(19,4),PKG_AGNCY_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_CAR_CNT:int,PKG_AGNCY_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_RM_CNT:int,PKG_AGNCY_RM_NIGHT_UNIT_CNT:int,PKG_AGNCY_TCKT_UNIT_CNT:int,PKG_AGNCY_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_TRAIN_TCKT_UNIT_CNT:int,PKG_AIR_DURATN_DAY_CNT:int,PKG_AIR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AIR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_CNT:int,PKG_CAR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_CAR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_RENTL_DAY_UNIT_CNT:int,PKG_COST_ADJ_AMT_USD:decimal(19,4),PKG_CRUIS_CABN_UNIT_CNT:int,PKG_CRUIS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_FEE_PRICE_AMT_USD:decimal(19,4),PKG_DEST_SVC_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_MARGN_AMT_USD:decimal(19,4),PKG_DEST_SVC_TCKT_UNIT_CNT:int,PKG_INS_FEE_PRICE_AMT_USD:decimal(19,4),PKG_INS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_INS_ITM_UNIT_CNT:int,PKG_INS_MARGN_AMT_USD:decimal(19,4),PKG_LODG_FEE_PRICE_AMT_USD:decimal(19,4),PKG_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_LODG_MARGN_AMT_USD:decimal(19,4),PKG_MERCH_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_PRICE_ADJ_AMT_USD:decimal(19,4),PKG_SAVE_AMT_USD:decimal(19,4),PKG_SAVE_PRICE_AMT_USD:decimal(19,4),PKG_TAX_COST_AMT_USD:decimal(19,4),PKG_TAX_PRICE_AMT_USD:decimal(19,4),PKG_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_TRAIN_MARGN_AMT_USD:decimal(19,4),TOTL_PKG_FEE_COST_AMT_USD:decimal(19,4),TOTL_PKG_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_PKG_UNIT_CNT:int,DURATN_DAY_CNT:int,PKG_MERCH_CAR_CNT:int,PKG_MERCH_RM_CNT:int,PKG_MERCH_RM_NIGHT_UNIT_CNT:int,PKG_MERCH_TCKT_UNIT_CNT:int,PKG_MERCH_TRAIN_TCKT_UNIT_CNT:int,PKG_RM_CNT:int,PKG_RM_NIGHT_UNIT_CNT:int,PKG_TCKT_SEG_CNT:int,PKG_TCKT_UNIT_CNT:int,PKG_TOTL_TRVLR_CNT:int,PKG_TRAIN_DURATN_DAY_CNT:int,PKG_TRAIN_TCKT_UNIT_CNT:int,TRANS_AGNT_TOOL_NAME:string,TRANS_SRC_TYP_NAME:string,ONLINE_OFFLN_IND:string,MGMT_UNIT_KEY:int,MGMT_UNIT_CODE:string,MGMT_UNIT_NAME:string,MGMT_UNIT_LVL_1_NAME:string,MGMT_UNIT_LVL_2_NAME:string,MGMT_UNIT_LVL_3_NAME:string,MGMT_UNIT_LVL_4_NAME:string,MGMT_UNIT_LVL_5_NAME:string,MGMT_UNIT_LVL_6_NAME:string,MGMT_UNIT_LVL_7_NAME:string,MGMT_UNIT_LVL_8_NAME:string,MGMT_UNIT_LVL_9_NAME:string,MGMT_UNIT_LVL_10_NAME:string,MGMT_UNIT_LVL_11_NAME:string,MGMT_UNIT_LVL_12_NAME:string,MGMT_UNIT_LVL_13_NAME:string,MGMT_UNIT_LVL_14_NAME:string,PURCH_TRVL_ACCT_KEY:int,BKG_SERVICE_TYPE_NAME:string,BKG_SERVICE_CATEGORY_NAME:string,ETE_POS:string,ETE_FLAG:string,AGNT_ASST_CHG_FEE_AMT_USD:decimal(19,4),AGNT_ASST_CNCL_FEE_AMT_USD:decimal(19,4),SERV_FEE_TRANS_CNT:int,CNCL_FEE_TRANS_CNT:int,ABS_CHG_TRANS_CNT:int,ABS_CNCL_TRANS_CNT:int,BKG_AGENT_TIER_NAME:string>>"
        },
        {
          "Name": "ngcc_trans_typ_skill_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_trans_typ_skill_name",
          "Type": "string"
        },
        {
          "Name": "ngcc_product_skill_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_product_skill_name",
          "Type": "string"
        },
        {
          "Name": "ngcc_lang_skill_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_lang_skill_name",
          "Type": "string"
        },
        {
          "Name": "cust_ops_ngcc_agnt_id",
          "Type": "string"
        },
        {
          "Name": "ngcc_query_typ_skill_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_query_typ_skill_cat_name",
          "Type": "string"
        },
        {
          "Name": "ngcc_query_typ_skill_cat_desc",
          "Type": "string"
        },
        {
          "Name": "ngcc_seg_skill_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_seg_skill_cat_name",
          "Type": "string"
        },
        {
          "Name": "ngcc_seg_typ_skill_cat_desc",
          "Type": "string"
        },
        {
          "Name": "ngcc_extrnl_agnt_supprt_skill_id",
          "Type": "int"
        },
        {
          "Name": "ngcc_extrnl_agnt_supprt_skill_name",
          "Type": "string"
        },
        {
          "Name": "ngcc_extrnl_agnt_supprt_skill_desc",
          "Type": "string"
        },
        {
          "Name": "nav_int_case_id",
          "Type": "string"
        },
        {
          "Name": "nav_int_typ_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_typ_reasn_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_stat_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_local_currn",
          "Type": "string"
        },
        {
          "Name": "nav_int_assign_queue_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_cncl_case_ind",
          "Type": "string"
        },
        {
          "Name": "nav_int_guest_acct_case_ind",
          "Type": "string"
        },
        {
          "Name": "nav_int_anchor_ind",
          "Type": "string"
        },
        {
          "Name": "nav_int_case_sla_typ_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_sla_datetm",
          "Type": "string"
        },
        {
          "Name": "nav_int_sla_goal_datetm",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_org_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_div_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_org_unit_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_wrk_grp_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_resolve_org_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_resolve_div_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_resolve_org_unit_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_resolve_wrk_grp_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_cust_case_id",
          "Type": "string"
        },
        {
          "Name": "nav_int_cust_score_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_cust_identity_verify_ind",
          "Type": "string"
        },
        {
          "Name": "nav_int_itin_nbr",
          "Type": "string"
        },
        {
          "Name": "nav_int_tpid",
          "Type": "int"
        },
        {
          "Name": "nav_int_trl",
          "Type": "bigint"
        },
        {
          "Name": "nav_int_answr_second_cnt",
          "Type": "bigint"
        },
        {
          "Name": "nav_int_resolve_second_cnt",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "nav_int_task_wrap_up_duratn_second_cnt",
          "Type": "int"
        },
        {
          "Name": "nav_int_nav_int_tm_second_cnt",
          "Type": "decimal(19,4)"
        },
        {
          "Name": "nav_int_no_of_item_create_cnt",
          "Type": "int"
        },
        {
          "Name": "nav_int_case_cnt",
          "Type": "int"
        },
        {
          "Name": "nav_resolve_abandon_case_cnt",
          "Type": "int"
        },
        {
          "Name": "nav_resolve_cancel_case_cnt",
          "Type": "int"
        },
        {
          "Name": "nav_resolve_complete_case_cnt",
          "Type": "int"
        },
        {
          "Name": "nav_int_create_agnt_login_id",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_create_agnt_earns_cmsn_ind",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_email_addr",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_emp_typ_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_frst_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_middl_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_last_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_knwn_as_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_hire_date",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_termnatn_date",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_job_title_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_mgmt_div_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_mgr_frst_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_mgr_last_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_prim_business_grp_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_prim_typ_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_profcncy_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_role_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_prim_lang_code",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_prim_lang_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_scndry_lang_code",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_scndry_lang_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_tertiary_lang_code",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_tertiary_lang_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_vndr_loc_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_vndr_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_hire_tenr_day",
          "Type": "int"
        },
        {
          "Name": "nav_int_create_agnt_role_tenr_day",
          "Type": "int"
        },
        {
          "Name": "nav_int_create_agnt_service_category_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_update_agnt_login_id",
          "Type": "string"
        },
        {
          "Name": "nav_int_update_agnt_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_resolve_agnt_login_id",
          "Type": "string"
        },
        {
          "Name": "nav_int_resolve_agnt_key",
          "Type": "int"
        },
        {
          "Name": "nav_tm_zone_key",
          "Type": "int"
        },
        {
          "Name": "nav_tm_zone_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_source_create_datetm",
          "Type": "string"
        },
        {
          "Name": "nav_int_pst_create_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_gmt_create_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_aest_create_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_pst_create_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_gmt_create_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_aest_create_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_source_update_datetm",
          "Type": "string"
        },
        {
          "Name": "nav_int_pst_update_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_gmt_update_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_aest_update_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_pst_update_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_gmt_update_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_aest_update_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_source_resolve_datetm",
          "Type": "string"
        },
        {
          "Name": "nav_int_pst_resolve_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_gmt_resolve_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_aest_resolve_date_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_pst_resolve_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_gmt_resolve_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_aest_resolve_tm_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_em_lang_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_em_lang_code",
          "Type": "string"
        },
        {
          "Name": "nav_int_em_lang_key",
          "Type": "int"
        },
        {
          "Name": "nav_int_svc_list",
          "Type": "array<struct<SVC_CASE_ID:string,SVC_CASE_STAT_NAME:string,SVC_ASSIGN_QUEUE_NAME:string,SVC_INT_TYP_NAME:string,SVC_TRVL_STG_IND:string,SVC_LOCAL_CURRN:string,SVC_ERR_LOGIN_NAME:string,SVC_GUEST_ACCT_CASE_IND:string,SVC_CALLER_DISCONNECT_IND:string,SVC_CHG_EXECUTED_TYP_IND:string,SVC_COUPN_ISSUE_IND:string,SVC_COUPN_EXPR_DATE:string,SVC_VNDR_CONSULT_IND:string,SVC_REFUND_IND:string,SVC_WRITE_OFF_IND:string,SVC_ACTN_NAME:string,SVC_ACTN_DESC:string,SVC_MSSNG_RES_ACTN_CODE:string,SVC_MSSNG_RES_ACTN_NAME:string,SVC_GUEST_ACCT_IND:string,SVC_INS_IND:string,SVC_ANCHOR_IND:string,SVC_CUST_CALLBK_IND:string,SVC_CUST_CALLBK_DESC:string,SVC_COMPLAINT_IND:string,SVC_NEW_CASE_IND:string,SVC_CASE_CUST_EMAIL_ADDR:string,SVC_SLA_TYP_NAME:string,SVC_SLA_DATETM:string,SVC_SLA_GOAL_DATETM:string,SVC_ESCALATE_IND:string,SVC_ESCALATE_PARENT_CASE_ID:string,SVC_ESCALATE_ACTN_NAME:string,SVC_ESCALATE_COMMUNICATION_ISSUE_NAME:string,SVC_ESCALATE_FAULT_NAME:string,SVC_ESCALATE_ISSUE_TYP_NAME:string,SVC_ESCALATE_RESOLTN_NAME:string,SVC_ESCALATE_ROOT_CAUSE_NAME:string,SVC_ITIN_NBR:string,SVC_TPID:int,SVC_TPID_KEY:int,SVC_TUID:bigint,SVC_TRL:bigint,SVC_PRODUCT_CAT_NAME:string,SVC_PRODUCT_CAT_KEY:smallint,SVC_PRODUCT_CAT_ID:smallint,SVC_HOTEL_ID:bigint,SVC_LODG_PROPERTY_KEY:int,SVC_LODG_PROPERTY_NAME:string,SVC_PROPERTY_BRAND_NAME:string,SVC_PROPERTY_CITY_NAME:string,SVC_PROPERTY_CNTRY_CODE:string,SVC_PROPERTY_REGN_NAME:string,SVC_PROPERTY_PARNT_CHAIN_NAME:string,SVC_PROPERTY_TYP_NAME:string,SVC_BUSINESS_PARTNR_KEY:int,SVC_BUSINESS_PARTNR_NAME:string,SVC_ACTV_BUSINESS_PARTNR_IND:string,SVC_BUSINESS_PARTNR_MED_NAME:string,SVC_BUSINESS_PARTNR_MGMT_UNIT_NAME:string,SVC_BUSINESS_PARTNR_RPT_CO_NAME:string,SVC_BUSINESS_PARTNR_SEG_NAME:string,SVC_BUSINESS_PARTNR_SVC_BRAND_NAME:string,SVC_BUSINESS_PARTNR_SVC_CNTRY_NAME:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_DESC:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_NAME:string,SVC_BUSINESS_PARTNR_SYS_NAME:string,SVC_BUSINESS_PARTNR_TPID_NAME:string,SVC_MGMT_UNIT_KEY:int,SVC_MGMT_UNIT_NAME:string,SVC_MGMT_UNIT_LVL_1_NAME:string,SVC_MGMT_UNIT_LVL_2_NAME:string,SVC_MGMT_UNIT_LVL_3_NAME:string,SVC_MGMT_UNIT_LVL_4_NAME:string,SVC_MGMT_UNIT_LVL_5_NAME:string,SVC_MGMT_UNIT_LVL_6_NAME:string,SVC_MGMT_UNIT_LVL_7_NAME:string,SVC_MGMT_UNIT_LVL_8_NAME:string,SVC_MGMT_UNIT_LVL_9_NAME:string,SVC_MGMT_UNIT_LVL_10_NAME:string,SVC_MGMT_UNIT_LVL_11_NAME:string,SVC_MGMT_UNIT_LVL_12_NAME:string,SVC_MGMT_UNIT_LVL_13_NAME:string,SVC_MGMT_UNIT_LVL_14_NAME:string,SVC_ONLINE_OFFLN_IND_KEY:int,SVC_ONLINE_OFFLN_IND:string,SVC_PURCH_TRVL_ACCT_ID:string,SVC_PURCH_TRVL_ACCT_KEY:int,SVC_PURCH_TRVL_ACCT_FRST_NAME:string,SVC_PURCH_TRVL_ACCT_LAST_NAME:string,SVC_PURCH_TRVL_ACCT_EMAIL_ADDR:string,SVC_PURCH_TRVL_ACCT_ADDR_1:string,SVC_PURCH_TRVL_ACCT_ADDR_2:string,SVC_PURCH_TRVL_ACCT_CITY_NAME:string,SVC_PURCH_TRVL_ACCT_POSTAL_CODE:string,SVC_PURCH_TRVL_ACCT_STATE_PROVNC_NAME:string,SVC_PURCH_TRVL_ACCT_CNTRY_CODE:string,SVC_PURCH_TRVL_ACCT_CNTRY_NAME:string,SVC_PURCH_TRVL_ACCT_SUPER_REGN_NAME:string,SVC_PURCH_TRVL_ACCT_CREATE_DATE:string,SVC_BKG_TYP_CODE:string,SVC_BKG_TYP_NAME:string,SVC_BKG_TYP_BUSINESS_MODEL_NAME:string,SVC_AIRLN_CARRIER:string,SVC_AIRFARE_TYP_CODE:string,SVC_AIRFARE_TYP_NAME:string,SVC_FLGHT_SCHED_CHG_OPTN_IND:string,SVC_LOW_COST_CARRIER_IND:string,SVC_SECURE_FLGHT_IND:string,SVC_BRKAGE_CANDIDATE_IND:string,SVC_CUST_CASE_ID:string,SVC_CUST_SCORE_NAME:string,SVC_CUST_IDENTITY_VERIFY_IND:string,SVC_CREDT_EXPR_TYP_CODE:string,SVC_CREDT_EXPR_TYP_DESC:string,SVC_EARLY_CHK_OUT_IND:string,SVC_EXPE_REWARDS_IND:string,SVC_INTNT_NAME:string,SVC_INTNT_DESC:string,SVC_CAT_LVL_1_INTENT_NAME:string,SVC_CNCL_LVL_2_INTENT_CODE:string,SVC_CNCL_LVL_2_INTENT_NAME:string,SVC_CNCL_CHG_LVL_2_INTENT_CODE:string,SVC_CNCL_CHG_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_3_INTENT_NAME:string,SVC_COUPN_LVL_2_INTENT_CODE:string,SVC_COUPN_LVL_2_INTENT_NAME:string,SVC_COUPN_LVL_3_INTENT_CODE:string,SVC_COUPN_LVL_3_INTENT_NAME:string,SVC_CRUIS_AFFL_NAME:string,SVC_RECONFIRM_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_2_INTENT_CODE:string,SVC_REFUND_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_3_INTENT_CODE:string,SVC_REFUND_LVL_3_INTENT_NAME:string,SVC_REFUND_METHD_LVL_4_INTENT_NAME:string,SVC_REQST_LVL_2_INTENT_CODE:string,SVC_REQST_LVL_2_INTENT_NAME:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_CODE:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_NAME:string,SVC_CONSULT_LIST:array<struct<SVC_CONSULT_CASE_ESCALATE_ID:string,SVC_CONSULT_AGNT_ROLE_NAME:string,SVC_CONSULT_NOTE_TXT:string,SVC_CONSULT_REASN_NAME:string,SVC_CONSULT_VNDR_TYP_ID:int,SVC_CONSULT_VNDR_TYP_NAME:string,SVC_ESCALATE_TYP_ID:int,SVC_ESCALATE_TYP_NAME:string,SVC_ESCALATE_PARTY_TYP_NAME:string>>,BKG_CONF_ID:bigint,REFUND_APPRVR_FRST_NAME:string,REFUND_APPRVR_LAST_NAME:string,REFUND_APPRVR_TITLE_NAME:string,SVC_LOYLTY_POLICY_FIN_IMPACT_IND:string,SVC_LOYLTY_POLICY_NAME:string,SVC_LOYLTY_ACTIVITY_REVIEW_IND:string,SVC_LOYLTY_MEMBR_ID:bigint,SVC_CREATE_ORG_NAME:string,SVC_CREATE_DIV_NAME:string,SVC_CREATE_ORG_UNIT_NAME:string,SVC_CREATE_WRK_GRP_NAME:string,SVC_RESOLVE_ORG_NAME:string,SVC_RESOLVE_DIV_NAME:string,SVC_RESOLVE_ORG_UNIT_NAME:string,SVC_RESOLVE_WRK_GRP_NAME:string,SVC_S_CASE_CNT:int,SVC_E_CASE_CNT:int,SVC_O_CASE_CNT:int,SVC_ESCALATION_CASE_CNT:int,SVC_RESOLVE_SECOND_CNT:decimal(19,4),SVC_CREATE_AGNT_LOGIN_ID:string,SVC_CREATE_AGNT_KEY:int,SVC_CREATE_AGNT_EARNS_CMSN_IND:string,SVC_CREATE_AGNT_EMAIL_ADDR:string,SVC_CREATE_AGNT_EMP_TYP_NAME:string,SVC_CREATE_AGNT_FRST_NAME:string,SVC_CREATE_AGNT_MIDDL_NAME:string,SVC_CREATE_AGNT_LAST_NAME:string,SVC_CREATE_AGNT_KNWN_AS_NAME:string,SVC_CREATE_AGNT_HIRE_DATE:string,SVC_CREATE_AGNT_TERMNATN_DATE:string,SVC_CREATE_AGNT_JOB_TITLE_NAME:string,SVC_CREATE_AGNT_MGMT_DIV_NAME:string,SVC_CREATE_AGNT_MGR_FRST_NAME:string,SVC_CREATE_AGNT_MGR_LAST_NAME:string,SVC_CREATE_AGNT_PRIM_BUSINESS_GRP_NAME:string,SVC_CREATE_AGNT_PRIM_TYP_NAME:string,SVC_CREATE_AGNT_PROFCNCY_NAME:string,SVC_CREATE_AGNT_ROLE_NAME:string,SVC_CREATE_AGNT_PRIM_LANG_CODE:string,SVC_CREATE_AGNT_PRIM_LANG_NAME:string,SVC_CREATE_AGNT_SCNDRY_LANG_CODE:string,SVC_CREATE_AGNT_SCNDRY_LANG_NAME:string,SVC_CREATE_AGNT_TERTIARY_LANG_CODE:string,SVC_CREATE_AGNT_TERTIARY_LANG_NAME:string,SVC_CREATE_AGNT_VNDR_LOC_NAME:string,SVC_CREATE_AGNT_VNDR_NAME:string,SVC_CREATE_AGNT_HIRE_TENR_DAY:int,SVC_CREATE_AGNT_ROLE_TENR_DAY:int,SVC_CREATE_AGNT_SERVICE_CATEGORY_NAME:string,SVC_UPDATE_AGNT_LOGIN_ID:string,SVC_UPDATE_AGNT_KEY:int,SVC_RESOLVE_AGNT_LOGIN_ID:string,SVC_RESOLVE_AGNT_KEY:int,SVC_SOURCE_CREATE_DATETM:string,SVC_PST_CREATE_DATE_KEY:int,SVC_GMT_CREATE_DATE_KEY:int,SVC_AEST_CREATE_DATE_KEY:int,SVC_PST_CREATE_TM_KEY:int,SVC_GMT_CREATE_TM_KEY:int,SVC_AEST_CREATE_TM_KEY:int,SVC_SOURCE_UPDATE_DATETM:string,SVC_PST_UPDATE_DATE_KEY:int,SVC_GMT_UPDATE_DATE_KEY:int,SVC_AEST_UPDATE_DATE_KEY:int,SVC_PST_UPDATE_TM_KEY:int,SVC_GMT_UPDATE_TM_KEY:int,SVC_AEST_UPDATE_TM_KEY:int,SVC_SOURCE_RESOLVE_DATETM:string,SVC_PST_RESOLVE_DATE_KEY:int,SVC_GMT_RESOLVE_DATE_KEY:int,SVC_AEST_RESOLVE_DATE_KEY:int,SVC_PST_RESOLVE_TM_KEY:int,SVC_GMT_RESOLVE_TM_KEY:int,SVC_AEST_RESOLVE_TM_KEY:int,SVC_CASE_USER_EMAIL_ADDR:string,SVC_LANG_CODE:string,SVC_LANG_KEY:int,RESPONDENT_ID:string,SVC_GEN_LVL_2_INTENT_CODE:string,SVC_GEN_LVL_2_INTENT_NAME:string>>"
        },
        {
          "Name": "call_eval_detail",
          "Type": "array<struct<CALL_EVAL_ID:int,CALL_EVAL_SITE_ID:int,CALL_EVAL_QUESTN_ID:int,CALL_EVAL_ANSWR_ID:int,CALL_EVAL_CREATE_DATETM:string,SRC_DB_VERSION_NBR:int,CALL_EVAL_UPDATE_DATETM:string,PST_CALL_EVAL_CREATE_DATE_KEY:int,PST_CALL_EVAL_CREATE_DATE:string,GMT_CALL_EVAL_CREATE_DATE_KEY:int,GMT_CALL_EVAL_CREATE_DATE:string,PST_CALL_EVAL_UPDATE_DATE_KEY:int,PST_CALL_EVAL_UPDATE_DATE:string,GMT_CALL_EVAL_UPDATE_DATE_KEY:int,GMT_CALL_EVAL_UPDATE_DATE:string,CALL_EVAL_AGNT_NICE_USER_ID:int,CALL_EVAL_AGNT_PERIPH_NBR:array<string>,CALL_EVAL_SEG_ID:string,CALL_EVAL_CASE_NBR:string,CALL_EVAL_ITIN_NBR:string,CALL_EVAL_SEG_DURATN_TM:string,CUST_OPS_CALL_EVAL_QUESTN_KEY:int,CALL_EVAL_QUESTN_SECTN_NAME:string,CALL_EVAL_QUESTN_SUB_SECTN_NAME:string,CALL_EVAL_QUESTN_CALL_OUTCOME:string,CALL_EVAL_QUESTN_SUB_SECTN_NBR:string,CALL_EVAL_QUESTN_NBR:string,CALL_EVAL_QUESTN_LBL_NAME:string,CALL_EVAL_QUESTN_CAPTN_NAME:string,CALL_EVAL_QUESTN_HIER_NBR:string,CALL_EVAL_QUESTN_TYP_ID:int,CALL_EVAL_QUESTN_SCORABLE_NBR:int,CALL_EVAL_QUESTN_IS_FATAL_NBR:int,CUST_OPS_CALL_EVAL_ANSWR_KEY:int,CALL_EVAL_ANSWR_SHORT_REASN_NAME:string,CALL_EVAL_ANSWR_DETAIL_REASN_NAME:string,CUST_OPS_CALL_EVAL_FORM_KEY:int,CALL_EVAL_FORM_ID:int,CALL_EVAL_FORM_TYP_NAME:string,CALL_EVAL_FORM_SRC_NAME:string,CALL_EVAL_FORM_NAME:string,CALL_EVAL_FORM_CREATE_DATETM:string,CALL_EVAL_FORM_UPDATE_DATETM:string,CALL_EVAL_FORM_ENABL_IND:string,CUST_OPS_CALL_EVALUATOR_KEY:int,CALL_EVAL_EVALUATOR_USER_ID:int,CALL_EVALUATOR_FULL_NAME:string,CALL_EVALUATOR_LOCATION_NAME:string,CUST_OPS_CALL_EVAL_TYP_KEY:int,CALL_EVAL_TYP_ID:int,CALL_EVAL_TYP_NAME:string,CUST_OPS_CALL_EVAL_AUTOFAIL_ANSWR_IND_KEY:int,CALL_EVAL_AUTOFAIL_ANSWR_IND:string,CALL_EVAL_AUTOFAIL_ANSWR_CNT:int,CALL_EVAL_ANSWR_CNT:int,POSSIBLE_POINT_NBR:decimal(19,4),EARN_POINT_NBR:decimal(19,4),ADJUSTED_POSSIBLE_POINT_CNT:decimal(19,4),ADJUSTED_EARN_POINT_CNT:decimal(19,4)>>"
        },
        {
          "Name": "cvp_call_guid",
          "Type": "string"
        },
        {
          "Name": "cvp_ab_test_ind",
          "Type": "string"
        },
        {
          "Name": "cvp_call_start_date",
          "Type": "string"
        },
        {
          "Name": "cvp_call_pst_start_date_key",
          "Type": "int"
        },
        {
          "Name": "cvp_call_gmt_start_date_key",
          "Type": "int"
        },
        {
          "Name": "cvp_call_start_datetm",
          "Type": "string"
        },
        {
          "Name": "cvp_call_end_datetm",
          "Type": "string"
        },
        {
          "Name": "cvp_call_time_zone_key",
          "Type": "int"
        },
        {
          "Name": "cvp_call_time_zone",
          "Type": "string"
        },
        {
          "Name": "cvp_call_ani",
          "Type": "string"
        },
        {
          "Name": "cvp_cust_ops_phon_assign_key",
          "Type": "int"
        },
        {
          "Name": "cvp_call_dnis",
          "Type": "string"
        },
        {
          "Name": "cvp_call_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_auto_ani_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_manual_ani_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_auto_itin_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_manual_itin_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_override_itin_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_hang_up_time_brct_0_10_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_hang_up_time_brct_11_20_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_hang_up_time_brct_21_30_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_hang_up_time_brct_31_up_cnt",
          "Type": "int"
        },
        {
          "Name": "cvp_elemnt_list",
          "Type": "array<struct<CVP_SESSN_ID:bigint,CVP_SESSN_APP_KEY:int,CVP_SESSN_APP_NAME:string,CVP_SESSN_START_DATETM:string,CVP_SESSN_END_DATETM:string,CVP_SESSN_PRODUCT:string,CVP_SESSN_PRODUCT_CAT_KEY:int,CVP_SESSN_TRANS_TYP_CODE:string,CVP_SESSN_TRANS_TYP_KEY:int,CVP_SESSN_TRANS_TYP_NAME:string,CVP_SESSN_CALLER_NEED_CODE:string,CVP_SESSN_CALLER_NEED_KEY:int,CVP_SESSN_CALLER_NEED_NAME:string,CVP_SESSN_LANG_KEY:int,CVP_SESSN_LANG_CODE:string,CVP_SESSN_PRIME_LANG_CNT:bigint,CVP_SESSN_NON_PRIME_LANG_CNT:bigint,CVP_SESSN_BUSINESS_PARTNR_KEY:bigint,CVP_SESSN_BUSINESS_PARTNR_NAME:string,CVP_SESSN_DNIS_NAME:string,CVP_SESSN_ITIN:string,CVP_ELEMNT_ID:bigint,CVP_ELEMNT_KEY:int,CVP_ELEMNT_NAME:string,CVP_ELEMNT_START_DATETM:string,CVP_ELEMNT_END_DATETM:string,CVP_ELEMNT_NUM_INTERACT_CNT:int,CVP_ELEMNT_RESLT:int,CVP_ELEMNT_EXIT_STATE:string,CVP_LODG_CNCL_ELIG_CNT:int,CVP_LODG_CNCL_SUCCSS_CNT:int,CVP_LODG_CNCL_FAIL_CNT:int,CVP_CNCL_SECURE_VALID_METHOD_NAME:string,CVP_CNCL_SECURE_VALID_ZIP_CNT:int,CVP_CNCL_SECURE_VALID_LAST_FOUR_CNT:int,CVP_CNCL_SECURE_VALID_FAIL_CNT:int,CVP_CNCL_TERM_ACCPT_CNT:int,CVP_CNCL_TERM_DECLN_CNT:int,CVP_ANYTHING_ELSE_HUNG_UP_CNT:int,CVP_ANYTHING_ELSE_NEW_CNT:int,CVP_ANYTHING_ELSE_MAIN_MENU_CNT:int,CVP_PHON_NBR_PROMPT_CNT:int,CVP_PHON_FROM_ITIN_LKUP_CNT:int,CVP_ITIN_PROMPT_CNT:int,CVP_ITIN_LKUP_SUCCESS_CNT:int,CVP_ITIN_LKUP_FAIL_CNT:int,CVP_ITIN_SELCT_DIFF_CNT:int,CVP_BKG_MENU_NO_INPUT_CNT:int,CVP_BKG_MENU_HUNG_UP_CNT:int,CVP_ELEMNT_OPT_OUT_CNT:int,CVP_ELEMNT_HANG_UP_CNT:int,CVP_ELEMNT_DURATION:bigint,CVP_ELEMNT_REPT_CNT:int,CVP_ELEMNT_MAX_NO_INPUT_CNT:int,CVP_ELEMNT_DTFM_INPUT_CNT:bigint,CVP_ELEMNT_VOICE_INPUT_CNT:bigint,CVP_ELEMNT_UTTERANCE:string,CVP_ELEMNT_INTERPRETATION:string,CVP_ELEMNT_VOICE_CONFIDENCE_PCT:string,CVP_ELEMNT_NO_INPUT_CNT:int,CVP_ELEMNT_REPEAT_TERMINATE_CNT:int>>"
        },
        {
          "Name": "respondent_id",
          "Type": "string"
        },
        {
          "Name": "load_tag",
          "Type": "bigint"
        },
        {
          "Name": "call_service_agnt_typ",
          "Type": "string"
        },
        {
          "Name": "call_agent_tier_name",
          "Type": "string"
        },
        {
          "Name": "caller_service_type",
          "Type": "string"
        },
        {
          "Name": "caller_loyalty_name",
          "Type": "string"
        },
        {
          "Name": "nav_int_create_agnt_service_tier_name",
          "Type": "string"
        }
      ],
      "Location": "s3://apiary-classic-387910953075-us-east-1-onprem-conversation/hadoop_dm_cust_ops_call_bkg_detail/cust_ops_call_src_sys_name=BKG/partition_day=2013-01-01",
      "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
      "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
      "Compressed": false,
      "NumberOfBuckets": -1,
      "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "Parameters": {
          "serialization.format": "1",
          "hive.serialization.extend.nesting.levels": "true",
          "hive.serialization.extend.additional.nesting.levels": "true"
        }
      },
      "SortColumns": [],
      "StoredAsSubDirectories": false
    },
    "Parameters": {},
    "CatalogId": "387910953075"
  }
}
```

# Partition in hive

```
hive> DESCRIBE FORMATTED onprem_conversation.hadoop_dm_cust_ops_call_bkg_detail PARTITION (cust_ops_call_src_sys_name='BKG', partition_day='2013-01-01');
OK
# col_name            	data_type           	comment

cust_ops_call_id    	string
cust_ops_call_seg_seq_nbr	int
trans_date_key      	int
trans_agnt_key      	int
agnt_skill_target_id	int
router_call_day_id  	int
router_call_id      	int
src_call_start_datetm	string
src_call_end_datetm 	string
aest_call_start_date_key	int
gmt_call_start_date_key	int
pst_call_start_date_key	int
pst_call_end_date_key	int
pst_call_start_tm_key	int
pst_call_end_tm_key 	int
agnt_periph_nbr     	string
call_itin_nbr       	string
inbnd_dialed_nbr    	string
outbnd_dialed_nbr   	string
ani_nbr             	string
cust_ops_ngcc_agnt_sessn_id	string
cust_ops_ngcc_media_id	string
orignl_ani_nbr      	string
orignl_cust_ops_ngcc_cntct_id	string
vq_inbnd_call_id    	string
vq_return_call_id   	string
cust_ops_agnt_key   	int
cust_ops_agnt_id    	int
call_agnt_frst_name 	string
call_agnt_middl_name	string
call_agnt_last_name 	string
call_agnt_job_title_name	string
call_agnt_hire_date 	string
call_agnt_hire_tenr_day	int
call_agnt_termnatn_date	string
call_agnt_vndr_loc_id	int
call_agnt_vndr_loc_name	string
call_agnt_vndr_id   	smallint
call_agnt_vndr_name 	string
call_agnt_role_id   	smallint
call_agnt_role_name 	string
call_agnt_role_tenr_day	int
call_agnt_prim_cust_ops_typ_id	smallint
call_agnt_prim_cust_ops_typ_name	string
call_agnt_profcncy_id	smallint
call_agnt_profcncy_name	string
call_agnt_mgr_frst_name	string
call_agnt_mgr_last_name	string
call_agnt_full_name 	string
call_agnt_mgr_full_name	string
call_agnt_typ_id    	int
call_agnt_typ_name  	string
call_business_partnr_key	int
call_business_partnr_id	int
call_business_partnr_sys_name	string
call_expe_business_partnr_id	int
call_ian_business_partnr_id	int
call_as400_business_partnr_src_code	string
call_business_partnr_name	string
call_business_partnr_tpid	int
call_business_partnr_tpid_name	string
call_business_partnr_svc_brand_name	string
call_business_partnr_svc_short_cntry_code	string
call_business_partnr_svc_cntry_name	string
call_business_partnr_svc_super_regn_name	string
call_business_partnr_svc_super_regn_desc	string
call_actv_business_partnr_ind	string
call_business_partnr_acct_mgr_name	string
call_business_partnr_b2b_billng_typ_name	string
call_business_partnr_co_name	string
call_business_partnr_home_url	string
call_business_partnr_med_name	string
call_business_partnr_mgmt_unit_code	string
call_business_partnr_mgmt_unit_name	string
call_business_partnr_oper_regn_name	string
call_business_partnr_rpt_co_code	string
call_business_partnr_rpt_co_name	string
call_business_partnr_seg_name	string
call_business_partnr_site_platform_name	string
call_business_partnr_start_date	string
call_business_partnr_website_domain_name	string
call_parnt_business_partnr_id	int
call_parnt_business_partnr_name	string
call_tpid_key       	int
call_tpid           	int
call_tpid_name      	string
call_tpid_cntry_code	string
call_tpid_cntry_name	string
call_tpid_hwire_pos_code	string
call_mgmt_unit_key  	smallint
call_mgmt_unit_code 	string
call_mgmt_unit_name 	string
call_mgmt_unit_lvl_1_name	string
call_mgmt_unit_lvl_2_name	string
call_mgmt_unit_lvl_3_name	string
call_mgmt_unit_lvl_4_name	string
call_mgmt_unit_lvl_5_name	string
call_mgmt_unit_lvl_6_name	string
call_mgmt_unit_lvl_7_name	string
call_mgmt_unit_lvl_8_name	string
call_mgmt_unit_lvl_9_name	string
call_mgmt_unit_lvl_10_name	string
call_mgmt_unit_lvl_11_name	string
call_mgmt_unit_lvl_12_name	string
call_mgmt_unit_lvl_13_name	string
call_mgmt_unit_lvl_14_name	string
call_lang_key       	int
call_lang_code      	string
call_lang_name      	string
call_product_cat_key	smallint
call_product_cat_name	string
call_service_type_name	string
call_service_category_name	string
cust_ops_call_typ_key	int
cust_ops_call_typ_id	int
call_typ_code_list  	string
call_typ_desc       	string
call_typ_caller_need_id	smallint
call_typ_caller_need_code	string
call_typ_caller_need_name	string
call_typ_cust_ops_product_typ_id	smallint
call_typ_cust_ops_product_typ_code	string
call_typ_cust_ops_product_typ_name	string
call_typ_cust_trans_typ_id	smallint
call_typ_cust_trans_typ_code	string
call_typ_cust_trans_typ_name	string
call_typ_call_seg_grp_code	string
call_typ_call_seg_grp_name	string
call_typ_phon_site_chnnl_plcmnt_code	string
call_typ_phon_site_chnnl_plcmnt_name	string
call_typ_route_typ_code	string
call_typ_staff_grp_code	string
cust_ops_skill_grp_key	int
skill_grp_id        	int
skill_grp_code_list 	string
skill_grp_desc      	string
base_skill_grp_caller_need_id	smallint
base_skill_grp_caller_need_code	string
base_skill_grp_caller_need_name	string
base_skill_grp_cust_ops_product_typ_id	smallint
base_skill_grp_cust_ops_product_typ_code	string
base_skill_grp_cust_ops_product_typ_name	string
base_skill_grp_cust_trans_typ_id	smallint
base_skill_grp_cust_trans_typ_code	string
base_skill_grp_cust_trans_typ_name	string
base_skill_grp_call_seg_grp_code	string
base_skill_grp_call_seg_grp_name	string
base_skill_grp_desc 	string
base_skill_grp_code_list	string
base_skill_grp_phon_site_chnnl_plcmnt_code	string
base_skill_grp_phon_site_chnnl_plcmnt_name	string
base_skill_grp_staff_grp_code	string
frcast_grp_call_seg_grp_code	string
frcast_grp_call_seg_grp_name	string
frcast_grp_caller_need_code	string
frcast_grp_caller_need_name	string
frcast_grp_cust_ops_product_typ_code	string
frcast_grp_cust_ops_product_typ_name	string
frcast_grp_cust_trans_typ_code	string
frcast_grp_cust_trans_typ_name	string
frcast_grp_lang_code	string
frcast_grp_lang_name	string
skill_grp_profcncy_code	string
cust_ops_icrs_call_periph_dispostn_typ_key	int
cust_ops_icrs_periph_call_typ_id	int
cust_ops_icrs_call_dispostn_typ_id	int
periph_call_typ_name	string
call_dispostn_typ_name	string
call_dispostn_name  	string
call_sys_err        	string
call_sys_gb         	string
call_sys_partval    	string
call_sys_ref_id     	string
call_sys_lodg_property_name	string
call_typ_var_cust_ops_brand_name	string
call_typ_var_bkg_windw_name	string
call_typ_var_caller_need_code	string
call_typ_var_caller_need_name	string
call_typ_var_intl_dom_code	string
call_typ_var_intl_dom_name	string
call_typ_var_cust_ops_product_typ_code	string
call_typ_var_cust_ops_product_typ_name	string
call_typ_var_cust_trans_typ_code	string
call_typ_var_cust_trans_typ_name	string
call_typ_var_phon_site_chnnl_plcmnt_code	string
call_typ_var_phon_site_chnnl_plcmnt_name	string
call_typ_var_cust_ops_pos_code	string
call_typ_var_cust_ops_pos_name	string
call_typ_var_cust_ops_sub_brand_name	string
call_typ_var_trvl_duratn_code	string
call_typ_var_trvl_duratn_name	string
call_exprnce_var_intfc_fails	string
call_exprnce_var_lang_list	string
call_sys_var_media_publctn_id	string
call_exprnce_var_persna	string
target_seg_call_typ_key	int
target_seg_call_typ_id	int
target_seg_call_typ_code_list	string
target_seg_call_typ_desc	string
target_seg_call_typ_caller_need_id	smallint
target_seg_call_typ_caller_need_code	string
target_seg_call_typ_caller_need_name	string
target_seg_call_typ_cust_ops_product_typ_id	smallint
target_seg_call_typ_cust_ops_product_typ_code	string
target_seg_call_typ_cust_ops_product_typ_name	string
target_seg_call_typ_cust_trans_typ_id	smallint
target_seg_call_typ_cust_trans_typ_code	string
target_seg_call_typ_cust_trans_typ_name	string
target_seg_call_typ_call_seg_grp_code	string
target_seg_call_typ_call_seg_grp_name	string
target_seg_call_typ_phon_site_chnnl_plcmnt_code	string
target_seg_call_typ_phon_site_chnnl_plcmnt_name	string
target_seg_call_typ_route_typ_code	string
target_seg_call_typ_staff_grp_code	string
call_seg_state_ind  	string
vq_call_ind         	string
vq_call_state_ind   	string
cust_ops_phon_assign_key	int
cust_ops_phon_assign_id	int
cust_ops_phon_id    	int
phon_name           	string
cust_ops_phon_typ_id	smallint
cust_ops_phon_typ_name	string
phon_carrier_id     	smallint
phon_carrier_name   	string
phon_cntry_prfx_nbr 	string
phon_cntry_name     	string
cust_ops_phon_regn_id	smallint
cust_ops_phon_regn_name	string
phon_vanity_desc    	string
phon_local_nbr      	string
intl_phon_nbr       	string
rcf_phon_nbr        	string
rcf_phon_carrier_id 	smallint
rcf_phon_carrier_name	string
phon_acquir_date    	string
phon_retire_date    	string
phon_carrier_start_datetm	string
phon_carrier_end_datetm	string
phon_carrier_acct_nbr	string
phon_assign_format_phon_txt	string
phon_assign_business_partnr_key	int
phon_assign_start_datetm	string
phon_assign_end_datetm	string
phon_assign_parnt_phon_id	int
phon_assign_cust_ops_branding_cat_id	smallint
phon_assign_cust_ops_branding_cat_name	string
phon_assign_cust_ops_brand_lvl_1_id	smallint
phon_assign_cust_ops_brand_lvl_1_name	string
phon_assign_cust_ops_brand_lvl_2_id	smallint
phon_assign_cust_ops_brand_lvl_2_name	string
phon_assign_cust_ops_brand_lvl_3_id	smallint
phon_assign_cust_ops_brand_lvl_3_name	string
phon_assign_cust_ops_pos_id	smallint
phon_assign_cust_ops_pos_code	string
phon_assign_cust_ops_pos_desc	string
phon_assign_iso_lang_code	string
phon_assign_iso_lang_name	string
phon_assign_cust_ops_product_typ_id	smallint
phon_assign_cust_ops_product_typ_name	string
phon_assign_mktg_chnnl_name_1	string
phon_assign_mktg_chnnl_name_2	string
phon_assign_mktg_cmpgn_lvl_1_id	smallint
phon_assign_mktg_cmpgn_lvl_1_name	string
phon_assign_mktg_cmpgn_lvl_2_id	smallint
phon_assign_mktg_cmpgn_lvl_2_name	string
phon_assign_media_typ_lvl_1_id	smallint
phon_assign_media_typ_lvl_1_name	string
phon_assign_media_typ_lvl_2_id	smallint
phon_assign_media_typ_lvl_2_name	string
phon_assign_media_typ_lvl_3_id	smallint
phon_assign_media_typ_lvl_3_name	string
phon_assign_publctn_typ_lvl_1_id	smallint
phon_assign_publctn_typ_lvl_1_name	string
phon_assign_publctn_typ_lvl_2_id	smallint
phon_assign_publctn_typ_lvl_2_name	string
phon_assign_site_plcmnt_lvl_1_id	smallint
phon_assign_site_plcmnt_lvl_1_name	string
phon_assign_site_plcmnt_lvl_2_id	smallint
phon_assign_site_plcmnt_lvl_2_name	string
phon_assign_site_chnnl_plcmnt_id	smallint
phon_assign_site_chnnl_plcmnt_code	string
phon_assign_site_chnnl_plcmnt_name	string
phon_assign_mktg_effrt_desc	string
phon_assign_srch_cat_id	smallint
phon_assign_srch_cat_name	string
phon_assign_srch_term_desc	string
phon_assign_holder_id	smallint
phon_assign_holder_frst_name	string
phon_assign_holder_last_name	string
phon_assign_desc    	string
phon_assign_creative_id	smallint
phon_assign_creative_name	string
phon_assign_call_to_actn_id	smallint
phon_assign_call_to_actn_name	string
phon_assign_ivr_exprnce_id	int
phon_assign_ivr_exprnce_code_list	string
phon_assign_ivr_exprnce_desc	string
phon_assign_hrs_of_operatn_desc	string
phon_assign_cost_per_minute_desc	string
phon_assign_prk_until_datetm	string
phon_assign_rpt_dnp_ind	string
cust_ops_call_src_tm_zone_key	int
cust_ops_call_src_tm_zone_id	int
cust_ops_call_src_tm_zone_name	string
cust_ops_call_duratn_windw_key	int
cust_ops_ngcc_cntct_dirctn_key	int
cust_ops_ngcc_dirctn_id	int
ngcc_cntct_dirctn_name	string
cust_ops_ngcc_cntct_disconnect_typ_key	int
cust_ops_ngcc_cntct_disconnect_reasn_id	int
ngcc_cntct_disconnect_typ_name	string
cust_ops_ngcc_dispostn_typ_id	int
ngcc_cntct_dispostn_typ_name	string
agnt_handled_ind    	string
cust_disconnect_ind 	string
handled_wthn_sla_ind	string
ivr_handled_ind     	string
outbnd_ind          	string
cust_ops_ngcc_comptncy_grp_key	int
cust_ops_ngcc_comptncy_grp_id	int
agnt_ngcc_comptncy_grp_name	string
short_call_ind      	string
zero_talk_tm_ind    	string
agnt_disconnect_cnt 	int
attch_outbnd_cnt    	int
handle_cnt          	int
unattch_outbnd_cnt  	int
aftr_call_wrk_second_cnt	int
attch_outbnd_second_cnt	int
hold_second_cnt     	int
talk_second_cnt     	int
totl_handle_second_cnt	int
unattch_outbnd_second_cnt	int
hold_call_ind       	string
system_disconnect_cnt	int
abandon_second_cnt  	int
netwrk_abandon_second_cnt	int
trnsfr_abandon_second_cnt	int
totl_offr_cnt       	int
inbnd_queue_offr_cnt	int
transfr_queue_offr_cnt	int
answr_second_cnt    	int
netwrk_answr_second_cnt	int
trnsfr_answr_second_cnt	int
answr_20_second_call_ind	string
answr_30_second_call_ind	string
answr_60_second_call_ind	string
answr_120_second_call_ind	string
agnt_conect_attmpt_cnt	int
agnt_trnsfr_cnt     	int
arrv_cnt            	int
block_call_cnt      	int
cntct_cnt           	int
hold_abandon_cnt    	int
inbnd_cnt           	int
ivr_second_cnt      	int
othr_err_cnt        	int
queue_abandon_cnt   	int
retry_agnt_cnt      	int
sys_terminate_cnt   	int
sys_trnsfr_cnt      	int
transfr_init_cnt    	int
arrv_second_cnt     	int
duratn_second_cnt   	int
hang_up_second_cnt  	int
ivr_queue_second_cnt	int
ring_second_cnt     	int
totl_wrap_up_second_cnt	int
terminate_wrap_up_second_cnt	int
transfr_queue_second_cnt	int
agnt_disconnect_short_call_cnt	int
short_call_cnt      	int
zero_talk_cnt       	int
hold_call_cnt       	int
answr_20_second_call_cnt	int
answr_30_second_call_cnt	int
answr_60_second_call_cnt	int
answr_120_second_call_cnt	int
tier1_to_tier2_cnt  	int
ivr_terminate_cnt   	int
netwrk_handle_call_cnt	int
netwrk_totl_handle_tm	int
netwrk_handle_talk_tm	int
netwrk_handle_hold_tm	int
netwrk_aftr_call_wrk_tm	int
trnsfr_handle_call_cnt	int
trnsfr_totl_handle_tm	int
trnsfr_handle_talk_tm	int
trnsfr_handle_hold_tm	int
trnsfr_aftr_call_wrk_tm	int
agnt_disconnect_ind 	string
ivr_delay_second_cnt	int
gmt_trans_date_key  	int
pst_trans_date_key  	int
prim_purch_trvl_acct_key	int
gross_trans_cnt     	int
gross_bkg_amt_usd   	decimal(19,4)
gross_purch_price_amt_usd	decimal(19,4)
gross_purch_cost_amt_usd	decimal(19,4)
gross_cncl_price_amt_usd	decimal(19,4)
gross_cncl_cost_amt_usd	decimal(19,4)
totl_cost_amt_usd   	decimal(19,4)
margn_amt_usd       	decimal(19,4)
gross_purch_ordr_cnt	int
gross_purch_trans_cnt	int
gross_cncl_ordr_cnt 	int
gross_cncl_trans_cnt	int
gross_ordr_cnt      	int
gross_agncy_trans_cnt	int
gross_merch_trans_cnt	int
cust_ops_est_bk_rev_amt_usd	decimal(19,4)
itin_detail         	array<struct<SRC_SYS_ID:int,TPID:int,TRL:int,PRODUCT_CAT_KEY:int,PRODUCT_LN_NAME:string,RESPONDENT_ID:string,BUSINESS_PARTNR_KEY:int,ITIN_NBR:string,ORDER_NBR:bigint,ITIN_BK_AGNT_KEY:int,ITIN_BK_AGNT_HIRE_TENR_DAY:int,ITIN_BK_AGNT_ROLE_TENR_DAY:int,BK_AGNT_KEY:int,BK_DATE_KEY:int,AB_TST_GRP_ID:int,BEGIN_USE_DATE_KEY:int,BEGIN_USE_DATE:string,BK_DATE:string,BK_DATETM:string,BKG_IND_KEY:int,PKG_BKG_IND_KEY:int,BKG_PRODUCT_LN_COMPONENT_KEY:int,BKG_WINDW_KEY:int,COST_CURRN_KEY:int,COUPN_KEY:int,CUST_OPS_BKG_IND_KEY:int,END_USE_DATE_KEY:int,END_USE_DATE:string,LGL_ENTITY_KEY:int,MKTG_CODE_KEY:int,ORACLE_GL_PRODUCT_KEY:int,PRICE_CURRN_KEY:int,PRODUCT_LN_KEY:int,PST_TRANS_DATE_KEY:int,PST_TRANS_DATE:string,PST_TRANS_TM_KEY:int,AIR_TRIP_TYP_NAME:string,SAT_NIGHT_STAY_IND:string,AIR_BKG_IND_KEY:int,AIR_SETTLMNT_AGNT_TYP_KEY:int,PLATNG_CARRIER_KEY:int,PNR_REC_LOCATOR_CODE:string,TCKT_AIR_FARE_TYP_KEY:int,TCKT_ROUTE_KEY:int,TOUR_OPERATR_KEY:int,AGNT_ASST_IND:string,BKG_WINDW_RNG_NAME:string,CAR_CAT_NAME:string,CAR_TYP_NAME:string,CREDT_CARD_TYP_KEY:int,CAR_BASE_PRICE_PERIOD_KEY:int,CAR_BKG_IND_KEY:int,CAR_CLASS_KEY:int,CAR_DROP_OFF_LOC_KEY:int,CAR_PICK_UP_LOC_KEY:int,CAR_SPCL_EQUIP_GRP_KEY:int,CAR_VNDR_AGRMNT_KEY:int,CAR_VNDR_KEY:int,CRUIS_ADJ_REASN_KEY:int,CRUIS_CABN_TYP_KEY:int,CRUIS_RSDNC_STATE_PROVNC_KEY:int,CRUIS_SUB_DEST_KEY:int,DISEMBRK_PORT_KEY:int,EMBRK_PORT_KEY:int,SHIP_KEY:int,AGNT_TOUCH_IND:string,OFFRNG_ITM_KEY:int,DEST_SVC_SRCH_LOC_KEY:int,DEST_REGN_KEY:int,INS_OFFRNG_CAT_KEY:int,INS_OFFRNG_CAT_NAME:string,LENGTH_OF_STAY_RNG_NAME:string,EEM_PROPERTY_IND:string,EXPE_HALF_STAR_RTG:decimal(19,4),GDS_PROPERTY_IND:string,LODG_PROPERTY_NAME:string,MERCH_PROPERTY_IND:string,OPAQUE_PROPERTY_IND:string,PROPERTY_BRAND_NAME:string,PROPERTY_CNTRCT_MODEL_NAME:string,PROPERTY_CNTRY_NAME:string,PROPERTY_PARNT_CHAIN_NAME:string,PROPERTY_MKT_NAME:string,BKG_REFRL_SRC_KEY:int,DISTR_KEY:int,DOM_INTL_BKG_ITM_IND_KEY:int,LENGTH_OF_STAY_KEY:int,LODG_PROPERTY_KEY:int,LODG_RATE_PLN_KEY:int,LODG_RATE_RULE_KEY:int,ORDER_CONF_NBR:string,PRICE_STRUCT_KEY:int,TPID_CURRN_KEY:int,TRANS_TYP_KEY:int,TRVL_DURATN_KEY:int,OFFRNG_ITM_NAME:string,OFFRNG_NAME:string,PKG_BEGIN_USE_DATE:string,PKG_CAR_VNDR_1_KEY:int,PKG_CAR_VNDR_2_KEY:int,PKG_END_USE_DATE:string,PKG_LODG_PROPERTY_1_KEY:int,PKG_LODG_PROPERTY_2_KEY:int,PRICE_MODEL_NAME:string,FLEX_MOR_IND:string,PKG_TYP_NAME:string,TRANS_CAT_NAME:string,TRANS_TYP_DESC:string,TRANS_TYP_ID:int,TRANS_TYP_NAME:string,TRANS_USE_PERIOD_NAME:string,COST_CURRN_NAME:string,DISEMBRK_PORT_NAME:string,EMBRK_PORT_NAME:string,PLATNG_CARRIER_NAME:string,TCKT_DEST_AIRPT_CODE:string,TCKT_DEST_AIRPT_CNTRY_CODE:string,TCKT_DEST_AIRPT_CNTRY_NAME:string,TCKT_ORIGN_AIRPT_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_NAME:string,TCKT_ROUTE_NAME:string,BUSINESS_MODEL_NAME:string,BUSINESS_MODEL_SUBTYP_NAME:string,PKG_IND:string,BKG_ID:int,BKG_ITM_ID:int,BKG_SYS_OF_REC_ID:int,BKG_SYS_OF_REC_NAME:string,ITIN_CREATE_DATETM:string,ORDER_ID:bigint,ORDER_LN_SEQ_NBR:int,PROPERTY_LOCAL_BK_DATE_KEY:int,PROPERTY_LOCAL_BK_TM_KEY:int,PST_ITIN_CREATE_DATE:string,PST_ITIN_CREATE_DATE_KEY:int,PST_ITIN_CREATE_TM_KEY:int,TRANS_TM_KEY:int,PKG_ID:bigint,PKG_TRANS_TYP_KEY:int,BK_LANG_KEY:int,BK_LANG_CODE:string,BK_LANG_NAME:string,BK_GROSS_TRANS_CNT:int,BK_GROSS_BKG_AMT_USD:decimal(19,4),BK_GROSS_PURCH_PRICE_AMT_USD:decimal(19,4),BK_GROSS_PURCH_COST_AMT_USD:decimal(19,4),BK_GROSS_CNCL_PRICE_AMT_USD:decimal(19,4),BK_GROSS_CNCL_COST_AMT_USD:decimal(19,4),BK_TOTL_COST_AMT_USD:decimal(19,4),BK_MARGN_AMT_USD:decimal(19,4),BK_GROSS_PURCH_ORDR_CNT:int,BK_GROSS_PURCH_TRANS_CNT:int,BK_GROSS_CNCL_ORDR_CNT:int,BK_GROSS_CNCL_TRANS_CNT:int,BK_GROSS_ORDR_CNT:int,BK_GROSS_AGNCY_TRANS_CNT:int,BK_GROSS_MERCH_TRANS_CNT:int,BK_CUST_OPS_EST_BK_REV_AMT_USD:decimal(19,4),COUPN_PRICE_AMT_USD:decimal(19,4),EST_COST_OF_SALE_AMT_USD:decimal(19,4),EST_GROSS_PROFIT_AMT_USD:decimal(19,4),EST_NET_REV_AMT_USD:decimal(19,4),EST_VAR_COST_OF_SALE_AMT_USD:decimal(19,4),EST_VAR_GROSS_PROFIT_AMT_USD:decimal(19,4),FRNT_END_CMSN_AMT_USD:decimal(19,4),OTHR_COST_ADJ_AMT_USD:decimal(19,4),OTHR_DEST_SVC_TCKT_CNT:int,OTHR_FEE_COST_AMT_USD:decimal(19,4),OTHR_FEE_PRICE_AMT_USD:decimal(19,4),ADULT_CNT:int,AGNT_ASST_PURCH_FEE_AMT_USD:decimal(19,4),AGNT_TOUCH_CNT:int,BASE_COST_AMT_USD:decimal(19,4),BASE_PRICE_AMT_USD:decimal(19,4),AGNT_ASST_EXCH_FEE_AMT_USD:decimal(19,4),AGNT_ASST_REFUND_FEE_AMT_USD:decimal(19,4),AGNT_ASST_VOID_FEE_AMT_USD:decimal(19,4),BKG_FEE_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_COST_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_PRICE_AMT_USD:decimal(19,4),DELIVERY_FEE_COST_AMT_USD:decimal(19,4),DELIVERY_FEE_PRICE_AMT_USD:decimal(19,4),EXCH_PNLTY_COST_AMT_USD:decimal(19,4),EXCH_PNLTY_PRICE_AMT_USD:decimal(19,4),LAP_INFANT_CNT:int,PAPR_TCKT_FEE_AMT_USD:decimal(19,4),UNUSED_TCKT_COST_AMT_USD:decimal(19,4),UNUSED_TCKT_PRICE_AMT_USD:decimal(19,4),AIR_TRANS_SEG_CNT:int,AIR_TRANS_TCKT_CNT:int,PEAK_RATE_COST_ADJ_AMT_USD:decimal(19,4),PEAK_RATE_PRICE_ADJ_AMT_USD:decimal(19,4),OTHR_PRICE_ADJ_AMT_USD:decimal(19,4),PURE_MARGN_AMT_USD:decimal(19,4),RENTL_DAY_CNT:int,VAR_PRICE_ADJ_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_COST_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_CMSN_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_LODG_CMSN_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_COST_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_COST_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_CMSN_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_COST_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_PRICE_AMT_USD:decimal(19,4),PORT_CHRG_COST_AMT_USD:decimal(19,4),PORT_CHRG_PRICE_AMT_USD:decimal(19,4),SENIOR_CNT:int,OTHR_TAX_COST_AMT_USD:decimal(19,4),OTHR_TAX_PRICE_AMT_USD:decimal(19,4),NET_RETAIL_RATE_AMT_USD:decimal(19,4),MARKUP_AMT_USD:decimal(19,4),ADULT_DEST_SVC_TCKT_CNT:int,CHILD_DEST_SVC_TCKT_CNT:int,DEST_SVC_BKG_ITM_CNT:int,TOTL_DEST_SVC_TCKT_CNT:int,ADULT_INS_ITM_CNT:int,CHILD_INS_ITM_CNT:int,OTHR_INS_ITM_CNT:int,TOTL_INS_ITM_CNT:int,CNCL_PNLTY_WAIVR_PRICE_ADJ_AMT_USD:decimal(19,4),DYN_RATE_RULE_COST_AMT_USD:decimal(19,4),DYN_RATE_RULE_PRICE_AMT_USD:decimal(19,4),EMP_DISC_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),EXTRA_PERSN_COST_AMT_USD:decimal(19,4),EXTRA_PERSN_PRICE_AMT_USD:decimal(19,4),GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),GENRIC_COUPN_PRICE_AMT_USD:decimal(19,4),INFANT_CNT:int,LODG_PKG_SAVE_AMT_USD:decimal(19,4),LOYLTY_POINT_PRICE_ADJ_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_COST_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),MARGN_SALES_TAX_COST_AMT_USD:decimal(19,4),MARGN_SALES_TAX_PRICE_AMT_USD:decimal(19,4),NET_SVC_FEE_PRICE_AMT_USD:decimal(19,4),OCCUP_TAX_COST_AMT_USD:decimal(19,4),OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),RATE_PLN_RESTR_COST_AMT_USD:decimal(19,4),RATE_PLN_RESTR_PRICE_AMT_USD:decimal(19,4),REBATE_PRICE_AMT_USD:decimal(19,4),REFUND_PRICE_ADJ_AMT_USD:decimal(19,4),RM_NIGHT_CNT:int,SALES_TAX_COST_AMT_USD:decimal(19,4),SALES_TAX_PRICE_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_COST_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_PRICE_AMT_USD:decimal(19,4),STNDAL_HTL_PRICE_MOD_AMT_USD:decimal(19,4),SUPPL_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_PRICE_ADJ_AMT_USD:decimal(19,4),SVC_CHRG_COST_AMT_USD:decimal(19,4),SVC_CHRG_PRICE_AMT_USD:decimal(19,4),SVC_FEE_PRICE_AMT_USD:decimal(19,4),TCM_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_CMSN_AMT_USD:decimal(19,4),TOTL_COST_ADJ_AMT_USD:decimal(19,4),TOTL_FEE_COST_AMT_USD:decimal(19,4),TOTL_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_COST_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_PRICE_AMT_USD:decimal(19,4),TOTL_PERSN_CNT:int,TOTL_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_TAX_COST_AMT_USD:decimal(19,4),TOTL_TAX_PRICE_AMT_USD:decimal(19,4),VAR_MARGN_COST_ADJ_USD:decimal(19,4),CNCL_CHG_FEE_PRICE_AMT_USD:decimal(19,4),CHILD_CNT:int,TRANS_DATETM:string,AIR_PKG_SAVE_AMT_USD:decimal(19,4),PKG_AGNCY_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_CAR_CNT:int,PKG_AGNCY_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_RM_CNT:int,PKG_AGNCY_RM_NIGHT_UNIT_CNT:int,PKG_AGNCY_TCKT_UNIT_CNT:int,PKG_AGNCY_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_TRAIN_TCKT_UNIT_CNT:int,PKG_AIR_DURATN_DAY_CNT:int,PKG_AIR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AIR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_CNT:int,PKG_CAR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_CAR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_RENTL_DAY_UNIT_CNT:int,PKG_COST_ADJ_AMT_USD:decimal(19,4),PKG_CRUIS_CABN_UNIT_CNT:int,PKG_CRUIS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_FEE_PRICE_AMT_USD:decimal(19,4),PKG_DEST_SVC_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_MARGN_AMT_USD:decimal(19,4),PKG_DEST_SVC_TCKT_UNIT_CNT:int,PKG_INS_FEE_PRICE_AMT_USD:decimal(19,4),PKG_INS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_INS_ITM_UNIT_CNT:int,PKG_INS_MARGN_AMT_USD:decimal(19,4),PKG_LODG_FEE_PRICE_AMT_USD:decimal(19,4),PKG_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_LODG_MARGN_AMT_USD:decimal(19,4),PKG_MERCH_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_PRICE_ADJ_AMT_USD:decimal(19,4),PKG_SAVE_AMT_USD:decimal(19,4),PKG_SAVE_PRICE_AMT_USD:decimal(19,4),PKG_TAX_COST_AMT_USD:decimal(19,4),PKG_TAX_PRICE_AMT_USD:decimal(19,4),PKG_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_TRAIN_MARGN_AMT_USD:decimal(19,4),TOTL_PKG_FEE_COST_AMT_USD:decimal(19,4),TOTL_PKG_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_PKG_UNIT_CNT:int,DURATN_DAY_CNT:int,PKG_MERCH_CAR_CNT:int,PKG_MERCH_RM_CNT:int,PKG_MERCH_RM_NIGHT_UNIT_CNT:int,PKG_MERCH_TCKT_UNIT_CNT:int,PKG_MERCH_TRAIN_TCKT_UNIT_CNT:int,PKG_RM_CNT:int,PKG_RM_NIGHT_UNIT_CNT:int,PKG_TCKT_SEG_CNT:int,PKG_TCKT_UNIT_CNT:int,PKG_TOTL_TRVLR_CNT:int,PKG_TRAIN_DURATN_DAY_CNT:int,PKG_TRAIN_TCKT_UNIT_CNT:int,TRANS_AGNT_TOOL_NAME:string,TRANS_SRC_TYP_NAME:string,ONLINE_OFFLN_IND:string,MGMT_UNIT_KEY:int,MGMT_UNIT_CODE:string,MGMT_UNIT_NAME:string,MGMT_UNIT_LVL_1_NAME:string,MGMT_UNIT_LVL_2_NAME:string,MGMT_UNIT_LVL_3_NAME:string,MGMT_UNIT_LVL_4_NAME:string,MGMT_UNIT_LVL_5_NAME:string,MGMT_UNIT_LVL_6_NAME:string,MGMT_UNIT_LVL_7_NAME:string,MGMT_UNIT_LVL_8_NAME:string,MGMT_UNIT_LVL_9_NAME:string,MGMT_UNIT_LVL_10_NAME:string,MGMT_UNIT_LVL_11_NAME:string,MGMT_UNIT_LVL_12_NAME:string,MGMT_UNIT_LVL_13_NAME:string,MGMT_UNIT_LVL_14_NAME:string,PURCH_TRVL_ACCT_KEY:int,BKG_SERVICE_TYPE_NAME:string,BKG_SERVICE_CATEGORY_NAME:string,ETE_POS:string,ETE_FLAG:string,AGNT_ASST_CHG_FEE_AMT_USD:decimal(19,4),AGNT_ASST_CNCL_FEE_AMT_USD:decimal(19,4),SERV_FEE_TRANS_CNT:int,CNCL_FEE_TRANS_CNT:int,ABS_CHG_TRANS_CNT:int,ABS_CNCL_TRANS_CNT:int,BKG_AGENT_TIER_NAME:string>>
ngcc_trans_typ_skill_id	int
ngcc_trans_typ_skill_name	string
ngcc_product_skill_id	int
ngcc_product_skill_name	string
ngcc_lang_skill_id  	int
ngcc_lang_skill_name	string
cust_ops_ngcc_agnt_id	string
ngcc_query_typ_skill_id	int
ngcc_query_typ_skill_cat_name	string
ngcc_query_typ_skill_cat_desc	string
ngcc_seg_skill_id   	int
ngcc_seg_skill_cat_name	string
ngcc_seg_typ_skill_cat_desc	string
ngcc_extrnl_agnt_supprt_skill_id	int
ngcc_extrnl_agnt_supprt_skill_name	string
ngcc_extrnl_agnt_supprt_skill_desc	string
nav_int_case_id     	string
nav_int_typ_name    	string
nav_int_typ_reasn_name	string
nav_int_stat_name   	string
nav_int_local_currn 	string
nav_int_assign_queue_name	string
nav_int_cncl_case_ind	string
nav_int_guest_acct_case_ind	string
nav_int_anchor_ind  	string
nav_int_case_sla_typ_name	string
nav_int_sla_datetm  	string
nav_int_sla_goal_datetm	string
nav_int_create_org_name	string
nav_int_create_div_name	string
nav_int_create_org_unit_name	string
nav_int_create_wrk_grp_name	string
nav_int_resolve_org_name	string
nav_int_resolve_div_name	string
nav_int_resolve_org_unit_name	string
nav_int_resolve_wrk_grp_name	string
nav_int_cust_case_id	string
nav_int_cust_score_name	string
nav_int_cust_identity_verify_ind	string
nav_int_itin_nbr    	string
nav_int_tpid        	int
nav_int_trl         	bigint
nav_int_answr_second_cnt	bigint
nav_int_resolve_second_cnt	decimal(19,4)
nav_int_task_wrap_up_duratn_second_cnt	int
nav_int_nav_int_tm_second_cnt	decimal(19,4)
nav_int_no_of_item_create_cnt	int
nav_int_case_cnt    	int
nav_resolve_abandon_case_cnt	int
nav_resolve_cancel_case_cnt	int
nav_resolve_complete_case_cnt	int
nav_int_create_agnt_login_id	string
nav_int_create_agnt_key	int
nav_int_create_agnt_earns_cmsn_ind	string
nav_int_create_agnt_email_addr	string
nav_int_create_agnt_emp_typ_name	string
nav_int_create_agnt_frst_name	string
nav_int_create_agnt_middl_name	string
nav_int_create_agnt_last_name	string
nav_int_create_agnt_knwn_as_name	string
nav_int_create_agnt_hire_date	string
nav_int_create_agnt_termnatn_date	string
nav_int_create_agnt_job_title_name	string
nav_int_create_agnt_mgmt_div_name	string
nav_int_create_agnt_mgr_frst_name	string
nav_int_create_agnt_mgr_last_name	string
nav_int_create_agnt_prim_business_grp_name	string
nav_int_create_agnt_prim_typ_name	string
nav_int_create_agnt_profcncy_name	string
nav_int_create_agnt_role_name	string
nav_int_create_agnt_prim_lang_code	string
nav_int_create_agnt_prim_lang_name	string
nav_int_create_agnt_scndry_lang_code	string
nav_int_create_agnt_scndry_lang_name	string
nav_int_create_agnt_tertiary_lang_code	string
nav_int_create_agnt_tertiary_lang_name	string
nav_int_create_agnt_vndr_loc_name	string
nav_int_create_agnt_vndr_name	string
nav_int_create_agnt_hire_tenr_day	int
nav_int_create_agnt_role_tenr_day	int
nav_int_create_agnt_service_category_name	string
nav_int_update_agnt_login_id	string
nav_int_update_agnt_key	int
nav_int_resolve_agnt_login_id	string
nav_int_resolve_agnt_key	int
nav_tm_zone_key     	int
nav_tm_zone_name    	string
nav_int_source_create_datetm	string
nav_int_pst_create_date_key	int
nav_int_gmt_create_date_key	int
nav_int_aest_create_date_key	int
nav_int_pst_create_tm_key	int
nav_int_gmt_create_tm_key	int
nav_int_aest_create_tm_key	int
nav_int_source_update_datetm	string
nav_int_pst_update_date_key	int
nav_int_gmt_update_date_key	int
nav_int_aest_update_date_key	int
nav_int_pst_update_tm_key	int
nav_int_gmt_update_tm_key	int
nav_int_aest_update_tm_key	int
nav_int_source_resolve_datetm	string
nav_int_pst_resolve_date_key	int
nav_int_gmt_resolve_date_key	int
nav_int_aest_resolve_date_key	int
nav_int_pst_resolve_tm_key	int
nav_int_gmt_resolve_tm_key	int
nav_int_aest_resolve_tm_key	int
nav_int_em_lang_name	string
nav_int_em_lang_code	string
nav_int_em_lang_key 	int
nav_int_svc_list    	array<struct<SVC_CASE_ID:string,SVC_CASE_STAT_NAME:string,SVC_ASSIGN_QUEUE_NAME:string,SVC_INT_TYP_NAME:string,SVC_TRVL_STG_IND:string,SVC_LOCAL_CURRN:string,SVC_ERR_LOGIN_NAME:string,SVC_GUEST_ACCT_CASE_IND:string,SVC_CALLER_DISCONNECT_IND:string,SVC_CHG_EXECUTED_TYP_IND:string,SVC_COUPN_ISSUE_IND:string,SVC_COUPN_EXPR_DATE:string,SVC_VNDR_CONSULT_IND:string,SVC_REFUND_IND:string,SVC_WRITE_OFF_IND:string,SVC_ACTN_NAME:string,SVC_ACTN_DESC:string,SVC_MSSNG_RES_ACTN_CODE:string,SVC_MSSNG_RES_ACTN_NAME:string,SVC_GUEST_ACCT_IND:string,SVC_INS_IND:string,SVC_ANCHOR_IND:string,SVC_CUST_CALLBK_IND:string,SVC_CUST_CALLBK_DESC:string,SVC_COMPLAINT_IND:string,SVC_NEW_CASE_IND:string,SVC_CASE_CUST_EMAIL_ADDR:string,SVC_SLA_TYP_NAME:string,SVC_SLA_DATETM:string,SVC_SLA_GOAL_DATETM:string,SVC_ESCALATE_IND:string,SVC_ESCALATE_PARENT_CASE_ID:string,SVC_ESCALATE_ACTN_NAME:string,SVC_ESCALATE_COMMUNICATION_ISSUE_NAME:string,SVC_ESCALATE_FAULT_NAME:string,SVC_ESCALATE_ISSUE_TYP_NAME:string,SVC_ESCALATE_RESOLTN_NAME:string,SVC_ESCALATE_ROOT_CAUSE_NAME:string,SVC_ITIN_NBR:string,SVC_TPID:int,SVC_TPID_KEY:int,SVC_TUID:bigint,SVC_TRL:bigint,SVC_PRODUCT_CAT_NAME:string,SVC_PRODUCT_CAT_KEY:smallint,SVC_PRODUCT_CAT_ID:smallint,SVC_HOTEL_ID:bigint,SVC_LODG_PROPERTY_KEY:int,SVC_LODG_PROPERTY_NAME:string,SVC_PROPERTY_BRAND_NAME:string,SVC_PROPERTY_CITY_NAME:string,SVC_PROPERTY_CNTRY_CODE:string,SVC_PROPERTY_REGN_NAME:string,SVC_PROPERTY_PARNT_CHAIN_NAME:string,SVC_PROPERTY_TYP_NAME:string,SVC_BUSINESS_PARTNR_KEY:int,SVC_BUSINESS_PARTNR_NAME:string,SVC_ACTV_BUSINESS_PARTNR_IND:string,SVC_BUSINESS_PARTNR_MED_NAME:string,SVC_BUSINESS_PARTNR_MGMT_UNIT_NAME:string,SVC_BUSINESS_PARTNR_RPT_CO_NAME:string,SVC_BUSINESS_PARTNR_SEG_NAME:string,SVC_BUSINESS_PARTNR_SVC_BRAND_NAME:string,SVC_BUSINESS_PARTNR_SVC_CNTRY_NAME:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_DESC:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_NAME:string,SVC_BUSINESS_PARTNR_SYS_NAME:string,SVC_BUSINESS_PARTNR_TPID_NAME:string,SVC_MGMT_UNIT_KEY:int,SVC_MGMT_UNIT_NAME:string,SVC_MGMT_UNIT_LVL_1_NAME:string,SVC_MGMT_UNIT_LVL_2_NAME:string,SVC_MGMT_UNIT_LVL_3_NAME:string,SVC_MGMT_UNIT_LVL_4_NAME:string,SVC_MGMT_UNIT_LVL_5_NAME:string,SVC_MGMT_UNIT_LVL_6_NAME:string,SVC_MGMT_UNIT_LVL_7_NAME:string,SVC_MGMT_UNIT_LVL_8_NAME:string,SVC_MGMT_UNIT_LVL_9_NAME:string,SVC_MGMT_UNIT_LVL_10_NAME:string,SVC_MGMT_UNIT_LVL_11_NAME:string,SVC_MGMT_UNIT_LVL_12_NAME:string,SVC_MGMT_UNIT_LVL_13_NAME:string,SVC_MGMT_UNIT_LVL_14_NAME:string,SVC_ONLINE_OFFLN_IND_KEY:int,SVC_ONLINE_OFFLN_IND:string,SVC_PURCH_TRVL_ACCT_ID:string,SVC_PURCH_TRVL_ACCT_KEY:int,SVC_PURCH_TRVL_ACCT_FRST_NAME:string,SVC_PURCH_TRVL_ACCT_LAST_NAME:string,SVC_PURCH_TRVL_ACCT_EMAIL_ADDR:string,SVC_PURCH_TRVL_ACCT_ADDR_1:string,SVC_PURCH_TRVL_ACCT_ADDR_2:string,SVC_PURCH_TRVL_ACCT_CITY_NAME:string,SVC_PURCH_TRVL_ACCT_POSTAL_CODE:string,SVC_PURCH_TRVL_ACCT_STATE_PROVNC_NAME:string,SVC_PURCH_TRVL_ACCT_CNTRY_CODE:string,SVC_PURCH_TRVL_ACCT_CNTRY_NAME:string,SVC_PURCH_TRVL_ACCT_SUPER_REGN_NAME:string,SVC_PURCH_TRVL_ACCT_CREATE_DATE:string,SVC_BKG_TYP_CODE:string,SVC_BKG_TYP_NAME:string,SVC_BKG_TYP_BUSINESS_MODEL_NAME:string,SVC_AIRLN_CARRIER:string,SVC_AIRFARE_TYP_CODE:string,SVC_AIRFARE_TYP_NAME:string,SVC_FLGHT_SCHED_CHG_OPTN_IND:string,SVC_LOW_COST_CARRIER_IND:string,SVC_SECURE_FLGHT_IND:string,SVC_BRKAGE_CANDIDATE_IND:string,SVC_CUST_CASE_ID:string,SVC_CUST_SCORE_NAME:string,SVC_CUST_IDENTITY_VERIFY_IND:string,SVC_CREDT_EXPR_TYP_CODE:string,SVC_CREDT_EXPR_TYP_DESC:string,SVC_EARLY_CHK_OUT_IND:string,SVC_EXPE_REWARDS_IND:string,SVC_INTNT_NAME:string,SVC_INTNT_DESC:string,SVC_CAT_LVL_1_INTENT_NAME:string,SVC_CNCL_LVL_2_INTENT_CODE:string,SVC_CNCL_LVL_2_INTENT_NAME:string,SVC_CNCL_CHG_LVL_2_INTENT_CODE:string,SVC_CNCL_CHG_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_3_INTENT_NAME:string,SVC_COUPN_LVL_2_INTENT_CODE:string,SVC_COUPN_LVL_2_INTENT_NAME:string,SVC_COUPN_LVL_3_INTENT_CODE:string,SVC_COUPN_LVL_3_INTENT_NAME:string,SVC_CRUIS_AFFL_NAME:string,SVC_RECONFIRM_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_2_INTENT_CODE:string,SVC_REFUND_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_3_INTENT_CODE:string,SVC_REFUND_LVL_3_INTENT_NAME:string,SVC_REFUND_METHD_LVL_4_INTENT_NAME:string,SVC_REQST_LVL_2_INTENT_CODE:string,SVC_REQST_LVL_2_INTENT_NAME:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_CODE:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_NAME:string,SVC_CONSULT_LIST:array<struct<SVC_CONSULT_CASE_ESCALATE_ID:string,SVC_CONSULT_AGNT_ROLE_NAME:string,SVC_CONSULT_NOTE_TXT:string,SVC_CONSULT_REASN_NAME:string,SVC_CONSULT_VNDR_TYP_ID:int,SVC_CONSULT_VNDR_TYP_NAME:string,SVC_ESCALATE_TYP_ID:int,SVC_ESCALATE_TYP_NAME:string,SVC_ESCALATE_PARTY_TYP_NAME:string>>,BKG_CONF_ID:bigint,REFUND_APPRVR_FRST_NAME:string,REFUND_APPRVR_LAST_NAME:string,REFUND_APPRVR_TITLE_NAME:string,SVC_LOYLTY_POLICY_FIN_IMPACT_IND:string,SVC_LOYLTY_POLICY_NAME:string,SVC_LOYLTY_ACTIVITY_REVIEW_IND:string,SVC_LOYLTY_MEMBR_ID:bigint,SVC_CREATE_ORG_NAME:string,SVC_CREATE_DIV_NAME:string,SVC_CREATE_ORG_UNIT_NAME:string,SVC_CREATE_WRK_GRP_NAME:string,SVC_RESOLVE_ORG_NAME:string,SVC_RESOLVE_DIV_NAME:string,SVC_RESOLVE_ORG_UNIT_NAME:string,SVC_RESOLVE_WRK_GRP_NAME:string,SVC_S_CASE_CNT:int,SVC_E_CASE_CNT:int,SVC_O_CASE_CNT:int,SVC_ESCALATION_CASE_CNT:int,SVC_RESOLVE_SECOND_CNT:decimal(19,4),SVC_CREATE_AGNT_LOGIN_ID:string,SVC_CREATE_AGNT_KEY:int,SVC_CREATE_AGNT_EARNS_CMSN_IND:string,SVC_CREATE_AGNT_EMAIL_ADDR:string,SVC_CREATE_AGNT_EMP_TYP_NAME:string,SVC_CREATE_AGNT_FRST_NAME:string,SVC_CREATE_AGNT_MIDDL_NAME:string,SVC_CREATE_AGNT_LAST_NAME:string,SVC_CREATE_AGNT_KNWN_AS_NAME:string,SVC_CREATE_AGNT_HIRE_DATE:string,SVC_CREATE_AGNT_TERMNATN_DATE:string,SVC_CREATE_AGNT_JOB_TITLE_NAME:string,SVC_CREATE_AGNT_MGMT_DIV_NAME:string,SVC_CREATE_AGNT_MGR_FRST_NAME:string,SVC_CREATE_AGNT_MGR_LAST_NAME:string,SVC_CREATE_AGNT_PRIM_BUSINESS_GRP_NAME:string,SVC_CREATE_AGNT_PRIM_TYP_NAME:string,SVC_CREATE_AGNT_PROFCNCY_NAME:string,SVC_CREATE_AGNT_ROLE_NAME:string,SVC_CREATE_AGNT_PRIM_LANG_CODE:string,SVC_CREATE_AGNT_PRIM_LANG_NAME:string,SVC_CREATE_AGNT_SCNDRY_LANG_CODE:string,SVC_CREATE_AGNT_SCNDRY_LANG_NAME:string,SVC_CREATE_AGNT_TERTIARY_LANG_CODE:string,SVC_CREATE_AGNT_TERTIARY_LANG_NAME:string,SVC_CREATE_AGNT_VNDR_LOC_NAME:string,SVC_CREATE_AGNT_VNDR_NAME:string,SVC_CREATE_AGNT_HIRE_TENR_DAY:int,SVC_CREATE_AGNT_ROLE_TENR_DAY:int,SVC_CREATE_AGNT_SERVICE_CATEGORY_NAME:string,SVC_UPDATE_AGNT_LOGIN_ID:string,SVC_UPDATE_AGNT_KEY:int,SVC_RESOLVE_AGNT_LOGIN_ID:string,SVC_RESOLVE_AGNT_KEY:int,SVC_SOURCE_CREATE_DATETM:string,SVC_PST_CREATE_DATE_KEY:int,SVC_GMT_CREATE_DATE_KEY:int,SVC_AEST_CREATE_DATE_KEY:int,SVC_PST_CREATE_TM_KEY:int,SVC_GMT_CREATE_TM_KEY:int,SVC_AEST_CREATE_TM_KEY:int,SVC_SOURCE_UPDATE_DATETM:string,SVC_PST_UPDATE_DATE_KEY:int,SVC_GMT_UPDATE_DATE_KEY:int,SVC_AEST_UPDATE_DATE_KEY:int,SVC_PST_UPDATE_TM_KEY:int,SVC_GMT_UPDATE_TM_KEY:int,SVC_AEST_UPDATE_TM_KEY:int,SVC_SOURCE_RESOLVE_DATETM:string,SVC_PST_RESOLVE_DATE_KEY:int,SVC_GMT_RESOLVE_DATE_KEY:int,SVC_AEST_RESOLVE_DATE_KEY:int,SVC_PST_RESOLVE_TM_KEY:int,SVC_GMT_RESOLVE_TM_KEY:int,SVC_AEST_RESOLVE_TM_KEY:int,SVC_CASE_USER_EMAIL_ADDR:string,SVC_LANG_CODE:string,SVC_LANG_KEY:int,RESPONDENT_ID:string,SVC_GEN_LVL_2_INTENT_CODE:string,SVC_GEN_LVL_2_INTENT_NAME:string>>
call_eval_detail    	array<struct<CALL_EVAL_ID:int,CALL_EVAL_SITE_ID:int,CALL_EVAL_QUESTN_ID:int,CALL_EVAL_ANSWR_ID:int,CALL_EVAL_CREATE_DATETM:string,SRC_DB_VERSION_NBR:int,CALL_EVAL_UPDATE_DATETM:string,PST_CALL_EVAL_CREATE_DATE_KEY:int,PST_CALL_EVAL_CREATE_DATE:string,GMT_CALL_EVAL_CREATE_DATE_KEY:int,GMT_CALL_EVAL_CREATE_DATE:string,PST_CALL_EVAL_UPDATE_DATE_KEY:int,PST_CALL_EVAL_UPDATE_DATE:string,GMT_CALL_EVAL_UPDATE_DATE_KEY:int,GMT_CALL_EVAL_UPDATE_DATE:string,CALL_EVAL_AGNT_NICE_USER_ID:int,CALL_EVAL_AGNT_PERIPH_NBR:array<string>,CALL_EVAL_SEG_ID:string,CALL_EVAL_CASE_NBR:string,CALL_EVAL_ITIN_NBR:string,CALL_EVAL_SEG_DURATN_TM:string,CUST_OPS_CALL_EVAL_QUESTN_KEY:int,CALL_EVAL_QUESTN_SECTN_NAME:string,CALL_EVAL_QUESTN_SUB_SECTN_NAME:string,CALL_EVAL_QUESTN_CALL_OUTCOME:string,CALL_EVAL_QUESTN_SUB_SECTN_NBR:string,CALL_EVAL_QUESTN_NBR:string,CALL_EVAL_QUESTN_LBL_NAME:string,CALL_EVAL_QUESTN_CAPTN_NAME:string,CALL_EVAL_QUESTN_HIER_NBR:string,CALL_EVAL_QUESTN_TYP_ID:int,CALL_EVAL_QUESTN_SCORABLE_NBR:int,CALL_EVAL_QUESTN_IS_FATAL_NBR:int,CUST_OPS_CALL_EVAL_ANSWR_KEY:int,CALL_EVAL_ANSWR_SHORT_REASN_NAME:string,CALL_EVAL_ANSWR_DETAIL_REASN_NAME:string,CUST_OPS_CALL_EVAL_FORM_KEY:int,CALL_EVAL_FORM_ID:int,CALL_EVAL_FORM_TYP_NAME:string,CALL_EVAL_FORM_SRC_NAME:string,CALL_EVAL_FORM_NAME:string,CALL_EVAL_FORM_CREATE_DATETM:string,CALL_EVAL_FORM_UPDATE_DATETM:string,CALL_EVAL_FORM_ENABL_IND:string,CUST_OPS_CALL_EVALUATOR_KEY:int,CALL_EVAL_EVALUATOR_USER_ID:int,CALL_EVALUATOR_FULL_NAME:string,CALL_EVALUATOR_LOCATION_NAME:string,CUST_OPS_CALL_EVAL_TYP_KEY:int,CALL_EVAL_TYP_ID:int,CALL_EVAL_TYP_NAME:string,CUST_OPS_CALL_EVAL_AUTOFAIL_ANSWR_IND_KEY:int,CALL_EVAL_AUTOFAIL_ANSWR_IND:string,CALL_EVAL_AUTOFAIL_ANSWR_CNT:int,CALL_EVAL_ANSWR_CNT:int,POSSIBLE_POINT_NBR:decimal(19,4),EARN_POINT_NBR:decimal(19,4),ADJUSTED_POSSIBLE_POINT_CNT:decimal(19,4),ADJUSTED_EARN_POINT_CNT:decimal(19,4)>>
cvp_call_guid       	string
cvp_ab_test_ind     	string
cvp_call_start_date 	string
cvp_call_pst_start_date_key	int
cvp_call_gmt_start_date_key	int
cvp_call_start_datetm	string
cvp_call_end_datetm 	string
cvp_call_time_zone_key	int
cvp_call_time_zone  	string
cvp_call_ani        	string
cvp_cust_ops_phon_assign_key	int
cvp_call_dnis       	string
cvp_call_cnt        	int
cvp_auto_ani_cnt    	int
cvp_manual_ani_cnt  	int
cvp_auto_itin_cnt   	int
cvp_manual_itin_cnt 	int
cvp_override_itin_cnt	int
cvp_hang_up_time_brct_0_10_cnt	int
cvp_hang_up_time_brct_11_20_cnt	int
cvp_hang_up_time_brct_21_30_cnt	int
cvp_hang_up_time_brct_31_up_cnt	int
cvp_elemnt_list     	array<struct<CVP_SESSN_ID:bigint,CVP_SESSN_APP_KEY:int,CVP_SESSN_APP_NAME:string,CVP_SESSN_START_DATETM:string,CVP_SESSN_END_DATETM:string,CVP_SESSN_PRODUCT:string,CVP_SESSN_PRODUCT_CAT_KEY:int,CVP_SESSN_TRANS_TYP_CODE:string,CVP_SESSN_TRANS_TYP_KEY:int,CVP_SESSN_TRANS_TYP_NAME:string,CVP_SESSN_CALLER_NEED_CODE:string,CVP_SESSN_CALLER_NEED_KEY:int,CVP_SESSN_CALLER_NEED_NAME:string,CVP_SESSN_LANG_KEY:int,CVP_SESSN_LANG_CODE:string,CVP_SESSN_PRIME_LANG_CNT:bigint,CVP_SESSN_NON_PRIME_LANG_CNT:bigint,CVP_SESSN_BUSINESS_PARTNR_KEY:bigint,CVP_SESSN_BUSINESS_PARTNR_NAME:string,CVP_SESSN_DNIS_NAME:string,CVP_SESSN_ITIN:string,CVP_ELEMNT_ID:bigint,CVP_ELEMNT_KEY:int,CVP_ELEMNT_NAME:string,CVP_ELEMNT_START_DATETM:string,CVP_ELEMNT_END_DATETM:string,CVP_ELEMNT_NUM_INTERACT_CNT:int,CVP_ELEMNT_RESLT:int,CVP_ELEMNT_EXIT_STATE:string,CVP_LODG_CNCL_ELIG_CNT:int,CVP_LODG_CNCL_SUCCSS_CNT:int,CVP_LODG_CNCL_FAIL_CNT:int,CVP_CNCL_SECURE_VALID_METHOD_NAME:string,CVP_CNCL_SECURE_VALID_ZIP_CNT:int,CVP_CNCL_SECURE_VALID_LAST_FOUR_CNT:int,CVP_CNCL_SECURE_VALID_FAIL_CNT:int,CVP_CNCL_TERM_ACCPT_CNT:int,CVP_CNCL_TERM_DECLN_CNT:int,CVP_ANYTHING_ELSE_HUNG_UP_CNT:int,CVP_ANYTHING_ELSE_NEW_CNT:int,CVP_ANYTHING_ELSE_MAIN_MENU_CNT:int,CVP_PHON_NBR_PROMPT_CNT:int,CVP_PHON_FROM_ITIN_LKUP_CNT:int,CVP_ITIN_PROMPT_CNT:int,CVP_ITIN_LKUP_SUCCESS_CNT:int,CVP_ITIN_LKUP_FAIL_CNT:int,CVP_ITIN_SELCT_DIFF_CNT:int,CVP_BKG_MENU_NO_INPUT_CNT:int,CVP_BKG_MENU_HUNG_UP_CNT:int,CVP_ELEMNT_OPT_OUT_CNT:int,CVP_ELEMNT_HANG_UP_CNT:int,CVP_ELEMNT_DURATION:bigint,CVP_ELEMNT_REPT_CNT:int,CVP_ELEMNT_MAX_NO_INPUT_CNT:int,CVP_ELEMNT_DTFM_INPUT_CNT:bigint,CVP_ELEMNT_VOICE_INPUT_CNT:bigint,CVP_ELEMNT_UTTERANCE:string,CVP_ELEMNT_INTERPRETATION:string,CVP_ELEMNT_VOICE_CONFIDENCE_PCT:string,CVP_ELEMNT_NO_INPUT_CNT:int,CVP_ELEMNT_REPEAT_TERMINATE_CNT:int>>
respondent_id       	string
load_tag            	bigint
call_service_agnt_typ	string
call_agent_tier_name	string
caller_service_type 	string
caller_loyalty_name 	string
nav_int_create_agnt_service_tier_name	string

# Partition Information
# col_name            	data_type           	comment

cust_ops_call_src_sys_name	string
partition_day       	string

# Detailed Partition Information
Partition Value:    	[BKG, 2013-01-01]
Database:           	onprem_conversation
Table:              	hadoop_dm_cust_ops_call_bkg_detail
CreateTime:         	Fri Sep 22 00:16:37 UTC 2023
LastAccessTime:     	UNKNOWN
Location:           	s3://apiary-classic-387910953075-us-east-1-onprem-conversation/hadoop_dm_cust_ops_call_bkg_detail/cust_ops_call_src_sys_name=BKG/partition_day=2013-01-01
Partition Parameters:
	transient_lastDdlTime	1695341797

# Storage Information
SerDe Library:      	org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe
InputFormat:        	org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
OutputFormat:       	org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat
Compressed:         	No
Num Buckets:        	-1
Bucket Columns:     	[]
Sort Columns:       	[]
Storage Desc Params:
	hive.serialization.extend.additional.nesting.levels	true
	hive.serialization.extend.nesting.levels	true
	serialization.format	1
Time taken: 3.962 seconds, Fetched: 587 row(s)
```

```
hive> DESCRIBE EXTENDED onprem_conversation.hadoop_dm_cust_ops_call_bkg_detail PARTITION (cust_ops_call_src_sys_name='BKG', partition_day='2013-01-01');
OK
cust_ops_call_id    	string
cust_ops_call_seg_seq_nbr	int
trans_date_key      	int
trans_agnt_key      	int
agnt_skill_target_id	int
router_call_day_id  	int
router_call_id      	int
src_call_start_datetm	string
src_call_end_datetm 	string
aest_call_start_date_key	int
gmt_call_start_date_key	int
pst_call_start_date_key	int
pst_call_end_date_key	int
pst_call_start_tm_key	int
pst_call_end_tm_key 	int
agnt_periph_nbr     	string
call_itin_nbr       	string
inbnd_dialed_nbr    	string
outbnd_dialed_nbr   	string
ani_nbr             	string
cust_ops_ngcc_agnt_sessn_id	string
cust_ops_ngcc_media_id	string
orignl_ani_nbr      	string
orignl_cust_ops_ngcc_cntct_id	string
vq_inbnd_call_id    	string
vq_return_call_id   	string
cust_ops_agnt_key   	int
cust_ops_agnt_id    	int
call_agnt_frst_name 	string
call_agnt_middl_name	string
call_agnt_last_name 	string
call_agnt_job_title_name	string
call_agnt_hire_date 	string
call_agnt_hire_tenr_day	int
call_agnt_termnatn_date	string
call_agnt_vndr_loc_id	int
call_agnt_vndr_loc_name	string
call_agnt_vndr_id   	smallint
call_agnt_vndr_name 	string
call_agnt_role_id   	smallint
call_agnt_role_name 	string
call_agnt_role_tenr_day	int
call_agnt_prim_cust_ops_typ_id	smallint
call_agnt_prim_cust_ops_typ_name	string
call_agnt_profcncy_id	smallint
call_agnt_profcncy_name	string
call_agnt_mgr_frst_name	string
call_agnt_mgr_last_name	string
call_agnt_full_name 	string
call_agnt_mgr_full_name	string
call_agnt_typ_id    	int
call_agnt_typ_name  	string
call_business_partnr_key	int
call_business_partnr_id	int
call_business_partnr_sys_name	string
call_expe_business_partnr_id	int
call_ian_business_partnr_id	int
call_as400_business_partnr_src_code	string
call_business_partnr_name	string
call_business_partnr_tpid	int
call_business_partnr_tpid_name	string
call_business_partnr_svc_brand_name	string
call_business_partnr_svc_short_cntry_code	string
call_business_partnr_svc_cntry_name	string
call_business_partnr_svc_super_regn_name	string
call_business_partnr_svc_super_regn_desc	string
call_actv_business_partnr_ind	string
call_business_partnr_acct_mgr_name	string
call_business_partnr_b2b_billng_typ_name	string
call_business_partnr_co_name	string
call_business_partnr_home_url	string
call_business_partnr_med_name	string
call_business_partnr_mgmt_unit_code	string
call_business_partnr_mgmt_unit_name	string
call_business_partnr_oper_regn_name	string
call_business_partnr_rpt_co_code	string
call_business_partnr_rpt_co_name	string
call_business_partnr_seg_name	string
call_business_partnr_site_platform_name	string
call_business_partnr_start_date	string
call_business_partnr_website_domain_name	string
call_parnt_business_partnr_id	int
call_parnt_business_partnr_name	string
call_tpid_key       	int
call_tpid           	int
call_tpid_name      	string
call_tpid_cntry_code	string
call_tpid_cntry_name	string
call_tpid_hwire_pos_code	string
call_mgmt_unit_key  	smallint
call_mgmt_unit_code 	string
call_mgmt_unit_name 	string
call_mgmt_unit_lvl_1_name	string
call_mgmt_unit_lvl_2_name	string
call_mgmt_unit_lvl_3_name	string
call_mgmt_unit_lvl_4_name	string
call_mgmt_unit_lvl_5_name	string
call_mgmt_unit_lvl_6_name	string
call_mgmt_unit_lvl_7_name	string
call_mgmt_unit_lvl_8_name	string
call_mgmt_unit_lvl_9_name	string
call_mgmt_unit_lvl_10_name	string
call_mgmt_unit_lvl_11_name	string
call_mgmt_unit_lvl_12_name	string
call_mgmt_unit_lvl_13_name	string
call_mgmt_unit_lvl_14_name	string
call_lang_key       	int
call_lang_code      	string
call_lang_name      	string
call_product_cat_key	smallint
call_product_cat_name	string
call_service_type_name	string
call_service_category_name	string
cust_ops_call_typ_key	int
cust_ops_call_typ_id	int
call_typ_code_list  	string
call_typ_desc       	string
call_typ_caller_need_id	smallint
call_typ_caller_need_code	string
call_typ_caller_need_name	string
call_typ_cust_ops_product_typ_id	smallint
call_typ_cust_ops_product_typ_code	string
call_typ_cust_ops_product_typ_name	string
call_typ_cust_trans_typ_id	smallint
call_typ_cust_trans_typ_code	string
call_typ_cust_trans_typ_name	string
call_typ_call_seg_grp_code	string
call_typ_call_seg_grp_name	string
call_typ_phon_site_chnnl_plcmnt_code	string
call_typ_phon_site_chnnl_plcmnt_name	string
call_typ_route_typ_code	string
call_typ_staff_grp_code	string
cust_ops_skill_grp_key	int
skill_grp_id        	int
skill_grp_code_list 	string
skill_grp_desc      	string
base_skill_grp_caller_need_id	smallint
base_skill_grp_caller_need_code	string
base_skill_grp_caller_need_name	string
base_skill_grp_cust_ops_product_typ_id	smallint
base_skill_grp_cust_ops_product_typ_code	string
base_skill_grp_cust_ops_product_typ_name	string
base_skill_grp_cust_trans_typ_id	smallint
base_skill_grp_cust_trans_typ_code	string
base_skill_grp_cust_trans_typ_name	string
base_skill_grp_call_seg_grp_code	string
base_skill_grp_call_seg_grp_name	string
base_skill_grp_desc 	string
base_skill_grp_code_list	string
base_skill_grp_phon_site_chnnl_plcmnt_code	string
base_skill_grp_phon_site_chnnl_plcmnt_name	string
base_skill_grp_staff_grp_code	string
frcast_grp_call_seg_grp_code	string
frcast_grp_call_seg_grp_name	string
frcast_grp_caller_need_code	string
frcast_grp_caller_need_name	string
frcast_grp_cust_ops_product_typ_code	string
frcast_grp_cust_ops_product_typ_name	string
frcast_grp_cust_trans_typ_code	string
frcast_grp_cust_trans_typ_name	string
frcast_grp_lang_code	string
frcast_grp_lang_name	string
skill_grp_profcncy_code	string
cust_ops_icrs_call_periph_dispostn_typ_key	int
cust_ops_icrs_periph_call_typ_id	int
cust_ops_icrs_call_dispostn_typ_id	int
periph_call_typ_name	string
call_dispostn_typ_name	string
call_dispostn_name  	string
call_sys_err        	string
call_sys_gb         	string
call_sys_partval    	string
call_sys_ref_id     	string
call_sys_lodg_property_name	string
call_typ_var_cust_ops_brand_name	string
call_typ_var_bkg_windw_name	string
call_typ_var_caller_need_code	string
call_typ_var_caller_need_name	string
call_typ_var_intl_dom_code	string
call_typ_var_intl_dom_name	string
call_typ_var_cust_ops_product_typ_code	string
call_typ_var_cust_ops_product_typ_name	string
call_typ_var_cust_trans_typ_code	string
call_typ_var_cust_trans_typ_name	string
call_typ_var_phon_site_chnnl_plcmnt_code	string
call_typ_var_phon_site_chnnl_plcmnt_name	string
call_typ_var_cust_ops_pos_code	string
call_typ_var_cust_ops_pos_name	string
call_typ_var_cust_ops_sub_brand_name	string
call_typ_var_trvl_duratn_code	string
call_typ_var_trvl_duratn_name	string
call_exprnce_var_intfc_fails	string
call_exprnce_var_lang_list	string
call_sys_var_media_publctn_id	string
call_exprnce_var_persna	string
target_seg_call_typ_key	int
target_seg_call_typ_id	int
target_seg_call_typ_code_list	string
target_seg_call_typ_desc	string
target_seg_call_typ_caller_need_id	smallint
target_seg_call_typ_caller_need_code	string
target_seg_call_typ_caller_need_name	string
target_seg_call_typ_cust_ops_product_typ_id	smallint
target_seg_call_typ_cust_ops_product_typ_code	string
target_seg_call_typ_cust_ops_product_typ_name	string
target_seg_call_typ_cust_trans_typ_id	smallint
target_seg_call_typ_cust_trans_typ_code	string
target_seg_call_typ_cust_trans_typ_name	string
target_seg_call_typ_call_seg_grp_code	string
target_seg_call_typ_call_seg_grp_name	string
target_seg_call_typ_phon_site_chnnl_plcmnt_code	string
target_seg_call_typ_phon_site_chnnl_plcmnt_name	string
target_seg_call_typ_route_typ_code	string
target_seg_call_typ_staff_grp_code	string
call_seg_state_ind  	string
vq_call_ind         	string
vq_call_state_ind   	string
cust_ops_phon_assign_key	int
cust_ops_phon_assign_id	int
cust_ops_phon_id    	int
phon_name           	string
cust_ops_phon_typ_id	smallint
cust_ops_phon_typ_name	string
phon_carrier_id     	smallint
phon_carrier_name   	string
phon_cntry_prfx_nbr 	string
phon_cntry_name     	string
cust_ops_phon_regn_id	smallint
cust_ops_phon_regn_name	string
phon_vanity_desc    	string
phon_local_nbr      	string
intl_phon_nbr       	string
rcf_phon_nbr        	string
rcf_phon_carrier_id 	smallint
rcf_phon_carrier_name	string
phon_acquir_date    	string
phon_retire_date    	string
phon_carrier_start_datetm	string
phon_carrier_end_datetm	string
phon_carrier_acct_nbr	string
phon_assign_format_phon_txt	string
phon_assign_business_partnr_key	int
phon_assign_start_datetm	string
phon_assign_end_datetm	string
phon_assign_parnt_phon_id	int
phon_assign_cust_ops_branding_cat_id	smallint
phon_assign_cust_ops_branding_cat_name	string
phon_assign_cust_ops_brand_lvl_1_id	smallint
phon_assign_cust_ops_brand_lvl_1_name	string
phon_assign_cust_ops_brand_lvl_2_id	smallint
phon_assign_cust_ops_brand_lvl_2_name	string
phon_assign_cust_ops_brand_lvl_3_id	smallint
phon_assign_cust_ops_brand_lvl_3_name	string
phon_assign_cust_ops_pos_id	smallint
phon_assign_cust_ops_pos_code	string
phon_assign_cust_ops_pos_desc	string
phon_assign_iso_lang_code	string
phon_assign_iso_lang_name	string
phon_assign_cust_ops_product_typ_id	smallint
phon_assign_cust_ops_product_typ_name	string
phon_assign_mktg_chnnl_name_1	string
phon_assign_mktg_chnnl_name_2	string
phon_assign_mktg_cmpgn_lvl_1_id	smallint
phon_assign_mktg_cmpgn_lvl_1_name	string
phon_assign_mktg_cmpgn_lvl_2_id	smallint
phon_assign_mktg_cmpgn_lvl_2_name	string
phon_assign_media_typ_lvl_1_id	smallint
phon_assign_media_typ_lvl_1_name	string
phon_assign_media_typ_lvl_2_id	smallint
phon_assign_media_typ_lvl_2_name	string
phon_assign_media_typ_lvl_3_id	smallint
phon_assign_media_typ_lvl_3_name	string
phon_assign_publctn_typ_lvl_1_id	smallint
phon_assign_publctn_typ_lvl_1_name	string
phon_assign_publctn_typ_lvl_2_id	smallint
phon_assign_publctn_typ_lvl_2_name	string
phon_assign_site_plcmnt_lvl_1_id	smallint
phon_assign_site_plcmnt_lvl_1_name	string
phon_assign_site_plcmnt_lvl_2_id	smallint
phon_assign_site_plcmnt_lvl_2_name	string
phon_assign_site_chnnl_plcmnt_id	smallint
phon_assign_site_chnnl_plcmnt_code	string
phon_assign_site_chnnl_plcmnt_name	string
phon_assign_mktg_effrt_desc	string
phon_assign_srch_cat_id	smallint
phon_assign_srch_cat_name	string
phon_assign_srch_term_desc	string
phon_assign_holder_id	smallint
phon_assign_holder_frst_name	string
phon_assign_holder_last_name	string
phon_assign_desc    	string
phon_assign_creative_id	smallint
phon_assign_creative_name	string
phon_assign_call_to_actn_id	smallint
phon_assign_call_to_actn_name	string
phon_assign_ivr_exprnce_id	int
phon_assign_ivr_exprnce_code_list	string
phon_assign_ivr_exprnce_desc	string
phon_assign_hrs_of_operatn_desc	string
phon_assign_cost_per_minute_desc	string
phon_assign_prk_until_datetm	string
phon_assign_rpt_dnp_ind	string
cust_ops_call_src_tm_zone_key	int
cust_ops_call_src_tm_zone_id	int
cust_ops_call_src_tm_zone_name	string
cust_ops_call_duratn_windw_key	int
cust_ops_ngcc_cntct_dirctn_key	int
cust_ops_ngcc_dirctn_id	int
ngcc_cntct_dirctn_name	string
cust_ops_ngcc_cntct_disconnect_typ_key	int
cust_ops_ngcc_cntct_disconnect_reasn_id	int
ngcc_cntct_disconnect_typ_name	string
cust_ops_ngcc_dispostn_typ_id	int
ngcc_cntct_dispostn_typ_name	string
agnt_handled_ind    	string
cust_disconnect_ind 	string
handled_wthn_sla_ind	string
ivr_handled_ind     	string
outbnd_ind          	string
cust_ops_ngcc_comptncy_grp_key	int
cust_ops_ngcc_comptncy_grp_id	int
agnt_ngcc_comptncy_grp_name	string
short_call_ind      	string
zero_talk_tm_ind    	string
agnt_disconnect_cnt 	int
attch_outbnd_cnt    	int
handle_cnt          	int
unattch_outbnd_cnt  	int
aftr_call_wrk_second_cnt	int
attch_outbnd_second_cnt	int
hold_second_cnt     	int
talk_second_cnt     	int
totl_handle_second_cnt	int
unattch_outbnd_second_cnt	int
hold_call_ind       	string
system_disconnect_cnt	int
abandon_second_cnt  	int
netwrk_abandon_second_cnt	int
trnsfr_abandon_second_cnt	int
totl_offr_cnt       	int
inbnd_queue_offr_cnt	int
transfr_queue_offr_cnt	int
answr_second_cnt    	int
netwrk_answr_second_cnt	int
trnsfr_answr_second_cnt	int
answr_20_second_call_ind	string
answr_30_second_call_ind	string
answr_60_second_call_ind	string
answr_120_second_call_ind	string
agnt_conect_attmpt_cnt	int
agnt_trnsfr_cnt     	int
arrv_cnt            	int
block_call_cnt      	int
cntct_cnt           	int
hold_abandon_cnt    	int
inbnd_cnt           	int
ivr_second_cnt      	int
othr_err_cnt        	int
queue_abandon_cnt   	int
retry_agnt_cnt      	int
sys_terminate_cnt   	int
sys_trnsfr_cnt      	int
transfr_init_cnt    	int
arrv_second_cnt     	int
duratn_second_cnt   	int
hang_up_second_cnt  	int
ivr_queue_second_cnt	int
ring_second_cnt     	int
totl_wrap_up_second_cnt	int
terminate_wrap_up_second_cnt	int
transfr_queue_second_cnt	int
agnt_disconnect_short_call_cnt	int
short_call_cnt      	int
zero_talk_cnt       	int
hold_call_cnt       	int
answr_20_second_call_cnt	int
answr_30_second_call_cnt	int
answr_60_second_call_cnt	int
answr_120_second_call_cnt	int
tier1_to_tier2_cnt  	int
ivr_terminate_cnt   	int
netwrk_handle_call_cnt	int
netwrk_totl_handle_tm	int
netwrk_handle_talk_tm	int
netwrk_handle_hold_tm	int
netwrk_aftr_call_wrk_tm	int
trnsfr_handle_call_cnt	int
trnsfr_totl_handle_tm	int
trnsfr_handle_talk_tm	int
trnsfr_handle_hold_tm	int
trnsfr_aftr_call_wrk_tm	int
agnt_disconnect_ind 	string
ivr_delay_second_cnt	int
gmt_trans_date_key  	int
pst_trans_date_key  	int
prim_purch_trvl_acct_key	int
gross_trans_cnt     	int
gross_bkg_amt_usd   	decimal(19,4)
gross_purch_price_amt_usd	decimal(19,4)
gross_purch_cost_amt_usd	decimal(19,4)
gross_cncl_price_amt_usd	decimal(19,4)
gross_cncl_cost_amt_usd	decimal(19,4)
totl_cost_amt_usd   	decimal(19,4)
margn_amt_usd       	decimal(19,4)
gross_purch_ordr_cnt	int
gross_purch_trans_cnt	int
gross_cncl_ordr_cnt 	int
gross_cncl_trans_cnt	int
gross_ordr_cnt      	int
gross_agncy_trans_cnt	int
gross_merch_trans_cnt	int
cust_ops_est_bk_rev_amt_usd	decimal(19,4)
itin_detail         	array<struct<SRC_SYS_ID:int,TPID:int,TRL:int,PRODUCT_CAT_KEY:int,PRODUCT_LN_NAME:string,RESPONDENT_ID:string,BUSINESS_PARTNR_KEY:int,ITIN_NBR:string,ORDER_NBR:bigint,ITIN_BK_AGNT_KEY:int,ITIN_BK_AGNT_HIRE_TENR_DAY:int,ITIN_BK_AGNT_ROLE_TENR_DAY:int,BK_AGNT_KEY:int,BK_DATE_KEY:int,AB_TST_GRP_ID:int,BEGIN_USE_DATE_KEY:int,BEGIN_USE_DATE:string,BK_DATE:string,BK_DATETM:string,BKG_IND_KEY:int,PKG_BKG_IND_KEY:int,BKG_PRODUCT_LN_COMPONENT_KEY:int,BKG_WINDW_KEY:int,COST_CURRN_KEY:int,COUPN_KEY:int,CUST_OPS_BKG_IND_KEY:int,END_USE_DATE_KEY:int,END_USE_DATE:string,LGL_ENTITY_KEY:int,MKTG_CODE_KEY:int,ORACLE_GL_PRODUCT_KEY:int,PRICE_CURRN_KEY:int,PRODUCT_LN_KEY:int,PST_TRANS_DATE_KEY:int,PST_TRANS_DATE:string,PST_TRANS_TM_KEY:int,AIR_TRIP_TYP_NAME:string,SAT_NIGHT_STAY_IND:string,AIR_BKG_IND_KEY:int,AIR_SETTLMNT_AGNT_TYP_KEY:int,PLATNG_CARRIER_KEY:int,PNR_REC_LOCATOR_CODE:string,TCKT_AIR_FARE_TYP_KEY:int,TCKT_ROUTE_KEY:int,TOUR_OPERATR_KEY:int,AGNT_ASST_IND:string,BKG_WINDW_RNG_NAME:string,CAR_CAT_NAME:string,CAR_TYP_NAME:string,CREDT_CARD_TYP_KEY:int,CAR_BASE_PRICE_PERIOD_KEY:int,CAR_BKG_IND_KEY:int,CAR_CLASS_KEY:int,CAR_DROP_OFF_LOC_KEY:int,CAR_PICK_UP_LOC_KEY:int,CAR_SPCL_EQUIP_GRP_KEY:int,CAR_VNDR_AGRMNT_KEY:int,CAR_VNDR_KEY:int,CRUIS_ADJ_REASN_KEY:int,CRUIS_CABN_TYP_KEY:int,CRUIS_RSDNC_STATE_PROVNC_KEY:int,CRUIS_SUB_DEST_KEY:int,DISEMBRK_PORT_KEY:int,EMBRK_PORT_KEY:int,SHIP_KEY:int,AGNT_TOUCH_IND:string,OFFRNG_ITM_KEY:int,DEST_SVC_SRCH_LOC_KEY:int,DEST_REGN_KEY:int,INS_OFFRNG_CAT_KEY:int,INS_OFFRNG_CAT_NAME:string,LENGTH_OF_STAY_RNG_NAME:string,EEM_PROPERTY_IND:string,EXPE_HALF_STAR_RTG:decimal(19,4),GDS_PROPERTY_IND:string,LODG_PROPERTY_NAME:string,MERCH_PROPERTY_IND:string,OPAQUE_PROPERTY_IND:string,PROPERTY_BRAND_NAME:string,PROPERTY_CNTRCT_MODEL_NAME:string,PROPERTY_CNTRY_NAME:string,PROPERTY_PARNT_CHAIN_NAME:string,PROPERTY_MKT_NAME:string,BKG_REFRL_SRC_KEY:int,DISTR_KEY:int,DOM_INTL_BKG_ITM_IND_KEY:int,LENGTH_OF_STAY_KEY:int,LODG_PROPERTY_KEY:int,LODG_RATE_PLN_KEY:int,LODG_RATE_RULE_KEY:int,ORDER_CONF_NBR:string,PRICE_STRUCT_KEY:int,TPID_CURRN_KEY:int,TRANS_TYP_KEY:int,TRVL_DURATN_KEY:int,OFFRNG_ITM_NAME:string,OFFRNG_NAME:string,PKG_BEGIN_USE_DATE:string,PKG_CAR_VNDR_1_KEY:int,PKG_CAR_VNDR_2_KEY:int,PKG_END_USE_DATE:string,PKG_LODG_PROPERTY_1_KEY:int,PKG_LODG_PROPERTY_2_KEY:int,PRICE_MODEL_NAME:string,FLEX_MOR_IND:string,PKG_TYP_NAME:string,TRANS_CAT_NAME:string,TRANS_TYP_DESC:string,TRANS_TYP_ID:int,TRANS_TYP_NAME:string,TRANS_USE_PERIOD_NAME:string,COST_CURRN_NAME:string,DISEMBRK_PORT_NAME:string,EMBRK_PORT_NAME:string,PLATNG_CARRIER_NAME:string,TCKT_DEST_AIRPT_CODE:string,TCKT_DEST_AIRPT_CNTRY_CODE:string,TCKT_DEST_AIRPT_CNTRY_NAME:string,TCKT_ORIGN_AIRPT_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_NAME:string,TCKT_ROUTE_NAME:string,BUSINESS_MODEL_NAME:string,BUSINESS_MODEL_SUBTYP_NAME:string,PKG_IND:string,BKG_ID:int,BKG_ITM_ID:int,BKG_SYS_OF_REC_ID:int,BKG_SYS_OF_REC_NAME:string,ITIN_CREATE_DATETM:string,ORDER_ID:bigint,ORDER_LN_SEQ_NBR:int,PROPERTY_LOCAL_BK_DATE_KEY:int,PROPERTY_LOCAL_BK_TM_KEY:int,PST_ITIN_CREATE_DATE:string,PST_ITIN_CREATE_DATE_KEY:int,PST_ITIN_CREATE_TM_KEY:int,TRANS_TM_KEY:int,PKG_ID:bigint,PKG_TRANS_TYP_KEY:int,BK_LANG_KEY:int,BK_LANG_CODE:string,BK_LANG_NAME:string,BK_GROSS_TRANS_CNT:int,BK_GROSS_BKG_AMT_USD:decimal(19,4),BK_GROSS_PURCH_PRICE_AMT_USD:decimal(19,4),BK_GROSS_PURCH_COST_AMT_USD:decimal(19,4),BK_GROSS_CNCL_PRICE_AMT_USD:decimal(19,4),BK_GROSS_CNCL_COST_AMT_USD:decimal(19,4),BK_TOTL_COST_AMT_USD:decimal(19,4),BK_MARGN_AMT_USD:decimal(19,4),BK_GROSS_PURCH_ORDR_CNT:int,BK_GROSS_PURCH_TRANS_CNT:int,BK_GROSS_CNCL_ORDR_CNT:int,BK_GROSS_CNCL_TRANS_CNT:int,BK_GROSS_ORDR_CNT:int,BK_GROSS_AGNCY_TRANS_CNT:int,BK_GROSS_MERCH_TRANS_CNT:int,BK_CUST_OPS_EST_BK_REV_AMT_USD:decimal(19,4),COUPN_PRICE_AMT_USD:decimal(19,4),EST_COST_OF_SALE_AMT_USD:decimal(19,4),EST_GROSS_PROFIT_AMT_USD:decimal(19,4),EST_NET_REV_AMT_USD:decimal(19,4),EST_VAR_COST_OF_SALE_AMT_USD:decimal(19,4),EST_VAR_GROSS_PROFIT_AMT_USD:decimal(19,4),FRNT_END_CMSN_AMT_USD:decimal(19,4),OTHR_COST_ADJ_AMT_USD:decimal(19,4),OTHR_DEST_SVC_TCKT_CNT:int,OTHR_FEE_COST_AMT_USD:decimal(19,4),OTHR_FEE_PRICE_AMT_USD:decimal(19,4),ADULT_CNT:int,AGNT_ASST_PURCH_FEE_AMT_USD:decimal(19,4),AGNT_TOUCH_CNT:int,BASE_COST_AMT_USD:decimal(19,4),BASE_PRICE_AMT_USD:decimal(19,4),AGNT_ASST_EXCH_FEE_AMT_USD:decimal(19,4),AGNT_ASST_REFUND_FEE_AMT_USD:decimal(19,4),AGNT_ASST_VOID_FEE_AMT_USD:decimal(19,4),BKG_FEE_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_COST_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_PRICE_AMT_USD:decimal(19,4),DELIVERY_FEE_COST_AMT_USD:decimal(19,4),DELIVERY_FEE_PRICE_AMT_USD:decimal(19,4),EXCH_PNLTY_COST_AMT_USD:decimal(19,4),EXCH_PNLTY_PRICE_AMT_USD:decimal(19,4),LAP_INFANT_CNT:int,PAPR_TCKT_FEE_AMT_USD:decimal(19,4),UNUSED_TCKT_COST_AMT_USD:decimal(19,4),UNUSED_TCKT_PRICE_AMT_USD:decimal(19,4),AIR_TRANS_SEG_CNT:int,AIR_TRANS_TCKT_CNT:int,PEAK_RATE_COST_ADJ_AMT_USD:decimal(19,4),PEAK_RATE_PRICE_ADJ_AMT_USD:decimal(19,4),OTHR_PRICE_ADJ_AMT_USD:decimal(19,4),PURE_MARGN_AMT_USD:decimal(19,4),RENTL_DAY_CNT:int,VAR_PRICE_ADJ_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_COST_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_CMSN_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_LODG_CMSN_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_COST_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_COST_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_CMSN_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_COST_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_PRICE_AMT_USD:decimal(19,4),PORT_CHRG_COST_AMT_USD:decimal(19,4),PORT_CHRG_PRICE_AMT_USD:decimal(19,4),SENIOR_CNT:int,OTHR_TAX_COST_AMT_USD:decimal(19,4),OTHR_TAX_PRICE_AMT_USD:decimal(19,4),NET_RETAIL_RATE_AMT_USD:decimal(19,4),MARKUP_AMT_USD:decimal(19,4),ADULT_DEST_SVC_TCKT_CNT:int,CHILD_DEST_SVC_TCKT_CNT:int,DEST_SVC_BKG_ITM_CNT:int,TOTL_DEST_SVC_TCKT_CNT:int,ADULT_INS_ITM_CNT:int,CHILD_INS_ITM_CNT:int,OTHR_INS_ITM_CNT:int,TOTL_INS_ITM_CNT:int,CNCL_PNLTY_WAIVR_PRICE_ADJ_AMT_USD:decimal(19,4),DYN_RATE_RULE_COST_AMT_USD:decimal(19,4),DYN_RATE_RULE_PRICE_AMT_USD:decimal(19,4),EMP_DISC_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),EXTRA_PERSN_COST_AMT_USD:decimal(19,4),EXTRA_PERSN_PRICE_AMT_USD:decimal(19,4),GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),GENRIC_COUPN_PRICE_AMT_USD:decimal(19,4),INFANT_CNT:int,LODG_PKG_SAVE_AMT_USD:decimal(19,4),LOYLTY_POINT_PRICE_ADJ_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_COST_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),MARGN_SALES_TAX_COST_AMT_USD:decimal(19,4),MARGN_SALES_TAX_PRICE_AMT_USD:decimal(19,4),NET_SVC_FEE_PRICE_AMT_USD:decimal(19,4),OCCUP_TAX_COST_AMT_USD:decimal(19,4),OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),RATE_PLN_RESTR_COST_AMT_USD:decimal(19,4),RATE_PLN_RESTR_PRICE_AMT_USD:decimal(19,4),REBATE_PRICE_AMT_USD:decimal(19,4),REFUND_PRICE_ADJ_AMT_USD:decimal(19,4),RM_NIGHT_CNT:int,SALES_TAX_COST_AMT_USD:decimal(19,4),SALES_TAX_PRICE_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_COST_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_PRICE_AMT_USD:decimal(19,4),STNDAL_HTL_PRICE_MOD_AMT_USD:decimal(19,4),SUPPL_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_PRICE_ADJ_AMT_USD:decimal(19,4),SVC_CHRG_COST_AMT_USD:decimal(19,4),SVC_CHRG_PRICE_AMT_USD:decimal(19,4),SVC_FEE_PRICE_AMT_USD:decimal(19,4),TCM_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_CMSN_AMT_USD:decimal(19,4),TOTL_COST_ADJ_AMT_USD:decimal(19,4),TOTL_FEE_COST_AMT_USD:decimal(19,4),TOTL_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_COST_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_PRICE_AMT_USD:decimal(19,4),TOTL_PERSN_CNT:int,TOTL_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_TAX_COST_AMT_USD:decimal(19,4),TOTL_TAX_PRICE_AMT_USD:decimal(19,4),VAR_MARGN_COST_ADJ_USD:decimal(19,4),CNCL_CHG_FEE_PRICE_AMT_USD:decimal(19,4),CHILD_CNT:int,TRANS_DATETM:string,AIR_PKG_SAVE_AMT_USD:decimal(19,4),PKG_AGNCY_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_CAR_CNT:int,PKG_AGNCY_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_RM_CNT:int,PKG_AGNCY_RM_NIGHT_UNIT_CNT:int,PKG_AGNCY_TCKT_UNIT_CNT:int,PKG_AGNCY_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_TRAIN_TCKT_UNIT_CNT:int,PKG_AIR_DURATN_DAY_CNT:int,PKG_AIR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AIR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_CNT:int,PKG_CAR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_CAR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_RENTL_DAY_UNIT_CNT:int,PKG_COST_ADJ_AMT_USD:decimal(19,4),PKG_CRUIS_CABN_UNIT_CNT:int,PKG_CRUIS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_FEE_PRICE_AMT_USD:decimal(19,4),PKG_DEST_SVC_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_MARGN_AMT_USD:decimal(19,4),PKG_DEST_SVC_TCKT_UNIT_CNT:int,PKG_INS_FEE_PRICE_AMT_USD:decimal(19,4),PKG_INS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_INS_ITM_UNIT_CNT:int,PKG_INS_MARGN_AMT_USD:decimal(19,4),PKG_LODG_FEE_PRICE_AMT_USD:decimal(19,4),PKG_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_LODG_MARGN_AMT_USD:decimal(19,4),PKG_MERCH_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_PRICE_ADJ_AMT_USD:decimal(19,4),PKG_SAVE_AMT_USD:decimal(19,4),PKG_SAVE_PRICE_AMT_USD:decimal(19,4),PKG_TAX_COST_AMT_USD:decimal(19,4),PKG_TAX_PRICE_AMT_USD:decimal(19,4),PKG_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_TRAIN_MARGN_AMT_USD:decimal(19,4),TOTL_PKG_FEE_COST_AMT_USD:decimal(19,4),TOTL_PKG_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_PKG_UNIT_CNT:int,DURATN_DAY_CNT:int,PKG_MERCH_CAR_CNT:int,PKG_MERCH_RM_CNT:int,PKG_MERCH_RM_NIGHT_UNIT_CNT:int,PKG_MERCH_TCKT_UNIT_CNT:int,PKG_MERCH_TRAIN_TCKT_UNIT_CNT:int,PKG_RM_CNT:int,PKG_RM_NIGHT_UNIT_CNT:int,PKG_TCKT_SEG_CNT:int,PKG_TCKT_UNIT_CNT:int,PKG_TOTL_TRVLR_CNT:int,PKG_TRAIN_DURATN_DAY_CNT:int,PKG_TRAIN_TCKT_UNIT_CNT:int,TRANS_AGNT_TOOL_NAME:string,TRANS_SRC_TYP_NAME:string,ONLINE_OFFLN_IND:string,MGMT_UNIT_KEY:int,MGMT_UNIT_CODE:string,MGMT_UNIT_NAME:string,MGMT_UNIT_LVL_1_NAME:string,MGMT_UNIT_LVL_2_NAME:string,MGMT_UNIT_LVL_3_NAME:string,MGMT_UNIT_LVL_4_NAME:string,MGMT_UNIT_LVL_5_NAME:string,MGMT_UNIT_LVL_6_NAME:string,MGMT_UNIT_LVL_7_NAME:string,MGMT_UNIT_LVL_8_NAME:string,MGMT_UNIT_LVL_9_NAME:string,MGMT_UNIT_LVL_10_NAME:string,MGMT_UNIT_LVL_11_NAME:string,MGMT_UNIT_LVL_12_NAME:string,MGMT_UNIT_LVL_13_NAME:string,MGMT_UNIT_LVL_14_NAME:string,PURCH_TRVL_ACCT_KEY:int,BKG_SERVICE_TYPE_NAME:string,BKG_SERVICE_CATEGORY_NAME:string,ETE_POS:string,ETE_FLAG:string,AGNT_ASST_CHG_FEE_AMT_USD:decimal(19,4),AGNT_ASST_CNCL_FEE_AMT_USD:decimal(19,4),SERV_FEE_TRANS_CNT:int,CNCL_FEE_TRANS_CNT:int,ABS_CHG_TRANS_CNT:int,ABS_CNCL_TRANS_CNT:int,BKG_AGENT_TIER_NAME:string>>
ngcc_trans_typ_skill_id	int
ngcc_trans_typ_skill_name	string
ngcc_product_skill_id	int
ngcc_product_skill_name	string
ngcc_lang_skill_id  	int
ngcc_lang_skill_name	string
cust_ops_ngcc_agnt_id	string
ngcc_query_typ_skill_id	int
ngcc_query_typ_skill_cat_name	string
ngcc_query_typ_skill_cat_desc	string
ngcc_seg_skill_id   	int
ngcc_seg_skill_cat_name	string
ngcc_seg_typ_skill_cat_desc	string
ngcc_extrnl_agnt_supprt_skill_id	int
ngcc_extrnl_agnt_supprt_skill_name	string
ngcc_extrnl_agnt_supprt_skill_desc	string
nav_int_case_id     	string
nav_int_typ_name    	string
nav_int_typ_reasn_name	string
nav_int_stat_name   	string
nav_int_local_currn 	string
nav_int_assign_queue_name	string
nav_int_cncl_case_ind	string
nav_int_guest_acct_case_ind	string
nav_int_anchor_ind  	string
nav_int_case_sla_typ_name	string
nav_int_sla_datetm  	string
nav_int_sla_goal_datetm	string
nav_int_create_org_name	string
nav_int_create_div_name	string
nav_int_create_org_unit_name	string
nav_int_create_wrk_grp_name	string
nav_int_resolve_org_name	string
nav_int_resolve_div_name	string
nav_int_resolve_org_unit_name	string
nav_int_resolve_wrk_grp_name	string
nav_int_cust_case_id	string
nav_int_cust_score_name	string
nav_int_cust_identity_verify_ind	string
nav_int_itin_nbr    	string
nav_int_tpid        	int
nav_int_trl         	bigint
nav_int_answr_second_cnt	bigint
nav_int_resolve_second_cnt	decimal(19,4)
nav_int_task_wrap_up_duratn_second_cnt	int
nav_int_nav_int_tm_second_cnt	decimal(19,4)
nav_int_no_of_item_create_cnt	int
nav_int_case_cnt    	int
nav_resolve_abandon_case_cnt	int
nav_resolve_cancel_case_cnt	int
nav_resolve_complete_case_cnt	int
nav_int_create_agnt_login_id	string
nav_int_create_agnt_key	int
nav_int_create_agnt_earns_cmsn_ind	string
nav_int_create_agnt_email_addr	string
nav_int_create_agnt_emp_typ_name	string
nav_int_create_agnt_frst_name	string
nav_int_create_agnt_middl_name	string
nav_int_create_agnt_last_name	string
nav_int_create_agnt_knwn_as_name	string
nav_int_create_agnt_hire_date	string
nav_int_create_agnt_termnatn_date	string
nav_int_create_agnt_job_title_name	string
nav_int_create_agnt_mgmt_div_name	string
nav_int_create_agnt_mgr_frst_name	string
nav_int_create_agnt_mgr_last_name	string
nav_int_create_agnt_prim_business_grp_name	string
nav_int_create_agnt_prim_typ_name	string
nav_int_create_agnt_profcncy_name	string
nav_int_create_agnt_role_name	string
nav_int_create_agnt_prim_lang_code	string
nav_int_create_agnt_prim_lang_name	string
nav_int_create_agnt_scndry_lang_code	string
nav_int_create_agnt_scndry_lang_name	string
nav_int_create_agnt_tertiary_lang_code	string
nav_int_create_agnt_tertiary_lang_name	string
nav_int_create_agnt_vndr_loc_name	string
nav_int_create_agnt_vndr_name	string
nav_int_create_agnt_hire_tenr_day	int
nav_int_create_agnt_role_tenr_day	int
nav_int_create_agnt_service_category_name	string
nav_int_update_agnt_login_id	string
nav_int_update_agnt_key	int
nav_int_resolve_agnt_login_id	string
nav_int_resolve_agnt_key	int
nav_tm_zone_key     	int
nav_tm_zone_name    	string
nav_int_source_create_datetm	string
nav_int_pst_create_date_key	int
nav_int_gmt_create_date_key	int
nav_int_aest_create_date_key	int
nav_int_pst_create_tm_key	int
nav_int_gmt_create_tm_key	int
nav_int_aest_create_tm_key	int
nav_int_source_update_datetm	string
nav_int_pst_update_date_key	int
nav_int_gmt_update_date_key	int
nav_int_aest_update_date_key	int
nav_int_pst_update_tm_key	int
nav_int_gmt_update_tm_key	int
nav_int_aest_update_tm_key	int
nav_int_source_resolve_datetm	string
nav_int_pst_resolve_date_key	int
nav_int_gmt_resolve_date_key	int
nav_int_aest_resolve_date_key	int
nav_int_pst_resolve_tm_key	int
nav_int_gmt_resolve_tm_key	int
nav_int_aest_resolve_tm_key	int
nav_int_em_lang_name	string
nav_int_em_lang_code	string
nav_int_em_lang_key 	int
nav_int_svc_list    	array<struct<SVC_CASE_ID:string,SVC_CASE_STAT_NAME:string,SVC_ASSIGN_QUEUE_NAME:string,SVC_INT_TYP_NAME:string,SVC_TRVL_STG_IND:string,SVC_LOCAL_CURRN:string,SVC_ERR_LOGIN_NAME:string,SVC_GUEST_ACCT_CASE_IND:string,SVC_CALLER_DISCONNECT_IND:string,SVC_CHG_EXECUTED_TYP_IND:string,SVC_COUPN_ISSUE_IND:string,SVC_COUPN_EXPR_DATE:string,SVC_VNDR_CONSULT_IND:string,SVC_REFUND_IND:string,SVC_WRITE_OFF_IND:string,SVC_ACTN_NAME:string,SVC_ACTN_DESC:string,SVC_MSSNG_RES_ACTN_CODE:string,SVC_MSSNG_RES_ACTN_NAME:string,SVC_GUEST_ACCT_IND:string,SVC_INS_IND:string,SVC_ANCHOR_IND:string,SVC_CUST_CALLBK_IND:string,SVC_CUST_CALLBK_DESC:string,SVC_COMPLAINT_IND:string,SVC_NEW_CASE_IND:string,SVC_CASE_CUST_EMAIL_ADDR:string,SVC_SLA_TYP_NAME:string,SVC_SLA_DATETM:string,SVC_SLA_GOAL_DATETM:string,SVC_ESCALATE_IND:string,SVC_ESCALATE_PARENT_CASE_ID:string,SVC_ESCALATE_ACTN_NAME:string,SVC_ESCALATE_COMMUNICATION_ISSUE_NAME:string,SVC_ESCALATE_FAULT_NAME:string,SVC_ESCALATE_ISSUE_TYP_NAME:string,SVC_ESCALATE_RESOLTN_NAME:string,SVC_ESCALATE_ROOT_CAUSE_NAME:string,SVC_ITIN_NBR:string,SVC_TPID:int,SVC_TPID_KEY:int,SVC_TUID:bigint,SVC_TRL:bigint,SVC_PRODUCT_CAT_NAME:string,SVC_PRODUCT_CAT_KEY:smallint,SVC_PRODUCT_CAT_ID:smallint,SVC_HOTEL_ID:bigint,SVC_LODG_PROPERTY_KEY:int,SVC_LODG_PROPERTY_NAME:string,SVC_PROPERTY_BRAND_NAME:string,SVC_PROPERTY_CITY_NAME:string,SVC_PROPERTY_CNTRY_CODE:string,SVC_PROPERTY_REGN_NAME:string,SVC_PROPERTY_PARNT_CHAIN_NAME:string,SVC_PROPERTY_TYP_NAME:string,SVC_BUSINESS_PARTNR_KEY:int,SVC_BUSINESS_PARTNR_NAME:string,SVC_ACTV_BUSINESS_PARTNR_IND:string,SVC_BUSINESS_PARTNR_MED_NAME:string,SVC_BUSINESS_PARTNR_MGMT_UNIT_NAME:string,SVC_BUSINESS_PARTNR_RPT_CO_NAME:string,SVC_BUSINESS_PARTNR_SEG_NAME:string,SVC_BUSINESS_PARTNR_SVC_BRAND_NAME:string,SVC_BUSINESS_PARTNR_SVC_CNTRY_NAME:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_DESC:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_NAME:string,SVC_BUSINESS_PARTNR_SYS_NAME:string,SVC_BUSINESS_PARTNR_TPID_NAME:string,SVC_MGMT_UNIT_KEY:int,SVC_MGMT_UNIT_NAME:string,SVC_MGMT_UNIT_LVL_1_NAME:string,SVC_MGMT_UNIT_LVL_2_NAME:string,SVC_MGMT_UNIT_LVL_3_NAME:string,SVC_MGMT_UNIT_LVL_4_NAME:string,SVC_MGMT_UNIT_LVL_5_NAME:string,SVC_MGMT_UNIT_LVL_6_NAME:string,SVC_MGMT_UNIT_LVL_7_NAME:string,SVC_MGMT_UNIT_LVL_8_NAME:string,SVC_MGMT_UNIT_LVL_9_NAME:string,SVC_MGMT_UNIT_LVL_10_NAME:string,SVC_MGMT_UNIT_LVL_11_NAME:string,SVC_MGMT_UNIT_LVL_12_NAME:string,SVC_MGMT_UNIT_LVL_13_NAME:string,SVC_MGMT_UNIT_LVL_14_NAME:string,SVC_ONLINE_OFFLN_IND_KEY:int,SVC_ONLINE_OFFLN_IND:string,SVC_PURCH_TRVL_ACCT_ID:string,SVC_PURCH_TRVL_ACCT_KEY:int,SVC_PURCH_TRVL_ACCT_FRST_NAME:string,SVC_PURCH_TRVL_ACCT_LAST_NAME:string,SVC_PURCH_TRVL_ACCT_EMAIL_ADDR:string,SVC_PURCH_TRVL_ACCT_ADDR_1:string,SVC_PURCH_TRVL_ACCT_ADDR_2:string,SVC_PURCH_TRVL_ACCT_CITY_NAME:string,SVC_PURCH_TRVL_ACCT_POSTAL_CODE:string,SVC_PURCH_TRVL_ACCT_STATE_PROVNC_NAME:string,SVC_PURCH_TRVL_ACCT_CNTRY_CODE:string,SVC_PURCH_TRVL_ACCT_CNTRY_NAME:string,SVC_PURCH_TRVL_ACCT_SUPER_REGN_NAME:string,SVC_PURCH_TRVL_ACCT_CREATE_DATE:string,SVC_BKG_TYP_CODE:string,SVC_BKG_TYP_NAME:string,SVC_BKG_TYP_BUSINESS_MODEL_NAME:string,SVC_AIRLN_CARRIER:string,SVC_AIRFARE_TYP_CODE:string,SVC_AIRFARE_TYP_NAME:string,SVC_FLGHT_SCHED_CHG_OPTN_IND:string,SVC_LOW_COST_CARRIER_IND:string,SVC_SECURE_FLGHT_IND:string,SVC_BRKAGE_CANDIDATE_IND:string,SVC_CUST_CASE_ID:string,SVC_CUST_SCORE_NAME:string,SVC_CUST_IDENTITY_VERIFY_IND:string,SVC_CREDT_EXPR_TYP_CODE:string,SVC_CREDT_EXPR_TYP_DESC:string,SVC_EARLY_CHK_OUT_IND:string,SVC_EXPE_REWARDS_IND:string,SVC_INTNT_NAME:string,SVC_INTNT_DESC:string,SVC_CAT_LVL_1_INTENT_NAME:string,SVC_CNCL_LVL_2_INTENT_CODE:string,SVC_CNCL_LVL_2_INTENT_NAME:string,SVC_CNCL_CHG_LVL_2_INTENT_CODE:string,SVC_CNCL_CHG_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_3_INTENT_NAME:string,SVC_COUPN_LVL_2_INTENT_CODE:string,SVC_COUPN_LVL_2_INTENT_NAME:string,SVC_COUPN_LVL_3_INTENT_CODE:string,SVC_COUPN_LVL_3_INTENT_NAME:string,SVC_CRUIS_AFFL_NAME:string,SVC_RECONFIRM_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_2_INTENT_CODE:string,SVC_REFUND_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_3_INTENT_CODE:string,SVC_REFUND_LVL_3_INTENT_NAME:string,SVC_REFUND_METHD_LVL_4_INTENT_NAME:string,SVC_REQST_LVL_2_INTENT_CODE:string,SVC_REQST_LVL_2_INTENT_NAME:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_CODE:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_NAME:string,SVC_CONSULT_LIST:array<struct<SVC_CONSULT_CASE_ESCALATE_ID:string,SVC_CONSULT_AGNT_ROLE_NAME:string,SVC_CONSULT_NOTE_TXT:string,SVC_CONSULT_REASN_NAME:string,SVC_CONSULT_VNDR_TYP_ID:int,SVC_CONSULT_VNDR_TYP_NAME:string,SVC_ESCALATE_TYP_ID:int,SVC_ESCALATE_TYP_NAME:string,SVC_ESCALATE_PARTY_TYP_NAME:string>>,BKG_CONF_ID:bigint,REFUND_APPRVR_FRST_NAME:string,REFUND_APPRVR_LAST_NAME:string,REFUND_APPRVR_TITLE_NAME:string,SVC_LOYLTY_POLICY_FIN_IMPACT_IND:string,SVC_LOYLTY_POLICY_NAME:string,SVC_LOYLTY_ACTIVITY_REVIEW_IND:string,SVC_LOYLTY_MEMBR_ID:bigint,SVC_CREATE_ORG_NAME:string,SVC_CREATE_DIV_NAME:string,SVC_CREATE_ORG_UNIT_NAME:string,SVC_CREATE_WRK_GRP_NAME:string,SVC_RESOLVE_ORG_NAME:string,SVC_RESOLVE_DIV_NAME:string,SVC_RESOLVE_ORG_UNIT_NAME:string,SVC_RESOLVE_WRK_GRP_NAME:string,SVC_S_CASE_CNT:int,SVC_E_CASE_CNT:int,SVC_O_CASE_CNT:int,SVC_ESCALATION_CASE_CNT:int,SVC_RESOLVE_SECOND_CNT:decimal(19,4),SVC_CREATE_AGNT_LOGIN_ID:string,SVC_CREATE_AGNT_KEY:int,SVC_CREATE_AGNT_EARNS_CMSN_IND:string,SVC_CREATE_AGNT_EMAIL_ADDR:string,SVC_CREATE_AGNT_EMP_TYP_NAME:string,SVC_CREATE_AGNT_FRST_NAME:string,SVC_CREATE_AGNT_MIDDL_NAME:string,SVC_CREATE_AGNT_LAST_NAME:string,SVC_CREATE_AGNT_KNWN_AS_NAME:string,SVC_CREATE_AGNT_HIRE_DATE:string,SVC_CREATE_AGNT_TERMNATN_DATE:string,SVC_CREATE_AGNT_JOB_TITLE_NAME:string,SVC_CREATE_AGNT_MGMT_DIV_NAME:string,SVC_CREATE_AGNT_MGR_FRST_NAME:string,SVC_CREATE_AGNT_MGR_LAST_NAME:string,SVC_CREATE_AGNT_PRIM_BUSINESS_GRP_NAME:string,SVC_CREATE_AGNT_PRIM_TYP_NAME:string,SVC_CREATE_AGNT_PROFCNCY_NAME:string,SVC_CREATE_AGNT_ROLE_NAME:string,SVC_CREATE_AGNT_PRIM_LANG_CODE:string,SVC_CREATE_AGNT_PRIM_LANG_NAME:string,SVC_CREATE_AGNT_SCNDRY_LANG_CODE:string,SVC_CREATE_AGNT_SCNDRY_LANG_NAME:string,SVC_CREATE_AGNT_TERTIARY_LANG_CODE:string,SVC_CREATE_AGNT_TERTIARY_LANG_NAME:string,SVC_CREATE_AGNT_VNDR_LOC_NAME:string,SVC_CREATE_AGNT_VNDR_NAME:string,SVC_CREATE_AGNT_HIRE_TENR_DAY:int,SVC_CREATE_AGNT_ROLE_TENR_DAY:int,SVC_CREATE_AGNT_SERVICE_CATEGORY_NAME:string,SVC_UPDATE_AGNT_LOGIN_ID:string,SVC_UPDATE_AGNT_KEY:int,SVC_RESOLVE_AGNT_LOGIN_ID:string,SVC_RESOLVE_AGNT_KEY:int,SVC_SOURCE_CREATE_DATETM:string,SVC_PST_CREATE_DATE_KEY:int,SVC_GMT_CREATE_DATE_KEY:int,SVC_AEST_CREATE_DATE_KEY:int,SVC_PST_CREATE_TM_KEY:int,SVC_GMT_CREATE_TM_KEY:int,SVC_AEST_CREATE_TM_KEY:int,SVC_SOURCE_UPDATE_DATETM:string,SVC_PST_UPDATE_DATE_KEY:int,SVC_GMT_UPDATE_DATE_KEY:int,SVC_AEST_UPDATE_DATE_KEY:int,SVC_PST_UPDATE_TM_KEY:int,SVC_GMT_UPDATE_TM_KEY:int,SVC_AEST_UPDATE_TM_KEY:int,SVC_SOURCE_RESOLVE_DATETM:string,SVC_PST_RESOLVE_DATE_KEY:int,SVC_GMT_RESOLVE_DATE_KEY:int,SVC_AEST_RESOLVE_DATE_KEY:int,SVC_PST_RESOLVE_TM_KEY:int,SVC_GMT_RESOLVE_TM_KEY:int,SVC_AEST_RESOLVE_TM_KEY:int,SVC_CASE_USER_EMAIL_ADDR:string,SVC_LANG_CODE:string,SVC_LANG_KEY:int,RESPONDENT_ID:string,SVC_GEN_LVL_2_INTENT_CODE:string,SVC_GEN_LVL_2_INTENT_NAME:string>>
call_eval_detail    	array<struct<CALL_EVAL_ID:int,CALL_EVAL_SITE_ID:int,CALL_EVAL_QUESTN_ID:int,CALL_EVAL_ANSWR_ID:int,CALL_EVAL_CREATE_DATETM:string,SRC_DB_VERSION_NBR:int,CALL_EVAL_UPDATE_DATETM:string,PST_CALL_EVAL_CREATE_DATE_KEY:int,PST_CALL_EVAL_CREATE_DATE:string,GMT_CALL_EVAL_CREATE_DATE_KEY:int,GMT_CALL_EVAL_CREATE_DATE:string,PST_CALL_EVAL_UPDATE_DATE_KEY:int,PST_CALL_EVAL_UPDATE_DATE:string,GMT_CALL_EVAL_UPDATE_DATE_KEY:int,GMT_CALL_EVAL_UPDATE_DATE:string,CALL_EVAL_AGNT_NICE_USER_ID:int,CALL_EVAL_AGNT_PERIPH_NBR:array<string>,CALL_EVAL_SEG_ID:string,CALL_EVAL_CASE_NBR:string,CALL_EVAL_ITIN_NBR:string,CALL_EVAL_SEG_DURATN_TM:string,CUST_OPS_CALL_EVAL_QUESTN_KEY:int,CALL_EVAL_QUESTN_SECTN_NAME:string,CALL_EVAL_QUESTN_SUB_SECTN_NAME:string,CALL_EVAL_QUESTN_CALL_OUTCOME:string,CALL_EVAL_QUESTN_SUB_SECTN_NBR:string,CALL_EVAL_QUESTN_NBR:string,CALL_EVAL_QUESTN_LBL_NAME:string,CALL_EVAL_QUESTN_CAPTN_NAME:string,CALL_EVAL_QUESTN_HIER_NBR:string,CALL_EVAL_QUESTN_TYP_ID:int,CALL_EVAL_QUESTN_SCORABLE_NBR:int,CALL_EVAL_QUESTN_IS_FATAL_NBR:int,CUST_OPS_CALL_EVAL_ANSWR_KEY:int,CALL_EVAL_ANSWR_SHORT_REASN_NAME:string,CALL_EVAL_ANSWR_DETAIL_REASN_NAME:string,CUST_OPS_CALL_EVAL_FORM_KEY:int,CALL_EVAL_FORM_ID:int,CALL_EVAL_FORM_TYP_NAME:string,CALL_EVAL_FORM_SRC_NAME:string,CALL_EVAL_FORM_NAME:string,CALL_EVAL_FORM_CREATE_DATETM:string,CALL_EVAL_FORM_UPDATE_DATETM:string,CALL_EVAL_FORM_ENABL_IND:string,CUST_OPS_CALL_EVALUATOR_KEY:int,CALL_EVAL_EVALUATOR_USER_ID:int,CALL_EVALUATOR_FULL_NAME:string,CALL_EVALUATOR_LOCATION_NAME:string,CUST_OPS_CALL_EVAL_TYP_KEY:int,CALL_EVAL_TYP_ID:int,CALL_EVAL_TYP_NAME:string,CUST_OPS_CALL_EVAL_AUTOFAIL_ANSWR_IND_KEY:int,CALL_EVAL_AUTOFAIL_ANSWR_IND:string,CALL_EVAL_AUTOFAIL_ANSWR_CNT:int,CALL_EVAL_ANSWR_CNT:int,POSSIBLE_POINT_NBR:decimal(19,4),EARN_POINT_NBR:decimal(19,4),ADJUSTED_POSSIBLE_POINT_CNT:decimal(19,4),ADJUSTED_EARN_POINT_CNT:decimal(19,4)>>
cvp_call_guid       	string
cvp_ab_test_ind     	string
cvp_call_start_date 	string
cvp_call_pst_start_date_key	int
cvp_call_gmt_start_date_key	int
cvp_call_start_datetm	string
cvp_call_end_datetm 	string
cvp_call_time_zone_key	int
cvp_call_time_zone  	string
cvp_call_ani        	string
cvp_cust_ops_phon_assign_key	int
cvp_call_dnis       	string
cvp_call_cnt        	int
cvp_auto_ani_cnt    	int
cvp_manual_ani_cnt  	int
cvp_auto_itin_cnt   	int
cvp_manual_itin_cnt 	int
cvp_override_itin_cnt	int
cvp_hang_up_time_brct_0_10_cnt	int
cvp_hang_up_time_brct_11_20_cnt	int
cvp_hang_up_time_brct_21_30_cnt	int
cvp_hang_up_time_brct_31_up_cnt	int
cvp_elemnt_list     	array<struct<CVP_SESSN_ID:bigint,CVP_SESSN_APP_KEY:int,CVP_SESSN_APP_NAME:string,CVP_SESSN_START_DATETM:string,CVP_SESSN_END_DATETM:string,CVP_SESSN_PRODUCT:string,CVP_SESSN_PRODUCT_CAT_KEY:int,CVP_SESSN_TRANS_TYP_CODE:string,CVP_SESSN_TRANS_TYP_KEY:int,CVP_SESSN_TRANS_TYP_NAME:string,CVP_SESSN_CALLER_NEED_CODE:string,CVP_SESSN_CALLER_NEED_KEY:int,CVP_SESSN_CALLER_NEED_NAME:string,CVP_SESSN_LANG_KEY:int,CVP_SESSN_LANG_CODE:string,CVP_SESSN_PRIME_LANG_CNT:bigint,CVP_SESSN_NON_PRIME_LANG_CNT:bigint,CVP_SESSN_BUSINESS_PARTNR_KEY:bigint,CVP_SESSN_BUSINESS_PARTNR_NAME:string,CVP_SESSN_DNIS_NAME:string,CVP_SESSN_ITIN:string,CVP_ELEMNT_ID:bigint,CVP_ELEMNT_KEY:int,CVP_ELEMNT_NAME:string,CVP_ELEMNT_START_DATETM:string,CVP_ELEMNT_END_DATETM:string,CVP_ELEMNT_NUM_INTERACT_CNT:int,CVP_ELEMNT_RESLT:int,CVP_ELEMNT_EXIT_STATE:string,CVP_LODG_CNCL_ELIG_CNT:int,CVP_LODG_CNCL_SUCCSS_CNT:int,CVP_LODG_CNCL_FAIL_CNT:int,CVP_CNCL_SECURE_VALID_METHOD_NAME:string,CVP_CNCL_SECURE_VALID_ZIP_CNT:int,CVP_CNCL_SECURE_VALID_LAST_FOUR_CNT:int,CVP_CNCL_SECURE_VALID_FAIL_CNT:int,CVP_CNCL_TERM_ACCPT_CNT:int,CVP_CNCL_TERM_DECLN_CNT:int,CVP_ANYTHING_ELSE_HUNG_UP_CNT:int,CVP_ANYTHING_ELSE_NEW_CNT:int,CVP_ANYTHING_ELSE_MAIN_MENU_CNT:int,CVP_PHON_NBR_PROMPT_CNT:int,CVP_PHON_FROM_ITIN_LKUP_CNT:int,CVP_ITIN_PROMPT_CNT:int,CVP_ITIN_LKUP_SUCCESS_CNT:int,CVP_ITIN_LKUP_FAIL_CNT:int,CVP_ITIN_SELCT_DIFF_CNT:int,CVP_BKG_MENU_NO_INPUT_CNT:int,CVP_BKG_MENU_HUNG_UP_CNT:int,CVP_ELEMNT_OPT_OUT_CNT:int,CVP_ELEMNT_HANG_UP_CNT:int,CVP_ELEMNT_DURATION:bigint,CVP_ELEMNT_REPT_CNT:int,CVP_ELEMNT_MAX_NO_INPUT_CNT:int,CVP_ELEMNT_DTFM_INPUT_CNT:bigint,CVP_ELEMNT_VOICE_INPUT_CNT:bigint,CVP_ELEMNT_UTTERANCE:string,CVP_ELEMNT_INTERPRETATION:string,CVP_ELEMNT_VOICE_CONFIDENCE_PCT:string,CVP_ELEMNT_NO_INPUT_CNT:int,CVP_ELEMNT_REPEAT_TERMINATE_CNT:int>>
respondent_id       	string
load_tag            	bigint
call_service_agnt_typ	string
call_agent_tier_name	string
caller_service_type 	string
caller_loyalty_name 	string
nav_int_create_agnt_service_tier_name	string
cust_ops_call_src_sys_name	string
partition_day       	string

# Partition Information
# col_name            	data_type           	comment

cust_ops_call_src_sys_name	string
partition_day       	string

Detailed Partition Information	Partition(values:[BKG, 2013-01-01], dbName:onprem_conversation, tableName:hadoop_dm_cust_ops_call_bkg_detail, createTime:1695341797, lastAccessTime:0, sd:StorageDescriptor(cols:[FieldSchema(name:cust_ops_call_id, type:string, comment:null), FieldSchema(name:cust_ops_call_seg_seq_nbr, type:int, comment:null), FieldSchema(name:trans_date_key, type:int, comment:null), FieldSchema(name:trans_agnt_key, type:int, comment:null), FieldSchema(name:agnt_skill_target_id, type:int, comment:null), FieldSchema(name:router_call_day_id, type:int, comment:null), FieldSchema(name:router_call_id, type:int, comment:null), FieldSchema(name:src_call_start_datetm, type:string, comment:null), FieldSchema(name:src_call_end_datetm, type:string, comment:null), FieldSchema(name:aest_call_start_date_key, type:int, comment:null), FieldSchema(name:gmt_call_start_date_key, type:int, comment:null), FieldSchema(name:pst_call_start_date_key, type:int, comment:null), FieldSchema(name:pst_call_end_date_key, type:int, comment:null), FieldSchema(name:pst_call_start_tm_key, type:int, comment:null), FieldSchema(name:pst_call_end_tm_key, type:int, comment:null), FieldSchema(name:agnt_periph_nbr, type:string, comment:null), FieldSchema(name:call_itin_nbr, type:string, comment:null), FieldSchema(name:inbnd_dialed_nbr, type:string, comment:null), FieldSchema(name:outbnd_dialed_nbr, type:string, comment:null), FieldSchema(name:ani_nbr, type:string, comment:null), FieldSchema(name:cust_ops_ngcc_agnt_sessn_id, type:string, comment:null), FieldSchema(name:cust_ops_ngcc_media_id, type:string, comment:null), FieldSchema(name:orignl_ani_nbr, type:string, comment:null), FieldSchema(name:orignl_cust_ops_ngcc_cntct_id, type:string, comment:null), FieldSchema(name:vq_inbnd_call_id, type:string, comment:null), FieldSchema(name:vq_return_call_id, type:string, comment:null), FieldSchema(name:cust_ops_agnt_key, type:int, comment:null), FieldSchema(name:cust_ops_agnt_id, type:int, comment:null), FieldSchema(name:call_agnt_frst_name, type:string, comment:null), FieldSchema(name:call_agnt_middl_name, type:string, comment:null), FieldSchema(name:call_agnt_last_name, type:string, comment:null), FieldSchema(name:call_agnt_job_title_name, type:string, comment:null), FieldSchema(name:call_agnt_hire_date, type:string, comment:null), FieldSchema(name:call_agnt_hire_tenr_day, type:int, comment:null), FieldSchema(name:call_agnt_termnatn_date, type:string, comment:null), FieldSchema(name:call_agnt_vndr_loc_id, type:int, comment:null), FieldSchema(name:call_agnt_vndr_loc_name, type:string, comment:null), FieldSchema(name:call_agnt_vndr_id, type:smallint, comment:null), FieldSchema(name:call_agnt_vndr_name, type:string, comment:null), FieldSchema(name:call_agnt_role_id, type:smallint, comment:null), FieldSchema(name:call_agnt_role_name, type:string, comment:null), FieldSchema(name:call_agnt_role_tenr_day, type:int, comment:null), FieldSchema(name:call_agnt_prim_cust_ops_typ_id, type:smallint, comment:null), FieldSchema(name:call_agnt_prim_cust_ops_typ_name, type:string, comment:null), FieldSchema(name:call_agnt_profcncy_id, type:smallint, comment:null), FieldSchema(name:call_agnt_profcncy_name, type:string, comment:null), FieldSchema(name:call_agnt_mgr_frst_name, type:string, comment:null), FieldSchema(name:call_agnt_mgr_last_name, type:string, comment:null), FieldSchema(name:call_agnt_full_name, type:string, comment:null), FieldSchema(name:call_agnt_mgr_full_name, type:string, comment:null), FieldSchema(name:call_agnt_typ_id, type:int, comment:null), FieldSchema(name:call_agnt_typ_name, type:string, comment:null), FieldSchema(name:call_business_partnr_key, type:int, comment:null), FieldSchema(name:call_business_partnr_id, type:int, comment:null), FieldSchema(name:call_business_partnr_sys_name, type:string, comment:null), FieldSchema(name:call_expe_business_partnr_id, type:int, comment:null), FieldSchema(name:call_ian_business_partnr_id, type:int, comment:null), FieldSchema(name:call_as400_business_partnr_src_code, type:string, comment:null), FieldSchema(name:call_business_partnr_name, type:string, comment:null), FieldSchema(name:call_business_partnr_tpid, type:int, comment:null), FieldSchema(name:call_business_partnr_tpid_name, type:string, comment:null), FieldSchema(name:call_business_partnr_svc_brand_name, type:string, comment:null), FieldSchema(name:call_business_partnr_svc_short_cntry_code, type:string, comment:null), FieldSchema(name:call_business_partnr_svc_cntry_name, type:string, comment:null), FieldSchema(name:call_business_partnr_svc_super_regn_name, type:string, comment:null), FieldSchema(name:call_business_partnr_svc_super_regn_desc, type:string, comment:null), FieldSchema(name:call_actv_business_partnr_ind, type:string, comment:null), FieldSchema(name:call_business_partnr_acct_mgr_name, type:string, comment:null), FieldSchema(name:call_business_partnr_b2b_billng_typ_name, type:string, comment:null), FieldSchema(name:call_business_partnr_co_name, type:string, comment:null), FieldSchema(name:call_business_partnr_home_url, type:string, comment:null), FieldSchema(name:call_business_partnr_med_name, type:string, comment:null), FieldSchema(name:call_business_partnr_mgmt_unit_code, type:string, comment:null), FieldSchema(name:call_business_partnr_mgmt_unit_name, type:string, comment:null), FieldSchema(name:call_business_partnr_oper_regn_name, type:string, comment:null), FieldSchema(name:call_business_partnr_rpt_co_code, type:string, comment:null), FieldSchema(name:call_business_partnr_rpt_co_name, type:string, comment:null), FieldSchema(name:call_business_partnr_seg_name, type:string, comment:null), FieldSchema(name:call_business_partnr_site_platform_name, type:string, comment:null), FieldSchema(name:call_business_partnr_start_date, type:string, comment:null), FieldSchema(name:call_business_partnr_website_domain_name, type:string, comment:null), FieldSchema(name:call_parnt_business_partnr_id, type:int, comment:null), FieldSchema(name:call_parnt_business_partnr_name, type:string, comment:null), FieldSchema(name:call_tpid_key, type:int, comment:null), FieldSchema(name:call_tpid, type:int, comment:null), FieldSchema(name:call_tpid_name, type:string, comment:null), FieldSchema(name:call_tpid_cntry_code, type:string, comment:null), FieldSchema(name:call_tpid_cntry_name, type:string, comment:null), FieldSchema(name:call_tpid_hwire_pos_code, type:string, comment:null), FieldSchema(name:call_mgmt_unit_key, type:smallint, comment:null), FieldSchema(name:call_mgmt_unit_code, type:string, comment:null), FieldSchema(name:call_mgmt_unit_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_1_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_2_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_3_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_4_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_5_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_6_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_7_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_8_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_9_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_10_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_11_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_12_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_13_name, type:string, comment:null), FieldSchema(name:call_mgmt_unit_lvl_14_name, type:string, comment:null), FieldSchema(name:call_lang_key, type:int, comment:null), FieldSchema(name:call_lang_code, type:string, comment:null), FieldSchema(name:call_lang_name, type:string, comment:null), FieldSchema(name:call_product_cat_key, type:smallint, comment:null), FieldSchema(name:call_product_cat_name, type:string, comment:null), FieldSchema(name:call_service_type_name, type:string, comment:null), FieldSchema(name:call_service_category_name, type:string, comment:null), FieldSchema(name:cust_ops_call_typ_key, type:int, comment:null), FieldSchema(name:cust_ops_call_typ_id, type:int, comment:null), FieldSchema(name:call_typ_code_list, type:string, comment:null), FieldSchema(name:call_typ_desc, type:string, comment:null), FieldSchema(name:call_typ_caller_need_id, type:smallint, comment:null), FieldSchema(name:call_typ_caller_need_code, type:string, comment:null), FieldSchema(name:call_typ_caller_need_name, type:string, comment:null), FieldSchema(name:call_typ_cust_ops_product_typ_id, type:smallint, comment:null), FieldSchema(name:call_typ_cust_ops_product_typ_code, type:string, comment:null), FieldSchema(name:call_typ_cust_ops_product_typ_name, type:string, comment:null), FieldSchema(name:call_typ_cust_trans_typ_id, type:smallint, comment:null), FieldSchema(name:call_typ_cust_trans_typ_code, type:string, comment:null), FieldSchema(name:call_typ_cust_trans_typ_name, type:string, comment:null), FieldSchema(name:call_typ_call_seg_grp_code, type:string, comment:null), FieldSchema(name:call_typ_call_seg_grp_name, type:string, comment:null), FieldSchema(name:call_typ_phon_site_chnnl_plcmnt_code, type:string, comment:null), FieldSchema(name:call_typ_phon_site_chnnl_plcmnt_name, type:string, comment:null), FieldSchema(name:call_typ_route_typ_code, type:string, comment:null), FieldSchema(name:call_typ_staff_grp_code, type:string, comment:null), FieldSchema(name:cust_ops_skill_grp_key, type:int, comment:null), FieldSchema(name:skill_grp_id, type:int, comment:null), FieldSchema(name:skill_grp_code_list, type:string, comment:null), FieldSchema(name:skill_grp_desc, type:string, comment:null), FieldSchema(name:base_skill_grp_caller_need_id, type:smallint, comment:null), FieldSchema(name:base_skill_grp_caller_need_code, type:string, comment:null), FieldSchema(name:base_skill_grp_caller_need_name, type:string, comment:null), FieldSchema(name:base_skill_grp_cust_ops_product_typ_id, type:smallint, comment:null), FieldSchema(name:base_skill_grp_cust_ops_product_typ_code, type:string, comment:null), FieldSchema(name:base_skill_grp_cust_ops_product_typ_name, type:string, comment:null), FieldSchema(name:base_skill_grp_cust_trans_typ_id, type:smallint, comment:null), FieldSchema(name:base_skill_grp_cust_trans_typ_code, type:string, comment:null), FieldSchema(name:base_skill_grp_cust_trans_typ_name, type:string, comment:null), FieldSchema(name:base_skill_grp_call_seg_grp_code, type:string, comment:null), FieldSchema(name:base_skill_grp_call_seg_grp_name, type:string, comment:null), FieldSchema(name:base_skill_grp_desc, type:string, comment:null), FieldSchema(name:base_skill_grp_code_list, type:string, comment:null), FieldSchema(name:base_skill_grp_phon_site_chnnl_plcmnt_code, type:string, comment:null), FieldSchema(name:base_skill_grp_phon_site_chnnl_plcmnt_name, type:string, comment:null), FieldSchema(name:base_skill_grp_staff_grp_code, type:string, comment:null), FieldSchema(name:frcast_grp_call_seg_grp_code, type:string, comment:null), FieldSchema(name:frcast_grp_call_seg_grp_name, type:string, comment:null), FieldSchema(name:frcast_grp_caller_need_code, type:string, comment:null), FieldSchema(name:frcast_grp_caller_need_name, type:string, comment:null), FieldSchema(name:frcast_grp_cust_ops_product_typ_code, type:string, comment:null), FieldSchema(name:frcast_grp_cust_ops_product_typ_name, type:string, comment:null), FieldSchema(name:frcast_grp_cust_trans_typ_code, type:string, comment:null), FieldSchema(name:frcast_grp_cust_trans_typ_name, type:string, comment:null), FieldSchema(name:frcast_grp_lang_code, type:string, comment:null), FieldSchema(name:frcast_grp_lang_name, type:string, comment:null), FieldSchema(name:skill_grp_profcncy_code, type:string, comment:null), FieldSchema(name:cust_ops_icrs_call_periph_dispostn_typ_key, type:int, comment:null), FieldSchema(name:cust_ops_icrs_periph_call_typ_id, type:int, comment:null), FieldSchema(name:cust_ops_icrs_call_dispostn_typ_id, type:int, comment:null), FieldSchema(name:periph_call_typ_name, type:string, comment:null), FieldSchema(name:call_dispostn_typ_name, type:string, comment:null), FieldSchema(name:call_dispostn_name, type:string, comment:null), FieldSchema(name:call_sys_err, type:string, comment:null), FieldSchema(name:call_sys_gb, type:string, comment:null), FieldSchema(name:call_sys_partval, type:string, comment:null), FieldSchema(name:call_sys_ref_id, type:string, comment:null), FieldSchema(name:call_sys_lodg_property_name, type:string, comment:null), FieldSchema(name:call_typ_var_cust_ops_brand_name, type:string, comment:null), FieldSchema(name:call_typ_var_bkg_windw_name, type:string, comment:null), FieldSchema(name:call_typ_var_caller_need_code, type:string, comment:null), FieldSchema(name:call_typ_var_caller_need_name, type:string, comment:null), FieldSchema(name:call_typ_var_intl_dom_code, type:string, comment:null), FieldSchema(name:call_typ_var_intl_dom_name, type:string, comment:null), FieldSchema(name:call_typ_var_cust_ops_product_typ_code, type:string, comment:null), FieldSchema(name:call_typ_var_cust_ops_product_typ_name, type:string, comment:null), FieldSchema(name:call_typ_var_cust_trans_typ_code, type:string, comment:null), FieldSchema(name:call_typ_var_cust_trans_typ_name, type:string, comment:null), FieldSchema(name:call_typ_var_phon_site_chnnl_plcmnt_code, type:string, comment:null), FieldSchema(name:call_typ_var_phon_site_chnnl_plcmnt_name, type:string, comment:null), FieldSchema(name:call_typ_var_cust_ops_pos_code, type:string, comment:null), FieldSchema(name:call_typ_var_cust_ops_pos_name, type:string, comment:null), FieldSchema(name:call_typ_var_cust_ops_sub_brand_name, type:string, comment:null), FieldSchema(name:call_typ_var_trvl_duratn_code, type:string, comment:null), FieldSchema(name:call_typ_var_trvl_duratn_name, type:string, comment:null), FieldSchema(name:call_exprnce_var_intfc_fails, type:string, comment:null), FieldSchema(name:call_exprnce_var_lang_list, type:string, comment:null), FieldSchema(name:call_sys_var_media_publctn_id, type:string, comment:null), FieldSchema(name:call_exprnce_var_persna, type:string, comment:null), FieldSchema(name:target_seg_call_typ_key, type:int, comment:null), FieldSchema(name:target_seg_call_typ_id, type:int, comment:null), FieldSchema(name:target_seg_call_typ_code_list, type:string, comment:null), FieldSchema(name:target_seg_call_typ_desc, type:string, comment:null), FieldSchema(name:target_seg_call_typ_caller_need_id, type:smallint, comment:null), FieldSchema(name:target_seg_call_typ_caller_need_code, type:string, comment:null), FieldSchema(name:target_seg_call_typ_caller_need_name, type:string, comment:null), FieldSchema(name:target_seg_call_typ_cust_ops_product_typ_id, type:smallint, comment:null), FieldSchema(name:target_seg_call_typ_cust_ops_product_typ_code, type:string, comment:null), FieldSchema(name:target_seg_call_typ_cust_ops_product_typ_name, type:string, comment:null), FieldSchema(name:target_seg_call_typ_cust_trans_typ_id, type:smallint, comment:null), FieldSchema(name:target_seg_call_typ_cust_trans_typ_code, type:string, comment:null), FieldSchema(name:target_seg_call_typ_cust_trans_typ_name, type:string, comment:null), FieldSchema(name:target_seg_call_typ_call_seg_grp_code, type:string, comment:null), FieldSchema(name:target_seg_call_typ_call_seg_grp_name, type:string, comment:null), FieldSchema(name:target_seg_call_typ_phon_site_chnnl_plcmnt_code, type:string, comment:null), FieldSchema(name:target_seg_call_typ_phon_site_chnnl_plcmnt_name, type:string, comment:null), FieldSchema(name:target_seg_call_typ_route_typ_code, type:string, comment:null), FieldSchema(name:target_seg_call_typ_staff_grp_code, type:string, comment:null), FieldSchema(name:call_seg_state_ind, type:string, comment:null), FieldSchema(name:vq_call_ind, type:string, comment:null), FieldSchema(name:vq_call_state_ind, type:string, comment:null), FieldSchema(name:cust_ops_phon_assign_key, type:int, comment:null), FieldSchema(name:cust_ops_phon_assign_id, type:int, comment:null), FieldSchema(name:cust_ops_phon_id, type:int, comment:null), FieldSchema(name:phon_name, type:string, comment:null), FieldSchema(name:cust_ops_phon_typ_id, type:smallint, comment:null), FieldSchema(name:cust_ops_phon_typ_name, type:string, comment:null), FieldSchema(name:phon_carrier_id, type:smallint, comment:null), FieldSchema(name:phon_carrier_name, type:string, comment:null), FieldSchema(name:phon_cntry_prfx_nbr, type:string, comment:null), FieldSchema(name:phon_cntry_name, type:string, comment:null), FieldSchema(name:cust_ops_phon_regn_id, type:smallint, comment:null), FieldSchema(name:cust_ops_phon_regn_name, type:string, comment:null), FieldSchema(name:phon_vanity_desc, type:string, comment:null), FieldSchema(name:phon_local_nbr, type:string, comment:null), FieldSchema(name:intl_phon_nbr, type:string, comment:null), FieldSchema(name:rcf_phon_nbr, type:string, comment:null), FieldSchema(name:rcf_phon_carrier_id, type:smallint, comment:null), FieldSchema(name:rcf_phon_carrier_name, type:string, comment:null), FieldSchema(name:phon_acquir_date, type:string, comment:null), FieldSchema(name:phon_retire_date, type:string, comment:null), FieldSchema(name:phon_carrier_start_datetm, type:string, comment:null), FieldSchema(name:phon_carrier_end_datetm, type:string, comment:null), FieldSchema(name:phon_carrier_acct_nbr, type:string, comment:null), FieldSchema(name:phon_assign_format_phon_txt, type:string, comment:null), FieldSchema(name:phon_assign_business_partnr_key, type:int, comment:null), FieldSchema(name:phon_assign_start_datetm, type:string, comment:null), FieldSchema(name:phon_assign_end_datetm, type:string, comment:null), FieldSchema(name:phon_assign_parnt_phon_id, type:int, comment:null), FieldSchema(name:phon_assign_cust_ops_branding_cat_id, type:smallint, comment:null), FieldSchema(name:phon_assign_cust_ops_branding_cat_name, type:string, comment:null), FieldSchema(name:phon_assign_cust_ops_brand_lvl_1_id, type:smallint, comment:null), FieldSchema(name:phon_assign_cust_ops_brand_lvl_1_name, type:string, comment:null), FieldSchema(name:phon_assign_cust_ops_brand_lvl_2_id, type:smallint, comment:null), FieldSchema(name:phon_assign_cust_ops_brand_lvl_2_name, type:string, comment:null), FieldSchema(name:phon_assign_cust_ops_brand_lvl_3_id, type:smallint, comment:null), FieldSchema(name:phon_assign_cust_ops_brand_lvl_3_name, type:string, comment:null), FieldSchema(name:phon_assign_cust_ops_pos_id, type:smallint, comment:null), FieldSchema(name:phon_assign_cust_ops_pos_code, type:string, comment:null), FieldSchema(name:phon_assign_cust_ops_pos_desc, type:string, comment:null), FieldSchema(name:phon_assign_iso_lang_code, type:string, comment:null), FieldSchema(name:phon_assign_iso_lang_name, type:string, comment:null), FieldSchema(name:phon_assign_cust_ops_product_typ_id, type:smallint, comment:null), FieldSchema(name:phon_assign_cust_ops_product_typ_name, type:string, comment:null), FieldSchema(name:phon_assign_mktg_chnnl_name_1, type:string, comment:null), FieldSchema(name:phon_assign_mktg_chnnl_name_2, type:string, comment:null), FieldSchema(name:phon_assign_mktg_cmpgn_lvl_1_id, type:smallint, comment:null), FieldSchema(name:phon_assign_mktg_cmpgn_lvl_1_name, type:string, comment:null), FieldSchema(name:phon_assign_mktg_cmpgn_lvl_2_id, type:smallint, comment:null), FieldSchema(name:phon_assign_mktg_cmpgn_lvl_2_name, type:string, comment:null), FieldSchema(name:phon_assign_media_typ_lvl_1_id, type:smallint, comment:null), FieldSchema(name:phon_assign_media_typ_lvl_1_name, type:string, comment:null), FieldSchema(name:phon_assign_media_typ_lvl_2_id, type:smallint, comment:null), FieldSchema(name:phon_assign_media_typ_lvl_2_name, type:string, comment:null), FieldSchema(name:phon_assign_media_typ_lvl_3_id, type:smallint, comment:null), FieldSchema(name:phon_assign_media_typ_lvl_3_name, type:string, comment:null), FieldSchema(name:phon_assign_publctn_typ_lvl_1_id, type:smallint, comment:null), FieldSchema(name:phon_assign_publctn_typ_lvl_1_name, type:string, comment:null), FieldSchema(name:phon_assign_publctn_typ_lvl_2_id, type:smallint, comment:null), FieldSchema(name:phon_assign_publctn_typ_lvl_2_name, type:string, comment:null), FieldSchema(name:phon_assign_site_plcmnt_lvl_1_id, type:smallint, comment:null), FieldSchema(name:phon_assign_site_plcmnt_lvl_1_name, type:string, comment:null), FieldSchema(name:phon_assign_site_plcmnt_lvl_2_id, type:smallint, comment:null), FieldSchema(name:phon_assign_site_plcmnt_lvl_2_name, type:string, comment:null), FieldSchema(name:phon_assign_site_chnnl_plcmnt_id, type:smallint, comment:null), FieldSchema(name:phon_assign_site_chnnl_plcmnt_code, type:string, comment:null), FieldSchema(name:phon_assign_site_chnnl_plcmnt_name, type:string, comment:null), FieldSchema(name:phon_assign_mktg_effrt_desc, type:string, comment:null), FieldSchema(name:phon_assign_srch_cat_id, type:smallint, comment:null), FieldSchema(name:phon_assign_srch_cat_name, type:string, comment:null), FieldSchema(name:phon_assign_srch_term_desc, type:string, comment:null), FieldSchema(name:phon_assign_holder_id, type:smallint, comment:null), FieldSchema(name:phon_assign_holder_frst_name, type:string, comment:null), FieldSchema(name:phon_assign_holder_last_name, type:string, comment:null), FieldSchema(name:phon_assign_desc, type:string, comment:null), FieldSchema(name:phon_assign_creative_id, type:smallint, comment:null), FieldSchema(name:phon_assign_creative_name, type:string, comment:null), FieldSchema(name:phon_assign_call_to_actn_id, type:smallint, comment:null), FieldSchema(name:phon_assign_call_to_actn_name, type:string, comment:null), FieldSchema(name:phon_assign_ivr_exprnce_id, type:int, comment:null), FieldSchema(name:phon_assign_ivr_exprnce_code_list, type:string, comment:null), FieldSchema(name:phon_assign_ivr_exprnce_desc, type:string, comment:null), FieldSchema(name:phon_assign_hrs_of_operatn_desc, type:string, comment:null), FieldSchema(name:phon_assign_cost_per_minute_desc, type:string, comment:null), FieldSchema(name:phon_assign_prk_until_datetm, type:string, comment:null), FieldSchema(name:phon_assign_rpt_dnp_ind, type:string, comment:null), FieldSchema(name:cust_ops_call_src_tm_zone_key, type:int, comment:null), FieldSchema(name:cust_ops_call_src_tm_zone_id, type:int, comment:null), FieldSchema(name:cust_ops_call_src_tm_zone_name, type:string, comment:null), FieldSchema(name:cust_ops_call_duratn_windw_key, type:int, comment:null), FieldSchema(name:cust_ops_ngcc_cntct_dirctn_key, type:int, comment:null), FieldSchema(name:cust_ops_ngcc_dirctn_id, type:int, comment:null), FieldSchema(name:ngcc_cntct_dirctn_name, type:string, comment:null), FieldSchema(name:cust_ops_ngcc_cntct_disconnect_typ_key, type:int, comment:null), FieldSchema(name:cust_ops_ngcc_cntct_disconnect_reasn_id, type:int, comment:null), FieldSchema(name:ngcc_cntct_disconnect_typ_name, type:string, comment:null), FieldSchema(name:cust_ops_ngcc_dispostn_typ_id, type:int, comment:null), FieldSchema(name:ngcc_cntct_dispostn_typ_name, type:string, comment:null), FieldSchema(name:agnt_handled_ind, type:string, comment:null), FieldSchema(name:cust_disconnect_ind, type:string, comment:null), FieldSchema(name:handled_wthn_sla_ind, type:string, comment:null), FieldSchema(name:ivr_handled_ind, type:string, comment:null), FieldSchema(name:outbnd_ind, type:string, comment:null), FieldSchema(name:cust_ops_ngcc_comptncy_grp_key, type:int, comment:null), FieldSchema(name:cust_ops_ngcc_comptncy_grp_id, type:int, comment:null), FieldSchema(name:agnt_ngcc_comptncy_grp_name, type:string, comment:null), FieldSchema(name:short_call_ind, type:string, comment:null), FieldSchema(name:zero_talk_tm_ind, type:string, comment:null), FieldSchema(name:agnt_disconnect_cnt, type:int, comment:null), FieldSchema(name:attch_outbnd_cnt, type:int, comment:null), FieldSchema(name:handle_cnt, type:int, comment:null), FieldSchema(name:unattch_outbnd_cnt, type:int, comment:null), FieldSchema(name:aftr_call_wrk_second_cnt, type:int, comment:null), FieldSchema(name:attch_outbnd_second_cnt, type:int, comment:null), FieldSchema(name:hold_second_cnt, type:int, comment:null), FieldSchema(name:talk_second_cnt, type:int, comment:null), FieldSchema(name:totl_handle_second_cnt, type:int, comment:null), FieldSchema(name:unattch_outbnd_second_cnt, type:int, comment:null), FieldSchema(name:hold_call_ind, type:string, comment:null), FieldSchema(name:system_disconnect_cnt, type:int, comment:null), FieldSchema(name:abandon_second_cnt, type:int, comment:null), FieldSchema(name:netwrk_abandon_second_cnt, type:int, comment:null), FieldSchema(name:trnsfr_abandon_second_cnt, type:int, comment:null), FieldSchema(name:totl_offr_cnt, type:int, comment:null), FieldSchema(name:inbnd_queue_offr_cnt, type:int, comment:null), FieldSchema(name:transfr_queue_offr_cnt, type:int, comment:null), FieldSchema(name:answr_second_cnt, type:int, comment:null), FieldSchema(name:netwrk_answr_second_cnt, type:int, comment:null), FieldSchema(name:trnsfr_answr_second_cnt, type:int, comment:null), FieldSchema(name:answr_20_second_call_ind, type:string, comment:null), FieldSchema(name:answr_30_second_call_ind, type:string, comment:null), FieldSchema(name:answr_60_second_call_ind, type:string, comment:null), FieldSchema(name:answr_120_second_call_ind, type:string, comment:null), FieldSchema(name:agnt_conect_attmpt_cnt, type:int, comment:null), FieldSchema(name:agnt_trnsfr_cnt, type:int, comment:null), FieldSchema(name:arrv_cnt, type:int, comment:null), FieldSchema(name:block_call_cnt, type:int, comment:null), FieldSchema(name:cntct_cnt, type:int, comment:null), FieldSchema(name:hold_abandon_cnt, type:int, comment:null), FieldSchema(name:inbnd_cnt, type:int, comment:null), FieldSchema(name:ivr_second_cnt, type:int, comment:null), FieldSchema(name:othr_err_cnt, type:int, comment:null), FieldSchema(name:queue_abandon_cnt, type:int, comment:null), FieldSchema(name:retry_agnt_cnt, type:int, comment:null), FieldSchema(name:sys_terminate_cnt, type:int, comment:null), FieldSchema(name:sys_trnsfr_cnt, type:int, comment:null), FieldSchema(name:transfr_init_cnt, type:int, comment:null), FieldSchema(name:arrv_second_cnt, type:int, comment:null), FieldSchema(name:duratn_second_cnt, type:int, comment:null), FieldSchema(name:hang_up_second_cnt, type:int, comment:null), FieldSchema(name:ivr_queue_second_cnt, type:int, comment:null), FieldSchema(name:ring_second_cnt, type:int, comment:null), FieldSchema(name:totl_wrap_up_second_cnt, type:int, comment:null), FieldSchema(name:terminate_wrap_up_second_cnt, type:int, comment:null), FieldSchema(name:transfr_queue_second_cnt, type:int, comment:null), FieldSchema(name:agnt_disconnect_short_call_cnt, type:int, comment:null), FieldSchema(name:short_call_cnt, type:int, comment:null), FieldSchema(name:zero_talk_cnt, type:int, comment:null), FieldSchema(name:hold_call_cnt, type:int, comment:null), FieldSchema(name:answr_20_second_call_cnt, type:int, comment:null), FieldSchema(name:answr_30_second_call_cnt, type:int, comment:null), FieldSchema(name:answr_60_second_call_cnt, type:int, comment:null), FieldSchema(name:answr_120_second_call_cnt, type:int, comment:null), FieldSchema(name:tier1_to_tier2_cnt, type:int, comment:null), FieldSchema(name:ivr_terminate_cnt, type:int, comment:null), FieldSchema(name:netwrk_handle_call_cnt, type:int, comment:null), FieldSchema(name:netwrk_totl_handle_tm, type:int, comment:null), FieldSchema(name:netwrk_handle_talk_tm, type:int, comment:null), FieldSchema(name:netwrk_handle_hold_tm, type:int, comment:null), FieldSchema(name:netwrk_aftr_call_wrk_tm, type:int, comment:null), FieldSchema(name:trnsfr_handle_call_cnt, type:int, comment:null), FieldSchema(name:trnsfr_totl_handle_tm, type:int, comment:null), FieldSchema(name:trnsfr_handle_talk_tm, type:int, comment:null), FieldSchema(name:trnsfr_handle_hold_tm, type:int, comment:null), FieldSchema(name:trnsfr_aftr_call_wrk_tm, type:int, comment:null), FieldSchema(name:agnt_disconnect_ind, type:string, comment:null), FieldSchema(name:ivr_delay_second_cnt, type:int, comment:null), FieldSchema(name:gmt_trans_date_key, type:int, comment:null), FieldSchema(name:pst_trans_date_key, type:int, comment:null), FieldSchema(name:prim_purch_trvl_acct_key, type:int, comment:null), FieldSchema(name:gross_trans_cnt, type:int, comment:null), FieldSchema(name:gross_bkg_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:gross_purch_price_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:gross_purch_cost_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:gross_cncl_price_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:gross_cncl_cost_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:totl_cost_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:margn_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:gross_purch_ordr_cnt, type:int, comment:null), FieldSchema(name:gross_purch_trans_cnt, type:int, comment:null), FieldSchema(name:gross_cncl_ordr_cnt, type:int, comment:null), FieldSchema(name:gross_cncl_trans_cnt, type:int, comment:null), FieldSchema(name:gross_ordr_cnt, type:int, comment:null), FieldSchema(name:gross_agncy_trans_cnt, type:int, comment:null), FieldSchema(name:gross_merch_trans_cnt, type:int, comment:null), FieldSchema(name:cust_ops_est_bk_rev_amt_usd, type:decimal(19,4), comment:null), FieldSchema(name:itin_detail, type:array<struct<SRC_SYS_ID:int,TPID:int,TRL:int,PRODUCT_CAT_KEY:int,PRODUCT_LN_NAME:string,RESPONDENT_ID:string,BUSINESS_PARTNR_KEY:int,ITIN_NBR:string,ORDER_NBR:bigint,ITIN_BK_AGNT_KEY:int,ITIN_BK_AGNT_HIRE_TENR_DAY:int,ITIN_BK_AGNT_ROLE_TENR_DAY:int,BK_AGNT_KEY:int,BK_DATE_KEY:int,AB_TST_GRP_ID:int,BEGIN_USE_DATE_KEY:int,BEGIN_USE_DATE:string,BK_DATE:string,BK_DATETM:string,BKG_IND_KEY:int,PKG_BKG_IND_KEY:int,BKG_PRODUCT_LN_COMPONENT_KEY:int,BKG_WINDW_KEY:int,COST_CURRN_KEY:int,COUPN_KEY:int,CUST_OPS_BKG_IND_KEY:int,END_USE_DATE_KEY:int,END_USE_DATE:string,LGL_ENTITY_KEY:int,MKTG_CODE_KEY:int,ORACLE_GL_PRODUCT_KEY:int,PRICE_CURRN_KEY:int,PRODUCT_LN_KEY:int,PST_TRANS_DATE_KEY:int,PST_TRANS_DATE:string,PST_TRANS_TM_KEY:int,AIR_TRIP_TYP_NAME:string,SAT_NIGHT_STAY_IND:string,AIR_BKG_IND_KEY:int,AIR_SETTLMNT_AGNT_TYP_KEY:int,PLATNG_CARRIER_KEY:int,PNR_REC_LOCATOR_CODE:string,TCKT_AIR_FARE_TYP_KEY:int,TCKT_ROUTE_KEY:int,TOUR_OPERATR_KEY:int,AGNT_ASST_IND:string,BKG_WINDW_RNG_NAME:string,CAR_CAT_NAME:string,CAR_TYP_NAME:string,CREDT_CARD_TYP_KEY:int,CAR_BASE_PRICE_PERIOD_KEY:int,CAR_BKG_IND_KEY:int,CAR_CLASS_KEY:int,CAR_DROP_OFF_LOC_KEY:int,CAR_PICK_UP_LOC_KEY:int,CAR_SPCL_EQUIP_GRP_KEY:int,CAR_VNDR_AGRMNT_KEY:int,CAR_VNDR_KEY:int,CRUIS_ADJ_REASN_KEY:int,CRUIS_CABN_TYP_KEY:int,CRUIS_RSDNC_STATE_PROVNC_KEY:int,CRUIS_SUB_DEST_KEY:int,DISEMBRK_PORT_KEY:int,EMBRK_PORT_KEY:int,SHIP_KEY:int,AGNT_TOUCH_IND:string,OFFRNG_ITM_KEY:int,DEST_SVC_SRCH_LOC_KEY:int,DEST_REGN_KEY:int,INS_OFFRNG_CAT_KEY:int,INS_OFFRNG_CAT_NAME:string,LENGTH_OF_STAY_RNG_NAME:string,EEM_PROPERTY_IND:string,EXPE_HALF_STAR_RTG:decimal(19,4),GDS_PROPERTY_IND:string,LODG_PROPERTY_NAME:string,MERCH_PROPERTY_IND:string,OPAQUE_PROPERTY_IND:string,PROPERTY_BRAND_NAME:string,PROPERTY_CNTRCT_MODEL_NAME:string,PROPERTY_CNTRY_NAME:string,PROPERTY_PARNT_CHAIN_NAME:string,PROPERTY_MKT_NAME:string,BKG_REFRL_SRC_KEY:int,DISTR_KEY:int,DOM_INTL_BKG_ITM_IND_KEY:int,LENGTH_OF_STAY_KEY:int,LODG_PROPERTY_KEY:int,LODG_RATE_PLN_KEY:int,LODG_RATE_RULE_KEY:int,ORDER_CONF_NBR:string,PRICE_STRUCT_KEY:int,TPID_CURRN_KEY:int,TRANS_TYP_KEY:int,TRVL_DURATN_KEY:int,OFFRNG_ITM_NAME:string,OFFRNG_NAME:string,PKG_BEGIN_USE_DATE:string,PKG_CAR_VNDR_1_KEY:int,PKG_CAR_VNDR_2_KEY:int,PKG_END_USE_DATE:string,PKG_LODG_PROPERTY_1_KEY:int,PKG_LODG_PROPERTY_2_KEY:int,PRICE_MODEL_NAME:string,FLEX_MOR_IND:string,PKG_TYP_NAME:string,TRANS_CAT_NAME:string,TRANS_TYP_DESC:string,TRANS_TYP_ID:int,TRANS_TYP_NAME:string,TRANS_USE_PERIOD_NAME:string,COST_CURRN_NAME:string,DISEMBRK_PORT_NAME:string,EMBRK_PORT_NAME:string,PLATNG_CARRIER_NAME:string,TCKT_DEST_AIRPT_CODE:string,TCKT_DEST_AIRPT_CNTRY_CODE:string,TCKT_DEST_AIRPT_CNTRY_NAME:string,TCKT_ORIGN_AIRPT_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_CODE:string,TCKT_ORIGN_AIRPT_CNTRY_NAME:string,TCKT_ROUTE_NAME:string,BUSINESS_MODEL_NAME:string,BUSINESS_MODEL_SUBTYP_NAME:string,PKG_IND:string,BKG_ID:int,BKG_ITM_ID:int,BKG_SYS_OF_REC_ID:int,BKG_SYS_OF_REC_NAME:string,ITIN_CREATE_DATETM:string,ORDER_ID:bigint,ORDER_LN_SEQ_NBR:int,PROPERTY_LOCAL_BK_DATE_KEY:int,PROPERTY_LOCAL_BK_TM_KEY:int,PST_ITIN_CREATE_DATE:string,PST_ITIN_CREATE_DATE_KEY:int,PST_ITIN_CREATE_TM_KEY:int,TRANS_TM_KEY:int,PKG_ID:bigint,PKG_TRANS_TYP_KEY:int,BK_LANG_KEY:int,BK_LANG_CODE:string,BK_LANG_NAME:string,BK_GROSS_TRANS_CNT:int,BK_GROSS_BKG_AMT_USD:decimal(19,4),BK_GROSS_PURCH_PRICE_AMT_USD:decimal(19,4),BK_GROSS_PURCH_COST_AMT_USD:decimal(19,4),BK_GROSS_CNCL_PRICE_AMT_USD:decimal(19,4),BK_GROSS_CNCL_COST_AMT_USD:decimal(19,4),BK_TOTL_COST_AMT_USD:decimal(19,4),BK_MARGN_AMT_USD:decimal(19,4),BK_GROSS_PURCH_ORDR_CNT:int,BK_GROSS_PURCH_TRANS_CNT:int,BK_GROSS_CNCL_ORDR_CNT:int,BK_GROSS_CNCL_TRANS_CNT:int,BK_GROSS_ORDR_CNT:int,BK_GROSS_AGNCY_TRANS_CNT:int,BK_GROSS_MERCH_TRANS_CNT:int,BK_CUST_OPS_EST_BK_REV_AMT_USD:decimal(19,4),COUPN_PRICE_AMT_USD:decimal(19,4),EST_COST_OF_SALE_AMT_USD:decimal(19,4),EST_GROSS_PROFIT_AMT_USD:decimal(19,4),EST_NET_REV_AMT_USD:decimal(19,4),EST_VAR_COST_OF_SALE_AMT_USD:decimal(19,4),EST_VAR_GROSS_PROFIT_AMT_USD:decimal(19,4),FRNT_END_CMSN_AMT_USD:decimal(19,4),OTHR_COST_ADJ_AMT_USD:decimal(19,4),OTHR_DEST_SVC_TCKT_CNT:int,OTHR_FEE_COST_AMT_USD:decimal(19,4),OTHR_FEE_PRICE_AMT_USD:decimal(19,4),ADULT_CNT:int,AGNT_ASST_PURCH_FEE_AMT_USD:decimal(19,4),AGNT_TOUCH_CNT:int,BASE_COST_AMT_USD:decimal(19,4),BASE_PRICE_AMT_USD:decimal(19,4),AGNT_ASST_EXCH_FEE_AMT_USD:decimal(19,4),AGNT_ASST_REFUND_FEE_AMT_USD:decimal(19,4),AGNT_ASST_VOID_FEE_AMT_USD:decimal(19,4),BKG_FEE_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_COST_AMT_USD:decimal(19,4),CREDT_CARD_SURCHG_PRICE_AMT_USD:decimal(19,4),DELIVERY_FEE_COST_AMT_USD:decimal(19,4),DELIVERY_FEE_PRICE_AMT_USD:decimal(19,4),EXCH_PNLTY_COST_AMT_USD:decimal(19,4),EXCH_PNLTY_PRICE_AMT_USD:decimal(19,4),LAP_INFANT_CNT:int,PAPR_TCKT_FEE_AMT_USD:decimal(19,4),UNUSED_TCKT_COST_AMT_USD:decimal(19,4),UNUSED_TCKT_PRICE_AMT_USD:decimal(19,4),AIR_TRANS_SEG_CNT:int,AIR_TRANS_TCKT_CNT:int,PEAK_RATE_COST_ADJ_AMT_USD:decimal(19,4),PEAK_RATE_PRICE_ADJ_AMT_USD:decimal(19,4),OTHR_PRICE_ADJ_AMT_USD:decimal(19,4),PURE_MARGN_AMT_USD:decimal(19,4),RENTL_DAY_CNT:int,VAR_PRICE_ADJ_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_COST_AMT_USD:decimal(19,4),CRUIS_FUEL_SURCHG_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_AIR_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_AIR_CMSN_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_COST_AMT_USD:decimal(19,4),CRUIS_LN_LODG_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_LN_LODG_CMSN_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_COST_AMT_USD:decimal(19,4),CRUIS_PREPD_GRAT_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_COST_AMT_USD:decimal(19,4),CRUIS_SAIL_BASE_PRICE_AMT_USD:decimal(19,4),CRUIS_SAIL_CMSN_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_COST_AMT_USD:decimal(19,4),CRUIS_TRNSFR_FEE_PRICE_AMT_USD:decimal(19,4),PORT_CHRG_COST_AMT_USD:decimal(19,4),PORT_CHRG_PRICE_AMT_USD:decimal(19,4),SENIOR_CNT:int,OTHR_TAX_COST_AMT_USD:decimal(19,4),OTHR_TAX_PRICE_AMT_USD:decimal(19,4),NET_RETAIL_RATE_AMT_USD:decimal(19,4),MARKUP_AMT_USD:decimal(19,4),ADULT_DEST_SVC_TCKT_CNT:int,CHILD_DEST_SVC_TCKT_CNT:int,DEST_SVC_BKG_ITM_CNT:int,TOTL_DEST_SVC_TCKT_CNT:int,ADULT_INS_ITM_CNT:int,CHILD_INS_ITM_CNT:int,OTHR_INS_ITM_CNT:int,TOTL_INS_ITM_CNT:int,CNCL_PNLTY_WAIVR_PRICE_ADJ_AMT_USD:decimal(19,4),DYN_RATE_RULE_COST_AMT_USD:decimal(19,4),DYN_RATE_RULE_PRICE_AMT_USD:decimal(19,4),EMP_DISC_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),EXPE_PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),EXTRA_PERSN_COST_AMT_USD:decimal(19,4),EXTRA_PERSN_PRICE_AMT_USD:decimal(19,4),GDWLL_PRICE_ADJ_AMT_USD:decimal(19,4),GENRIC_COUPN_PRICE_AMT_USD:decimal(19,4),INFANT_CNT:int,LODG_PKG_SAVE_AMT_USD:decimal(19,4),LOYLTY_POINT_PRICE_ADJ_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_COST_AMT_USD:decimal(19,4),MARGN_OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),MARGN_SALES_TAX_COST_AMT_USD:decimal(19,4),MARGN_SALES_TAX_PRICE_AMT_USD:decimal(19,4),NET_SVC_FEE_PRICE_AMT_USD:decimal(19,4),OCCUP_TAX_COST_AMT_USD:decimal(19,4),OCCUP_TAX_PRICE_AMT_USD:decimal(19,4),PNLTY_PRICE_ADJ_AMT_USD:decimal(19,4),RATE_PLN_RESTR_COST_AMT_USD:decimal(19,4),RATE_PLN_RESTR_PRICE_AMT_USD:decimal(19,4),REBATE_PRICE_AMT_USD:decimal(19,4),REFUND_PRICE_ADJ_AMT_USD:decimal(19,4),RM_NIGHT_CNT:int,SALES_TAX_COST_AMT_USD:decimal(19,4),SALES_TAX_PRICE_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_COST_AMT_USD:decimal(19,4),SNGL_SUPPLMNT_PRICE_AMT_USD:decimal(19,4),STNDAL_HTL_PRICE_MOD_AMT_USD:decimal(19,4),SUPPL_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_COST_ADJ_AMT_USD:decimal(19,4),SUPPL_RECON_PRICE_ADJ_AMT_USD:decimal(19,4),SVC_CHRG_COST_AMT_USD:decimal(19,4),SVC_CHRG_PRICE_AMT_USD:decimal(19,4),SVC_FEE_PRICE_AMT_USD:decimal(19,4),TCM_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_CMSN_AMT_USD:decimal(19,4),TOTL_COST_ADJ_AMT_USD:decimal(19,4),TOTL_FEE_COST_AMT_USD:decimal(19,4),TOTL_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_COST_AMT_USD:decimal(19,4),TOTL_GENRL_BKG_PRICE_AMT_USD:decimal(19,4),TOTL_PERSN_CNT:int,TOTL_PRICE_ADJ_AMT_USD:decimal(19,4),TOTL_TAX_COST_AMT_USD:decimal(19,4),TOTL_TAX_PRICE_AMT_USD:decimal(19,4),VAR_MARGN_COST_ADJ_USD:decimal(19,4),CNCL_CHG_FEE_PRICE_AMT_USD:decimal(19,4),CHILD_CNT:int,TRANS_DATETM:string,AIR_PKG_SAVE_AMT_USD:decimal(19,4),PKG_AGNCY_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_CAR_CNT:int,PKG_AGNCY_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_RM_CNT:int,PKG_AGNCY_RM_NIGHT_UNIT_CNT:int,PKG_AGNCY_TCKT_UNIT_CNT:int,PKG_AGNCY_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AGNCY_TRAIN_TCKT_UNIT_CNT:int,PKG_AIR_DURATN_DAY_CNT:int,PKG_AIR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_AIR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_CNT:int,PKG_CAR_FEE_PRICE_AMT_USD:decimal(19,4),PKG_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_CAR_MARGN_AMT_USD:decimal(19,4),PKG_CAR_RENTL_DAY_UNIT_CNT:int,PKG_COST_ADJ_AMT_USD:decimal(19,4),PKG_CRUIS_CABN_UNIT_CNT:int,PKG_CRUIS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_FEE_PRICE_AMT_USD:decimal(19,4),PKG_DEST_SVC_GROSS_BKG_AMT_USD:decimal(19,4),PKG_DEST_SVC_MARGN_AMT_USD:decimal(19,4),PKG_DEST_SVC_TCKT_UNIT_CNT:int,PKG_INS_FEE_PRICE_AMT_USD:decimal(19,4),PKG_INS_GROSS_BKG_AMT_USD:decimal(19,4),PKG_INS_ITM_UNIT_CNT:int,PKG_INS_MARGN_AMT_USD:decimal(19,4),PKG_LODG_FEE_PRICE_AMT_USD:decimal(19,4),PKG_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_LODG_MARGN_AMT_USD:decimal(19,4),PKG_MERCH_AIR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_CAR_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_LODG_GROSS_BKG_AMT_USD:decimal(19,4),PKG_MERCH_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_PRICE_ADJ_AMT_USD:decimal(19,4),PKG_SAVE_AMT_USD:decimal(19,4),PKG_SAVE_PRICE_AMT_USD:decimal(19,4),PKG_TAX_COST_AMT_USD:decimal(19,4),PKG_TAX_PRICE_AMT_USD:decimal(19,4),PKG_TRAIN_GROSS_BKG_AMT_USD:decimal(19,4),PKG_TRAIN_MARGN_AMT_USD:decimal(19,4),TOTL_PKG_FEE_COST_AMT_USD:decimal(19,4),TOTL_PKG_FEE_PRICE_AMT_USD:decimal(19,4),TOTL_PKG_UNIT_CNT:int,DURATN_DAY_CNT:int,PKG_MERCH_CAR_CNT:int,PKG_MERCH_RM_CNT:int,PKG_MERCH_RM_NIGHT_UNIT_CNT:int,PKG_MERCH_TCKT_UNIT_CNT:int,PKG_MERCH_TRAIN_TCKT_UNIT_CNT:int,PKG_RM_CNT:int,PKG_RM_NIGHT_UNIT_CNT:int,PKG_TCKT_SEG_CNT:int,PKG_TCKT_UNIT_CNT:int,PKG_TOTL_TRVLR_CNT:int,PKG_TRAIN_DURATN_DAY_CNT:int,PKG_TRAIN_TCKT_UNIT_CNT:int,TRANS_AGNT_TOOL_NAME:string,TRANS_SRC_TYP_NAME:string,ONLINE_OFFLN_IND:string,MGMT_UNIT_KEY:int,MGMT_UNIT_CODE:string,MGMT_UNIT_NAME:string,MGMT_UNIT_LVL_1_NAME:string,MGMT_UNIT_LVL_2_NAME:string,MGMT_UNIT_LVL_3_NAME:string,MGMT_UNIT_LVL_4_NAME:string,MGMT_UNIT_LVL_5_NAME:string,MGMT_UNIT_LVL_6_NAME:string,MGMT_UNIT_LVL_7_NAME:string,MGMT_UNIT_LVL_8_NAME:string,MGMT_UNIT_LVL_9_NAME:string,MGMT_UNIT_LVL_10_NAME:string,MGMT_UNIT_LVL_11_NAME:string,MGMT_UNIT_LVL_12_NAME:string,MGMT_UNIT_LVL_13_NAME:string,MGMT_UNIT_LVL_14_NAME:string,PURCH_TRVL_ACCT_KEY:int,BKG_SERVICE_TYPE_NAME:string,BKG_SERVICE_CATEGORY_NAME:string,ETE_POS:string,ETE_FLAG:string,AGNT_ASST_CHG_FEE_AMT_USD:decimal(19,4),AGNT_ASST_CNCL_FEE_AMT_USD:decimal(19,4),SERV_FEE_TRANS_CNT:int,CNCL_FEE_TRANS_CNT:int,ABS_CHG_TRANS_CNT:int,ABS_CNCL_TRANS_CNT:int,BKG_AGENT_TIER_NAME:string>>, comment:null), FieldSchema(name:ngcc_trans_typ_skill_id, type:int, comment:null), FieldSchema(name:ngcc_trans_typ_skill_name, type:string, comment:null), FieldSchema(name:ngcc_product_skill_id, type:int, comment:null), FieldSchema(name:ngcc_product_skill_name, type:string, comment:null), FieldSchema(name:ngcc_lang_skill_id, type:int, comment:null), FieldSchema(name:ngcc_lang_skill_name, type:string, comment:null), FieldSchema(name:cust_ops_ngcc_agnt_id, type:string, comment:null), FieldSchema(name:ngcc_query_typ_skill_id, type:int, comment:null), FieldSchema(name:ngcc_query_typ_skill_cat_name, type:string, comment:null), FieldSchema(name:ngcc_query_typ_skill_cat_desc, type:string, comment:null), FieldSchema(name:ngcc_seg_skill_id, type:int, comment:null), FieldSchema(name:ngcc_seg_skill_cat_name, type:string, comment:null), FieldSchema(name:ngcc_seg_typ_skill_cat_desc, type:string, comment:null), FieldSchema(name:ngcc_extrnl_agnt_supprt_skill_id, type:int, comment:null), FieldSchema(name:ngcc_extrnl_agnt_supprt_skill_name, type:string, comment:null), FieldSchema(name:ngcc_extrnl_agnt_supprt_skill_desc, type:string, comment:null), FieldSchema(name:nav_int_case_id, type:string, comment:null), FieldSchema(name:nav_int_typ_name, type:string, comment:null), FieldSchema(name:nav_int_typ_reasn_name, type:string, comment:null), FieldSchema(name:nav_int_stat_name, type:string, comment:null), FieldSchema(name:nav_int_local_currn, type:string, comment:null), FieldSchema(name:nav_int_assign_queue_name, type:string, comment:null), FieldSchema(name:nav_int_cncl_case_ind, type:string, comment:null), FieldSchema(name:nav_int_guest_acct_case_ind, type:string, comment:null), FieldSchema(name:nav_int_anchor_ind, type:string, comment:null), FieldSchema(name:nav_int_case_sla_typ_name, type:string, comment:null), FieldSchema(name:nav_int_sla_datetm, type:string, comment:null), FieldSchema(name:nav_int_sla_goal_datetm, type:string, comment:null), FieldSchema(name:nav_int_create_org_name, type:string, comment:null), FieldSchema(name:nav_int_create_div_name, type:string, comment:null), FieldSchema(name:nav_int_create_org_unit_name, type:string, comment:null), FieldSchema(name:nav_int_create_wrk_grp_name, type:string, comment:null), FieldSchema(name:nav_int_resolve_org_name, type:string, comment:null), FieldSchema(name:nav_int_resolve_div_name, type:string, comment:null), FieldSchema(name:nav_int_resolve_org_unit_name, type:string, comment:null), FieldSchema(name:nav_int_resolve_wrk_grp_name, type:string, comment:null), FieldSchema(name:nav_int_cust_case_id, type:string, comment:null), FieldSchema(name:nav_int_cust_score_name, type:string, comment:null), FieldSchema(name:nav_int_cust_identity_verify_ind, type:string, comment:null), FieldSchema(name:nav_int_itin_nbr, type:string, comment:null), FieldSchema(name:nav_int_tpid, type:int, comment:null), FieldSchema(name:nav_int_trl, type:bigint, comment:null), FieldSchema(name:nav_int_answr_second_cnt, type:bigint, comment:null), FieldSchema(name:nav_int_resolve_second_cnt, type:decimal(19,4), comment:null), FieldSchema(name:nav_int_task_wrap_up_duratn_second_cnt, type:int, comment:null), FieldSchema(name:nav_int_nav_int_tm_second_cnt, type:decimal(19,4), comment:null), FieldSchema(name:nav_int_no_of_item_create_cnt, type:int, comment:null), FieldSchema(name:nav_int_case_cnt, type:int, comment:null), FieldSchema(name:nav_resolve_abandon_case_cnt, type:int, comment:null), FieldSchema(name:nav_resolve_cancel_case_cnt, type:int, comment:null), FieldSchema(name:nav_resolve_complete_case_cnt, type:int, comment:null), FieldSchema(name:nav_int_create_agnt_login_id, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_key, type:int, comment:null), FieldSchema(name:nav_int_create_agnt_earns_cmsn_ind, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_email_addr, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_emp_typ_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_frst_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_middl_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_last_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_knwn_as_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_hire_date, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_termnatn_date, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_job_title_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_mgmt_div_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_mgr_frst_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_mgr_last_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_prim_business_grp_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_prim_typ_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_profcncy_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_role_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_prim_lang_code, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_prim_lang_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_scndry_lang_code, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_scndry_lang_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_tertiary_lang_code, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_tertiary_lang_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_vndr_loc_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_vndr_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_hire_tenr_day, type:int, comment:null), FieldSchema(name:nav_int_create_agnt_role_tenr_day, type:int, comment:null), FieldSchema(name:nav_int_create_agnt_service_category_name, type:string, comment:null), FieldSchema(name:nav_int_update_agnt_login_id, type:string, comment:null), FieldSchema(name:nav_int_update_agnt_key, type:int, comment:null), FieldSchema(name:nav_int_resolve_agnt_login_id, type:string, comment:null), FieldSchema(name:nav_int_resolve_agnt_key, type:int, comment:null), FieldSchema(name:nav_tm_zone_key, type:int, comment:null), FieldSchema(name:nav_tm_zone_name, type:string, comment:null), FieldSchema(name:nav_int_source_create_datetm, type:string, comment:null), FieldSchema(name:nav_int_pst_create_date_key, type:int, comment:null), FieldSchema(name:nav_int_gmt_create_date_key, type:int, comment:null), FieldSchema(name:nav_int_aest_create_date_key, type:int, comment:null), FieldSchema(name:nav_int_pst_create_tm_key, type:int, comment:null), FieldSchema(name:nav_int_gmt_create_tm_key, type:int, comment:null), FieldSchema(name:nav_int_aest_create_tm_key, type:int, comment:null), FieldSchema(name:nav_int_source_update_datetm, type:string, comment:null), FieldSchema(name:nav_int_pst_update_date_key, type:int, comment:null), FieldSchema(name:nav_int_gmt_update_date_key, type:int, comment:null), FieldSchema(name:nav_int_aest_update_date_key, type:int, comment:null), FieldSchema(name:nav_int_pst_update_tm_key, type:int, comment:null), FieldSchema(name:nav_int_gmt_update_tm_key, type:int, comment:null), FieldSchema(name:nav_int_aest_update_tm_key, type:int, comment:null), FieldSchema(name:nav_int_source_resolve_datetm, type:string, comment:null), FieldSchema(name:nav_int_pst_resolve_date_key, type:int, comment:null), FieldSchema(name:nav_int_gmt_resolve_date_key, type:int, comment:null), FieldSchema(name:nav_int_aest_resolve_date_key, type:int, comment:null), FieldSchema(name:nav_int_pst_resolve_tm_key, type:int, comment:null), FieldSchema(name:nav_int_gmt_resolve_tm_key, type:int, comment:null), FieldSchema(name:nav_int_aest_resolve_tm_key, type:int, comment:null), FieldSchema(name:nav_int_em_lang_name, type:string, comment:null), FieldSchema(name:nav_int_em_lang_code, type:string, comment:null), FieldSchema(name:nav_int_em_lang_key, type:int, comment:null), FieldSchema(name:nav_int_svc_list, type:array<struct<SVC_CASE_ID:string,SVC_CASE_STAT_NAME:string,SVC_ASSIGN_QUEUE_NAME:string,SVC_INT_TYP_NAME:string,SVC_TRVL_STG_IND:string,SVC_LOCAL_CURRN:string,SVC_ERR_LOGIN_NAME:string,SVC_GUEST_ACCT_CASE_IND:string,SVC_CALLER_DISCONNECT_IND:string,SVC_CHG_EXECUTED_TYP_IND:string,SVC_COUPN_ISSUE_IND:string,SVC_COUPN_EXPR_DATE:string,SVC_VNDR_CONSULT_IND:string,SVC_REFUND_IND:string,SVC_WRITE_OFF_IND:string,SVC_ACTN_NAME:string,SVC_ACTN_DESC:string,SVC_MSSNG_RES_ACTN_CODE:string,SVC_MSSNG_RES_ACTN_NAME:string,SVC_GUEST_ACCT_IND:string,SVC_INS_IND:string,SVC_ANCHOR_IND:string,SVC_CUST_CALLBK_IND:string,SVC_CUST_CALLBK_DESC:string,SVC_COMPLAINT_IND:string,SVC_NEW_CASE_IND:string,SVC_CASE_CUST_EMAIL_ADDR:string,SVC_SLA_TYP_NAME:string,SVC_SLA_DATETM:string,SVC_SLA_GOAL_DATETM:string,SVC_ESCALATE_IND:string,SVC_ESCALATE_PARENT_CASE_ID:string,SVC_ESCALATE_ACTN_NAME:string,SVC_ESCALATE_COMMUNICATION_ISSUE_NAME:string,SVC_ESCALATE_FAULT_NAME:string,SVC_ESCALATE_ISSUE_TYP_NAME:string,SVC_ESCALATE_RESOLTN_NAME:string,SVC_ESCALATE_ROOT_CAUSE_NAME:string,SVC_ITIN_NBR:string,SVC_TPID:int,SVC_TPID_KEY:int,SVC_TUID:bigint,SVC_TRL:bigint,SVC_PRODUCT_CAT_NAME:string,SVC_PRODUCT_CAT_KEY:smallint,SVC_PRODUCT_CAT_ID:smallint,SVC_HOTEL_ID:bigint,SVC_LODG_PROPERTY_KEY:int,SVC_LODG_PROPERTY_NAME:string,SVC_PROPERTY_BRAND_NAME:string,SVC_PROPERTY_CITY_NAME:string,SVC_PROPERTY_CNTRY_CODE:string,SVC_PROPERTY_REGN_NAME:string,SVC_PROPERTY_PARNT_CHAIN_NAME:string,SVC_PROPERTY_TYP_NAME:string,SVC_BUSINESS_PARTNR_KEY:int,SVC_BUSINESS_PARTNR_NAME:string,SVC_ACTV_BUSINESS_PARTNR_IND:string,SVC_BUSINESS_PARTNR_MED_NAME:string,SVC_BUSINESS_PARTNR_MGMT_UNIT_NAME:string,SVC_BUSINESS_PARTNR_RPT_CO_NAME:string,SVC_BUSINESS_PARTNR_SEG_NAME:string,SVC_BUSINESS_PARTNR_SVC_BRAND_NAME:string,SVC_BUSINESS_PARTNR_SVC_CNTRY_NAME:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_DESC:string,SVC_BUSINESS_PARTNR_SVC_SUPER_REGN_NAME:string,SVC_BUSINESS_PARTNR_SYS_NAME:string,SVC_BUSINESS_PARTNR_TPID_NAME:string,SVC_MGMT_UNIT_KEY:int,SVC_MGMT_UNIT_NAME:string,SVC_MGMT_UNIT_LVL_1_NAME:string,SVC_MGMT_UNIT_LVL_2_NAME:string,SVC_MGMT_UNIT_LVL_3_NAME:string,SVC_MGMT_UNIT_LVL_4_NAME:string,SVC_MGMT_UNIT_LVL_5_NAME:string,SVC_MGMT_UNIT_LVL_6_NAME:string,SVC_MGMT_UNIT_LVL_7_NAME:string,SVC_MGMT_UNIT_LVL_8_NAME:string,SVC_MGMT_UNIT_LVL_9_NAME:string,SVC_MGMT_UNIT_LVL_10_NAME:string,SVC_MGMT_UNIT_LVL_11_NAME:string,SVC_MGMT_UNIT_LVL_12_NAME:string,SVC_MGMT_UNIT_LVL_13_NAME:string,SVC_MGMT_UNIT_LVL_14_NAME:string,SVC_ONLINE_OFFLN_IND_KEY:int,SVC_ONLINE_OFFLN_IND:string,SVC_PURCH_TRVL_ACCT_ID:string,SVC_PURCH_TRVL_ACCT_KEY:int,SVC_PURCH_TRVL_ACCT_FRST_NAME:string,SVC_PURCH_TRVL_ACCT_LAST_NAME:string,SVC_PURCH_TRVL_ACCT_EMAIL_ADDR:string,SVC_PURCH_TRVL_ACCT_ADDR_1:string,SVC_PURCH_TRVL_ACCT_ADDR_2:string,SVC_PURCH_TRVL_ACCT_CITY_NAME:string,SVC_PURCH_TRVL_ACCT_POSTAL_CODE:string,SVC_PURCH_TRVL_ACCT_STATE_PROVNC_NAME:string,SVC_PURCH_TRVL_ACCT_CNTRY_CODE:string,SVC_PURCH_TRVL_ACCT_CNTRY_NAME:string,SVC_PURCH_TRVL_ACCT_SUPER_REGN_NAME:string,SVC_PURCH_TRVL_ACCT_CREATE_DATE:string,SVC_BKG_TYP_CODE:string,SVC_BKG_TYP_NAME:string,SVC_BKG_TYP_BUSINESS_MODEL_NAME:string,SVC_AIRLN_CARRIER:string,SVC_AIRFARE_TYP_CODE:string,SVC_AIRFARE_TYP_NAME:string,SVC_FLGHT_SCHED_CHG_OPTN_IND:string,SVC_LOW_COST_CARRIER_IND:string,SVC_SECURE_FLGHT_IND:string,SVC_BRKAGE_CANDIDATE_IND:string,SVC_CUST_CASE_ID:string,SVC_CUST_SCORE_NAME:string,SVC_CUST_IDENTITY_VERIFY_IND:string,SVC_CREDT_EXPR_TYP_CODE:string,SVC_CREDT_EXPR_TYP_DESC:string,SVC_EARLY_CHK_OUT_IND:string,SVC_EXPE_REWARDS_IND:string,SVC_INTNT_NAME:string,SVC_INTNT_DESC:string,SVC_CAT_LVL_1_INTENT_NAME:string,SVC_CNCL_LVL_2_INTENT_CODE:string,SVC_CNCL_LVL_2_INTENT_NAME:string,SVC_CNCL_CHG_LVL_2_INTENT_CODE:string,SVC_CNCL_CHG_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_2_INTENT_NAME:string,SVC_COMPLAINT_LVL_3_INTENT_NAME:string,SVC_COUPN_LVL_2_INTENT_CODE:string,SVC_COUPN_LVL_2_INTENT_NAME:string,SVC_COUPN_LVL_3_INTENT_CODE:string,SVC_COUPN_LVL_3_INTENT_NAME:string,SVC_CRUIS_AFFL_NAME:string,SVC_RECONFIRM_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_AIR_LVL_2_INTENT_NAME:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_CODE:string,SVC_RECONFIRM_PKG_LODG_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_2_INTENT_CODE:string,SVC_REFUND_LVL_2_INTENT_NAME:string,SVC_REFUND_LVL_3_INTENT_CODE:string,SVC_REFUND_LVL_3_INTENT_NAME:string,SVC_REFUND_METHD_LVL_4_INTENT_NAME:string,SVC_REQST_LVL_2_INTENT_CODE:string,SVC_REQST_LVL_2_INTENT_NAME:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_CODE:string,SVC_SPCL_SVC_REQST_LVL_2_INTENT_NAME:string,SVC_CONSULT_LIST:array<struct<SVC_CONSULT_CASE_ESCALATE_ID:string,SVC_CONSULT_AGNT_ROLE_NAME:string,SVC_CONSULT_NOTE_TXT:string,SVC_CONSULT_REASN_NAME:string,SVC_CONSULT_VNDR_TYP_ID:int,SVC_CONSULT_VNDR_TYP_NAME:string,SVC_ESCALATE_TYP_ID:int,SVC_ESCALATE_TYP_NAME:string,SVC_ESCALATE_PARTY_TYP_NAME:string>>,BKG_CONF_ID:bigint,REFUND_APPRVR_FRST_NAME:string,REFUND_APPRVR_LAST_NAME:string,REFUND_APPRVR_TITLE_NAME:string,SVC_LOYLTY_POLICY_FIN_IMPACT_IND:string,SVC_LOYLTY_POLICY_NAME:string,SVC_LOYLTY_ACTIVITY_REVIEW_IND:string,SVC_LOYLTY_MEMBR_ID:bigint,SVC_CREATE_ORG_NAME:string,SVC_CREATE_DIV_NAME:string,SVC_CREATE_ORG_UNIT_NAME:string,SVC_CREATE_WRK_GRP_NAME:string,SVC_RESOLVE_ORG_NAME:string,SVC_RESOLVE_DIV_NAME:string,SVC_RESOLVE_ORG_UNIT_NAME:string,SVC_RESOLVE_WRK_GRP_NAME:string,SVC_S_CASE_CNT:int,SVC_E_CASE_CNT:int,SVC_O_CASE_CNT:int,SVC_ESCALATION_CASE_CNT:int,SVC_RESOLVE_SECOND_CNT:decimal(19,4),SVC_CREATE_AGNT_LOGIN_ID:string,SVC_CREATE_AGNT_KEY:int,SVC_CREATE_AGNT_EARNS_CMSN_IND:string,SVC_CREATE_AGNT_EMAIL_ADDR:string,SVC_CREATE_AGNT_EMP_TYP_NAME:string,SVC_CREATE_AGNT_FRST_NAME:string,SVC_CREATE_AGNT_MIDDL_NAME:string,SVC_CREATE_AGNT_LAST_NAME:string,SVC_CREATE_AGNT_KNWN_AS_NAME:string,SVC_CREATE_AGNT_HIRE_DATE:string,SVC_CREATE_AGNT_TERMNATN_DATE:string,SVC_CREATE_AGNT_JOB_TITLE_NAME:string,SVC_CREATE_AGNT_MGMT_DIV_NAME:string,SVC_CREATE_AGNT_MGR_FRST_NAME:string,SVC_CREATE_AGNT_MGR_LAST_NAME:string,SVC_CREATE_AGNT_PRIM_BUSINESS_GRP_NAME:string,SVC_CREATE_AGNT_PRIM_TYP_NAME:string,SVC_CREATE_AGNT_PROFCNCY_NAME:string,SVC_CREATE_AGNT_ROLE_NAME:string,SVC_CREATE_AGNT_PRIM_LANG_CODE:string,SVC_CREATE_AGNT_PRIM_LANG_NAME:string,SVC_CREATE_AGNT_SCNDRY_LANG_CODE:string,SVC_CREATE_AGNT_SCNDRY_LANG_NAME:string,SVC_CREATE_AGNT_TERTIARY_LANG_CODE:string,SVC_CREATE_AGNT_TERTIARY_LANG_NAME:string,SVC_CREATE_AGNT_VNDR_LOC_NAME:string,SVC_CREATE_AGNT_VNDR_NAME:string,SVC_CREATE_AGNT_HIRE_TENR_DAY:int,SVC_CREATE_AGNT_ROLE_TENR_DAY:int,SVC_CREATE_AGNT_SERVICE_CATEGORY_NAME:string,SVC_UPDATE_AGNT_LOGIN_ID:string,SVC_UPDATE_AGNT_KEY:int,SVC_RESOLVE_AGNT_LOGIN_ID:string,SVC_RESOLVE_AGNT_KEY:int,SVC_SOURCE_CREATE_DATETM:string,SVC_PST_CREATE_DATE_KEY:int,SVC_GMT_CREATE_DATE_KEY:int,SVC_AEST_CREATE_DATE_KEY:int,SVC_PST_CREATE_TM_KEY:int,SVC_GMT_CREATE_TM_KEY:int,SVC_AEST_CREATE_TM_KEY:int,SVC_SOURCE_UPDATE_DATETM:string,SVC_PST_UPDATE_DATE_KEY:int,SVC_GMT_UPDATE_DATE_KEY:int,SVC_AEST_UPDATE_DATE_KEY:int,SVC_PST_UPDATE_TM_KEY:int,SVC_GMT_UPDATE_TM_KEY:int,SVC_AEST_UPDATE_TM_KEY:int,SVC_SOURCE_RESOLVE_DATETM:string,SVC_PST_RESOLVE_DATE_KEY:int,SVC_GMT_RESOLVE_DATE_KEY:int,SVC_AEST_RESOLVE_DATE_KEY:int,SVC_PST_RESOLVE_TM_KEY:int,SVC_GMT_RESOLVE_TM_KEY:int,SVC_AEST_RESOLVE_TM_KEY:int,SVC_CASE_USER_EMAIL_ADDR:string,SVC_LANG_CODE:string,SVC_LANG_KEY:int,RESPONDENT_ID:string,SVC_GEN_LVL_2_INTENT_CODE:string,SVC_GEN_LVL_2_INTENT_NAME:string>>, comment:null), FieldSchema(name:call_eval_detail, type:array<struct<CALL_EVAL_ID:int,CALL_EVAL_SITE_ID:int,CALL_EVAL_QUESTN_ID:int,CALL_EVAL_ANSWR_ID:int,CALL_EVAL_CREATE_DATETM:string,SRC_DB_VERSION_NBR:int,CALL_EVAL_UPDATE_DATETM:string,PST_CALL_EVAL_CREATE_DATE_KEY:int,PST_CALL_EVAL_CREATE_DATE:string,GMT_CALL_EVAL_CREATE_DATE_KEY:int,GMT_CALL_EVAL_CREATE_DATE:string,PST_CALL_EVAL_UPDATE_DATE_KEY:int,PST_CALL_EVAL_UPDATE_DATE:string,GMT_CALL_EVAL_UPDATE_DATE_KEY:int,GMT_CALL_EVAL_UPDATE_DATE:string,CALL_EVAL_AGNT_NICE_USER_ID:int,CALL_EVAL_AGNT_PERIPH_NBR:array<string>,CALL_EVAL_SEG_ID:string,CALL_EVAL_CASE_NBR:string,CALL_EVAL_ITIN_NBR:string,CALL_EVAL_SEG_DURATN_TM:string,CUST_OPS_CALL_EVAL_QUESTN_KEY:int,CALL_EVAL_QUESTN_SECTN_NAME:string,CALL_EVAL_QUESTN_SUB_SECTN_NAME:string,CALL_EVAL_QUESTN_CALL_OUTCOME:string,CALL_EVAL_QUESTN_SUB_SECTN_NBR:string,CALL_EVAL_QUESTN_NBR:string,CALL_EVAL_QUESTN_LBL_NAME:string,CALL_EVAL_QUESTN_CAPTN_NAME:string,CALL_EVAL_QUESTN_HIER_NBR:string,CALL_EVAL_QUESTN_TYP_ID:int,CALL_EVAL_QUESTN_SCORABLE_NBR:int,CALL_EVAL_QUESTN_IS_FATAL_NBR:int,CUST_OPS_CALL_EVAL_ANSWR_KEY:int,CALL_EVAL_ANSWR_SHORT_REASN_NAME:string,CALL_EVAL_ANSWR_DETAIL_REASN_NAME:string,CUST_OPS_CALL_EVAL_FORM_KEY:int,CALL_EVAL_FORM_ID:int,CALL_EVAL_FORM_TYP_NAME:string,CALL_EVAL_FORM_SRC_NAME:string,CALL_EVAL_FORM_NAME:string,CALL_EVAL_FORM_CREATE_DATETM:string,CALL_EVAL_FORM_UPDATE_DATETM:string,CALL_EVAL_FORM_ENABL_IND:string,CUST_OPS_CALL_EVALUATOR_KEY:int,CALL_EVAL_EVALUATOR_USER_ID:int,CALL_EVALUATOR_FULL_NAME:string,CALL_EVALUATOR_LOCATION_NAME:string,CUST_OPS_CALL_EVAL_TYP_KEY:int,CALL_EVAL_TYP_ID:int,CALL_EVAL_TYP_NAME:string,CUST_OPS_CALL_EVAL_AUTOFAIL_ANSWR_IND_KEY:int,CALL_EVAL_AUTOFAIL_ANSWR_IND:string,CALL_EVAL_AUTOFAIL_ANSWR_CNT:int,CALL_EVAL_ANSWR_CNT:int,POSSIBLE_POINT_NBR:decimal(19,4),EARN_POINT_NBR:decimal(19,4),ADJUSTED_POSSIBLE_POINT_CNT:decimal(19,4),ADJUSTED_EARN_POINT_CNT:decimal(19,4)>>, comment:null), FieldSchema(name:cvp_call_guid, type:string, comment:null), FieldSchema(name:cvp_ab_test_ind, type:string, comment:null), FieldSchema(name:cvp_call_start_date, type:string, comment:null), FieldSchema(name:cvp_call_pst_start_date_key, type:int, comment:null), FieldSchema(name:cvp_call_gmt_start_date_key, type:int, comment:null), FieldSchema(name:cvp_call_start_datetm, type:string, comment:null), FieldSchema(name:cvp_call_end_datetm, type:string, comment:null), FieldSchema(name:cvp_call_time_zone_key, type:int, comment:null), FieldSchema(name:cvp_call_time_zone, type:string, comment:null), FieldSchema(name:cvp_call_ani, type:string, comment:null), FieldSchema(name:cvp_cust_ops_phon_assign_key, type:int, comment:null), FieldSchema(name:cvp_call_dnis, type:string, comment:null), FieldSchema(name:cvp_call_cnt, type:int, comment:null), FieldSchema(name:cvp_auto_ani_cnt, type:int, comment:null), FieldSchema(name:cvp_manual_ani_cnt, type:int, comment:null), FieldSchema(name:cvp_auto_itin_cnt, type:int, comment:null), FieldSchema(name:cvp_manual_itin_cnt, type:int, comment:null), FieldSchema(name:cvp_override_itin_cnt, type:int, comment:null), FieldSchema(name:cvp_hang_up_time_brct_0_10_cnt, type:int, comment:null), FieldSchema(name:cvp_hang_up_time_brct_11_20_cnt, type:int, comment:null), FieldSchema(name:cvp_hang_up_time_brct_21_30_cnt, type:int, comment:null), FieldSchema(name:cvp_hang_up_time_brct_31_up_cnt, type:int, comment:null), FieldSchema(name:cvp_elemnt_list, type:array<struct<CVP_SESSN_ID:bigint,CVP_SESSN_APP_KEY:int,CVP_SESSN_APP_NAME:string,CVP_SESSN_START_DATETM:string,CVP_SESSN_END_DATETM:string,CVP_SESSN_PRODUCT:string,CVP_SESSN_PRODUCT_CAT_KEY:int,CVP_SESSN_TRANS_TYP_CODE:string,CVP_SESSN_TRANS_TYP_KEY:int,CVP_SESSN_TRANS_TYP_NAME:string,CVP_SESSN_CALLER_NEED_CODE:string,CVP_SESSN_CALLER_NEED_KEY:int,CVP_SESSN_CALLER_NEED_NAME:string,CVP_SESSN_LANG_KEY:int,CVP_SESSN_LANG_CODE:string,CVP_SESSN_PRIME_LANG_CNT:bigint,CVP_SESSN_NON_PRIME_LANG_CNT:bigint,CVP_SESSN_BUSINESS_PARTNR_KEY:bigint,CVP_SESSN_BUSINESS_PARTNR_NAME:string,CVP_SESSN_DNIS_NAME:string,CVP_SESSN_ITIN:string,CVP_ELEMNT_ID:bigint,CVP_ELEMNT_KEY:int,CVP_ELEMNT_NAME:string,CVP_ELEMNT_START_DATETM:string,CVP_ELEMNT_END_DATETM:string,CVP_ELEMNT_NUM_INTERACT_CNT:int,CVP_ELEMNT_RESLT:int,CVP_ELEMNT_EXIT_STATE:string,CVP_LODG_CNCL_ELIG_CNT:int,CVP_LODG_CNCL_SUCCSS_CNT:int,CVP_LODG_CNCL_FAIL_CNT:int,CVP_CNCL_SECURE_VALID_METHOD_NAME:string,CVP_CNCL_SECURE_VALID_ZIP_CNT:int,CVP_CNCL_SECURE_VALID_LAST_FOUR_CNT:int,CVP_CNCL_SECURE_VALID_FAIL_CNT:int,CVP_CNCL_TERM_ACCPT_CNT:int,CVP_CNCL_TERM_DECLN_CNT:int,CVP_ANYTHING_ELSE_HUNG_UP_CNT:int,CVP_ANYTHING_ELSE_NEW_CNT:int,CVP_ANYTHING_ELSE_MAIN_MENU_CNT:int,CVP_PHON_NBR_PROMPT_CNT:int,CVP_PHON_FROM_ITIN_LKUP_CNT:int,CVP_ITIN_PROMPT_CNT:int,CVP_ITIN_LKUP_SUCCESS_CNT:int,CVP_ITIN_LKUP_FAIL_CNT:int,CVP_ITIN_SELCT_DIFF_CNT:int,CVP_BKG_MENU_NO_INPUT_CNT:int,CVP_BKG_MENU_HUNG_UP_CNT:int,CVP_ELEMNT_OPT_OUT_CNT:int,CVP_ELEMNT_HANG_UP_CNT:int,CVP_ELEMNT_DURATION:bigint,CVP_ELEMNT_REPT_CNT:int,CVP_ELEMNT_MAX_NO_INPUT_CNT:int,CVP_ELEMNT_DTFM_INPUT_CNT:bigint,CVP_ELEMNT_VOICE_INPUT_CNT:bigint,CVP_ELEMNT_UTTERANCE:string,CVP_ELEMNT_INTERPRETATION:string,CVP_ELEMNT_VOICE_CONFIDENCE_PCT:string,CVP_ELEMNT_NO_INPUT_CNT:int,CVP_ELEMNT_REPEAT_TERMINATE_CNT:int>>, comment:null), FieldSchema(name:respondent_id, type:string, comment:null), FieldSchema(name:load_tag, type:bigint, comment:null), FieldSchema(name:call_service_agnt_typ, type:string, comment:null), FieldSchema(name:call_agent_tier_name, type:string, comment:null), FieldSchema(name:caller_service_type, type:string, comment:null), FieldSchema(name:caller_loyalty_name, type:string, comment:null), FieldSchema(name:nav_int_create_agnt_service_tier_name, type:string, comment:null), FieldSchema(name:cust_ops_call_src_sys_name, type:string, comment:null), FieldSchema(name:partition_day, type:string, comment:null)], location:s3://apiary-classic-387910953075-us-east-1-onprem-conversation/hadoop_dm_cust_ops_call_bkg_detail/cust_ops_call_src_sys_name=BKG/partition_day=2013-01-01, inputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat, outputFormat:org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat, compressed:false, numBuckets:-1, serdeInfo:SerDeInfo(name:null, serializationLib:org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe, parameters:{serialization.format=1, hive.serialization.extend.nesting.levels=true, hive.serialization.extend.additional.nesting.levels=true}), bucketCols:[], sortCols:[], parameters:{}, skewedInfo:SkewedInfo(skewedColNames:[], skewedColValues:[], skewedColValueLocationMaps:{}), storedAsSubDirectories:false), parameters:{transient_lastDdlTime=1695341797})
Time taken: 0.761 seconds, Fetched: 566 row(s)
```

# Analysis: does this partition trigger a resync under the comparator fix?

Comparing the Glue partition dump above against the Hive `DESCRIBE EXTENDED` dump for the
same partition (`cust_ops_call_src_sys_name=BKG/partition_day=2013-01-01`):

- **Partition-level `parameters`**: Hive has `{transient_lastDdlTime=1695341797}`; Glue has
  `{}`. This is the only difference at this level.
- **`lastAccessTime`**: Hive is `0` (`UNKNOWN`); Glue is `1970-01-01T00:00:00Z` (a real `Date`
  object, not `null`). Pre-fix, `Objects.equals(null, Date(0))` is `false` — a second,
  independent false-positive trigger.
- **`StorageDescriptor.parameters`**: `{}` on both sides (confirmed via `DESCRIBE EXTENDED`,
  which separates `sd.parameters` from `serdeInfo.parameters` unambiguously — the `DESCRIBE
  FORMATTED` "Storage Desc Params" label was actually displaying `SerDeInfo.parameters`, not
  `StorageDescriptor.parameters`). No difference.
- **`SerDeInfo.parameters`**: `{serialization.format=1, hive.serialization.extend.nesting.levels=true,
  hive.serialization.extend.additional.nesting.levels=true}` on both sides. No difference.
- No stats fields (`numRows`, `rawDataSize`, `totalSize`, `numFiles`, `COLUMN_STATS_ACCURATE`)
  are present anywhere in Hive's parameters for this partition — this table/partition has
  never had stats collected (old, one-time-loaded partition; `CreateTime` 2023-09-22 for
  `2013-01-01` source data).

**Verdict:**
- **Pre-fix comparator**: this partition would be classified as "different" and would trigger
  a resync — for two independent reasons (the `transient_lastDdlTime`-only parameter mismatch,
  and the `lastAccessTime` `null`-vs-`Date(0)` mismatch), neither of which reflects a real
  schema/data change.
- **Fixed comparator** (`transient_lastDdlTime` excluded from parameter equality checks;
  `lastAccessTime` comparison removed entirely): this partition is classified as **equal** —
  no resync triggered. This is a real, evidence-backed reproduction of the issue #147 false
  positive on the actual reported table.

**Why the fix's volatile-key list was narrowed to just `transient_lastDdlTime`:** the original
issue's mention of stats fields (`numRows`, `rawDataSize`, `totalSize`, `numFiles`,
`COLUMN_STATS_ACCURATE`) as contributing causes was a code-reading hypothesis, not something
observed on the actual failing table. This partition shows none of those fields present at
all, so it provides no evidence either way for filtering them — and filtering unverified keys
would suppress real update signals with a real (if usually low) cost. The fix therefore only
excludes `transient_lastDdlTime`, which is the one field directly confirmed — via this real
partition comparison — to cause a false positive on its own. Stats-field drift still correctly
triggers an update; this is locked in by
`HiveToGluePartitionComparatorTest.testStatsOnlyPartitionParametersStillForceUpdate` and
`testStatsOnlyStorageDescriptorAndSerdeParametersStillForceUpdate`.
