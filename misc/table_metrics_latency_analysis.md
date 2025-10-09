# Analysis of table metrics latency

Create the CSV file with the Dataiku scenario and upload it to Minio `admin-bucket` in folder `table_metrics_latency`

```bash
docker exec -ti minio-mc mc cp /data-transfer/table_metrics_latency.csv minio-1/admin-bucket/table_metrics_latency/
```

## Create Trino table to wrap the csv file

```sql
CREATE  TABLE minio.flight_db.table_metrics_latency (
    table_name               VARCHAR,
    latency            VARCHAR,
    timestamp             VARCHAR
)
WITH (
    external_location = 's3a://admin-bucket/table_metrics_latency/',
    format = 'CSV',
    skip_header_line_count = 1
);
```

## Create a view with correct datatypes

```sql
CREATE OR REPLACE VIEW minio.flight_db.metrics_latency_v
AS
SELECT table_name
, CAST(latency as bigint) as latency_ms
, CAST(timestamp as bigint) as timestamp_ms
FROM minio.flight_db.table_metrics_latency;
```

## Analysis

### Before Saturday, September 27, 2025 9:00:00 AM

```sql
select concat ('`' , table_name, '`')  as table_name 
, min(latency_ms)/1000 as min_latency_sec
, avg(latency_ms)/1000 as avg_latency_sec
, max(latency_ms)/1000 as max_latency_sec
, min(latency_ms)/1000/60 as min_latency_min
, avg(latency_ms)/1000/60 as avg_latency_min
, max(latency_ms)/1000/60 as max_latency_min
from minio.flight_db.metrics_latency_v
where timestamp_ms < 1758963600000
group by table_name
order by max(latency_ms) desc
LIMIT 40;
```

|table_name|min_latency_sec|avg_latency_sec|max_latency_sec|min_latency_min|avg_latency_min|max_latency_min|
|----------|---------------|---------------|---------------|---------------|---------------|---------------|
|`mra_measure`|294|1661.2997555555555|6752|4|27.68832925925926|112|
|`crm_servicingrole`|10|2647.0093636363636|6747|0|44.11682272727273|112|
|`mra_pricing`|332|937.0824347826086|6747|5|15.618040579710144|112|
|`mra_limit_usage`|6|1551.825711111111|6592|0|25.86376185185185|109|
|`crm_partner`|8|2864.297727272727|5350|0|47.73829545454545|89|
|`psv_dpr_cr0351`|8|1657.2389166666667|5201|0|27.62064861111111|86|
|`blb_equityoptionsasia1_out`|6|925.7510833333333|5156|0|15.429184722222223|85|
|`blb_corp_pfd_convert_quant_asia_bbid_px`|8|685.0556153846154|5154|0|11.417593589743591|85|
|`blb_equityoptionsasia2_out`|8|621.7444166666667|5058|0|10.362406944444444|84|
|`blb_equityoptionsasia3_out`|8|583.8153333333333|4589|0|9.730255555555555|76|
|`pss_espproductevents_bond_v2`|8|302.0586438356164|4530|0|5.034310730593607|75|
|`crm_partnernationality`|8|1171.6585833333334|4511|0|19.527643055555554|75|
|`crm_brslocationdetails`|6|508.96447368421053|4509|0|8.482741228070175|75|
|`cdm_par_kyc_wealth_profile_latest`|6|88.12453389830509|4501|0|1.4687422316384182|75|
|`cdm_par_individual_nationality_latest`|6|91.010125|4489|0|1.5168354166666667|74|
|`cdm_par_kyc_risk_considerations_latest`|6|79.77295689655172|4487|0|1.3295492816091954|74|
|`cdm_par_partner_to_partner_relation_latest`|6|86.98410833333334|4472|0|1.449735138888889|74|
|`cdm_par_business_relationship_latest`|6|109.01524137931035|4470|0|1.8169206896551726|74|
|`crm_brsclassification`|6|1486.559|4468|0|24.775983333333333|74|
|`cdm_par_client_classification_latest`|6|94.2767327586207|4463|0|1.571278879310345|74|
|`blb_equityoptionsasia5_out`|9|382.3695833333333|4443|0|6.372826388888888|74|
|`cdm_par_partner_latest`|6|124.85514035087719|4423|0|2.0809190058479534|73|
|`pae_gg_por_si_po_inscode`|9|1066.0324545454546|4392|0|17.767207575757578|73|
|`cdm_par_cash_account_latest`|6|94.88377876106195|4374|0|1.5813963126843658|72|
|`bcm_baszh_d_newordersingle`|9|884.079|4371|0|14.734649999999998|72|
|`pae_gg_por_post_line`|9|1239.1479090909093|4366|0|20.652465151515155|72|
|`cdm_par_servicing_user_latest`|6|80.77229090909091|4309|0|1.3462048484848486|71|
|`crm_brstoalertcontactdocument`|13|1400.6822727272727|4281|0|23.344704545454544|71|
|`cdm_par_document_latest`|6|140.04631683168316|4258|0|2.3341052805280524|70|
|`too_user_access_mc`|9|429.9613|4188|0|7.1660216666666665|69|
|`bcm_baszh_f_ordercancelrequest`|9|677.3762222222223|4188|0|11.289603703703705|69|
|`bcm_bjbf01_8_executionreport`|9|532.727125|4188|0|8.878785416666666|69|
|`bcm_ullf01_f_ordercancelrequest`|6|1653.3145|4153|0|27.555241666666667|69|
|`gat_lu_alertcusy`|6|319.59885185185186|4145|0|5.326647530864197|69|
|`gat_sg_alertcusy`|6|234.30085185185186|4132|0|3.9050141975308645|68|
|`gat_lu_alerttask`|6|511.3527407407407|4119|0|8.522545679012346|68|
|`gat_sg_alert`|6|416.65728571428576|4114|0|6.944288095238096|68|
|`bcm_jbcsgp_d_newordersingle`|9|1187.922111111111|4101|0|19.79870185185185|68|
|`gat_lu_alertaction`|6|650.3352857142856|4099|0|10.838921428571428|68|
|`gat_lu_alerthit`|6|160.45655555555555|4096|0|2.674275925925926|68|


### After Monday, September 29, 2025 9:00:00 AM and before  Saturday, October 4, 2025 9:00:37 AM

```sql
select concat ('`' , table_name, '`')  as table_name 
, min(latency_ms)/1000 as min_latency_sec
, avg(latency_ms)/1000 as avg_latency_sec
, max(latency_ms)/1000 as max_latency_sec
from minio.flight_db.metrics_latency_v
where timestamp_ms > 1759136400000
and timestamp_ms < 1759568437000
group by table_name
order by max(latency_ms) desc
LIMIT 40;
```

|table_name|min_latency_sec|avg_latency_sec|max_latency_sec|
|----------|---------------|---------------|---------------|
|`gat_ch_alerttransaction`|6|19.250625|77|
|`gat_mc_alerttransaction`|6|14.859777777777778|41|
|`gat_mc_alert`|6|13.177333333333333|40|
|`pss_espproductevents_productvalues_v2`|30|33.459|38|
|`mra_pricing`|20|27.781947368421054|38|
|`crm_brsclientriskprofile`|6|14.3392|37|
|`des_dep_dbew`|10|16.526400000000002|37|
|`gat_sg_alerttransaction`|6|15.970857142857144|35|
|`gat_hk_alertcustomer`|6|12.338666666666667|35|
|`lws_cr0202`|8|13.7378|34|
|`mra_measure`|18|23.90361111111111|33|
|`rpp_publication_types`|8|14.723|33|
|`rpp_languages`|9|14.444600000000001|30|
|`t2p_hk_nddeal`|9|14.072|29|
|`hit_publicationtypesubscriptioncounthistory`|9|13.9438|28|
|`ree_classification`|9|14.0634|28|
|`rpp_asset_classes`|9|13.608|28|
|`t2u_mc_nddeal`|8|16.4968|28|
|`t2u_mc_fxpostype`|7|12.4444|28|
|`t2u_mc_moneymarket`|8|15.1266|28|
|`memberzone_activityapi_activities_v1`|28|28.359|28|
|`hit_publicationpdfreadhistory`|9|14.766|27|
|`tem_limits`|8|13.67125|27|
|`tem_excesses`|8|13.59775|27|
|`pcmdocmgmt_pcdevents_agreementdata_v1`|15|21.162|27|
|`t2u_lu_dcmoptiontype`|7|11.341571428571429|27|
|`t2p_hk_repo`|8|12.6842|26|
|`t2p_hk_mddeal`|8|12.571200000000001|26|
|`gat_lu_investigation`|6|11.223|26|
|`gat_sg_alertcustomerrelation`|6|12.227333333333334|26|
|`gat_hk_alert`|6|10.944333333333335|25|
|`t2u_lu_aaarrangement`|7|12.3964|24|
|`iap_hk_iap2cst_portfolio`|7|10.54625|24|
|`gat_mc_alertaction`|6|10.481166666666667|24|
|`gat_hk_alerttransactionall`|6|10.918|23|
|`fma_assetrestriction`|18|20.3465|23|
|`blb_govt_national_euro_bbid_out`|8|12.0996|23|
|`t2u_mc_mddeal`|8|13.4824|23|
|`rpp_publications`|9|16.408|23|
|`tem_partner`|8|12.507|22|


### After Saturday, October 4, 2025 9:00:37 AM

```sql
select concat ('`' , table_name, '`')  as table_name 
, min(latency_ms)/1000 as min_latency_sec
, avg(latency_ms)/1000 as avg_latency_sec
, max(latency_ms)/1000 as max_latency_sec
from minio.flight_db.metrics_latency_v
where timestamp_ms > 1759568437000
group by table_name
order by max(latency_ms) desc
LIMIT 40;
```

|table_name|min_latency_sec|avg_latency_sec|max_latency_sec|
|----------|---------------|---------------|---------------|
|`pss_espproductevents_moneymarket_v1`|6|12.487142857142857|87|
|`lws_cr0801`|8|31.505|78|
|`lws_cr0102c`|8|31.653|77|
|`gat_sg_alerttransactionall`|8|37.682|66|
|`tra_facade_tradingdatastream_pnl_v3`|10|20.809166666666666|66|
|`crm_brsriskdetails`|6|26.065|56|
|`bcm_baszh_d_newordersingle`|8|25.212666666666667|55|
|`chsh_monitoring_traces_v1`|8|9.60258695652174|54|
|`lws_cr0804`|8|23.840333333333334|54|
|`ip2_ext_bny_positions`|8|12.937100000000001|54|
|`tra_facade_tradingdatastream_financialinstrumentposition_v5`|8|11.997583333333335|43|
|`tra_facade_tradingdatastream_transaction_v5`|8|11.741578947368422|43|
|`bcm_bas_variousmixed_d_newordersingle_v1`|12|27.5015|42|
|`pss_espproductevents_productvalues_v2`|33|36.86675|41|
|`aal_mc_ipp_u_users`|6|23.5255|40|
|`bcm_mur_fxtrade_forward`|8|17.53425|39|
|`des_fij_wrgn`|8|18.653|38|
|`crm_directtoclient`|8|17.552|35|
|`pcmcore_partnerprofile_industrycode_v1`|10|17.8386|34|
|`camp_indicatorsapi_participantindicators_v1`|8|14.8626|34|
|`bcm_jbzh_8_executionreport_v1`|17|25.717|33|
|`bcm_jbczh_f_ordercancelrequest`|33|33.579|33|
|`bcm_baszh_d_newordersingle_v1`|8|21.467|33|
|`gat_mc_alertcustomer`|8|17.947|32|
|`t2u_lu_aaproduct`|32|32.822|32|
|`mra_pricing`|20|24.3454|31|
|`bcm_jbcsgp_d_newordersingle`|8|16.859|30|
|`bcm_sg0020001_d_newordersingle_v1`|22|26.4355|30|
|`bcm_sg0020001_d_newordersingle`|8|15.4605|30|
|`pcmcore_pcmbrevents_brpurposes_v2_full`|8|13.1648|30|
|`bcm_blpemsx_8_executionreport`|30|30.639|30|
|`bcm_jbzh_8_executionreport`|11|21.032|30|
|`crx_br_crr_protocol_v2`|8|15.4106|30|
|`aal_lu_ipp_thirdparties`|9|19.5605|30|
|`tra_facade_tradingdatastream_instrumentidentifier_v4`|6|10.79725|29|
|`t2u_lu_fundstransfer`|8|15.659666666666666|29|
|`t2u_lu_fxpostype`|10|17.863333333333333|29|
|`gat_sg_alert`|8|18.7445|29|
|`tra_facade_tradingdatastream_instrument_v6`|8|10.22|29|
|`bcm_mur_fxstruk_accu`|9|16.321|28|






