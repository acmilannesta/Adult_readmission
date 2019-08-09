*********************************************************************************;
Libname saf2016 'C:\Users\hzhang3\Desktop\data2016\core';
libname hosp2016 'C:\Users\hzhang3\Desktop\data2016\hospital';
libname tx2016 'C:\Users\hzhang3\Desktop\data2016\transplant';
libname pred 'T:\biosprojs\USRDS\transplant outcomes\predictive analytics';
libname library 'C:\Users\hzhang3\Desktop\data2016\core';
options nofmterr;

/********USING THE COHORT BUILT IN PROGRAM COHORT MEDICARE INDEX HOSPITAL.103117.SAS 
************************************************************************************/
data cohort_preadm;
set pred.cohort_preadm;
by usrds_id;
run;        

data cohort_preadm_2;     
merge cohort_preadm(in=in1) 
      saf2016.medevid(keep=usrds_id hyper como_htn diabprim como_dm_ins como_dm_nomeds 
        como_dm_oral carfail como_chf ihd mi como_ashd 
        cva como_cvatia pvasc como_pvd pulmon como_copd
		mdcd medcov_medicaid mdcr medcov_medicare medcov_advantage)
      saf2016.patients(keep=usrds_id born pdis) 
      tx2016.txunos_ki_post_jul04(keep=usrds_id tdate dhiv agediab kpproc back_tbl_flush_dbl_enbki
        back_tbl_flush_kil back_tbl_flush_kir bmi bmi_l cmvigg dcmv_igg dcmv cold_isch_pump_ki_lt cold_isch_pump_ki_rt
		ebv_serostatus debv debna debv_igg dhdiab diabd dialdt dtype dnhtbeat donrel_u dphysical_capacity
        drecvdt funcstat funcstl dhbsag hbsagc dhcscrn dantihcv dhiv dhiv_antibody listdat physical_capacity
		physical_capacity_l preop_urine_protein protein_urine rabo dabo
		dage dwgt drace dhhyp dhdiab dcod dcreat dantihcv dhgt 
        rename=(tdate=tdate_unos));
by usrds_id;
if in1 and tdate=tdate_unos;

run; 

data cohort_preadm_3; 
set cohort_preadm_2;
by usrds_id;
if last.usrds_id;

if dhiv='N' then hiv_don_22='N';
else if dhiv='P' then hiv_don_22='P';
else hiv_don_22='U';

age_tx=int((tdate-born)/365.25+0.5);
diab_vintage_22=age_tx-agediab; 

if kpproc=101 then back_tbl_flush_22=back_tbl_flush_kil;
else if kpproc=102 then back_tbl_flush_22=back_tbl_flush_kir;
else if kpproc in (103, 104) then back_tbl_flush_22=back_tbl_flush_dbl_enbki;
format back_tbl_flush_22 btflush.;

bmi_change_22=bmi-bmi_l;

cmv_r=cmvigg;
cmv_d=dcmv; 
if cmv_d in (' ', 'C', 'ND', 'PD', 'U', '**other**') then cmv_d=dcmv_igg;
if cmv_d='N' and cmv_r='N' then cmv_react_risk_22=0;
else if cmv_r='P' then cmv_react_risk_22=1;
else if cmv_d='P' and cmv_r='N' then cmv_react_risk_22=2; 

if kpproc=101 then cold_isch_pump_ki_22=cold_isch_pump_ki_lt;
else if kpproc=102 then cold_isch_pump_ki_22=cold_isch_pump_ki_rt;
else if kpproc in (103, 104) then cold_isch_pump_ki_22=max(cold_isch_pump_ki_lt, cold_isch_pump_ki_rt);

los_22=dcdate-tdate; 

ebv_r=ebv_serostatus; 

if dtype='L' then do;
   if debv='Y' then ebv_d='P';
   else if devb='N' then ebv_d='N';
end;
if dtype='C' then do;
   if debna='P' or debv_igg='P' then ebv_d='P';
   else if debna='N' or debv_igg='N' then ebv_d='N';  
end;

if ebv_r='P' then ebv_react_risk_22=1;
else if ebv_d='N' and ebv_r='N' then ebv_react_risk_22=0;
else if ebv_d='P' and ebv_r='N' then ebv_react_risk_22=2;      

diab_don_22=diabd;
if diab_don_22=' ' then do;
   if dhdiab='1' then diab_don_22='N';
   if dhdiab in ('2', '3', '4', '5') then diab_don_22='Y';
   if dhdiab in ('998', ' ') then diab_don_22='U';
end;    

dialysis_vintage_22=tdate-first_se; 

no_hbeat_don_22=dnhtbeat;
if dtype='L' then no_hbeat_don_22='N';   

if donrel_u in (1, 2, 3, 4, 5, 6) then donrel_22='Biological';
else if donrel_u in (7, 8, 9, 10, 11, 12, 999) then donrel_22='Non-Biological';
else donrel_22='Unknown';

if dtype='C' then dphysical_capacity=998;
don_physical_capacity_22=dphysical_capacity;
if don_physical_capacity_22=. then don_physical_capacity_22=998;  

organ_recover_time_22=tdate-drecvdt; 

if funcstat in (1, 4100, 4090, 4080, 2100, 2090, 2080) then recip_funcstat_22=1;
else if funcstat in (2, 4070, 4060, 4050, 4040, 2070, 2060, 2050) then recip_funcstat_22=2;
else if funcstat in (3, 4030, 4020, 4010, 2040, 2030, 2020, 2010) then recip_funcstat_22=3;
else recip_funcstat_22=999;

if funcstl in (1, 4100, 4090, 4080, 2100, 2090, 2080) then recip_funcstat_l_22=1;
else if funcstl in (2, 4070, 4060, 4050, 4040, 2070, 2060, 2050) then recip_funcstat_l_22=2;
else if funcstl in (3, 4030, 4020, 4010, 2040, 2030, 2020, 2010) then recip_funcstat_l_22=3;
else recip_funcstat_l_22=999;

if recip_funcstat_22=999 or recip_funcstat_l_22=999 then recip_funcstat_change_22=999;
else if recip_funcstat_22<recip_funcstat_l_22 then recip_funcstat_change_22=1;
else if recip_funcstat_22=recip_funcstat_l_22 then recip_funcstat_change_22=2;
else if recip_funcstat_22>recip_funcstat_l_22 then recip_funcstat_change_22=3;

if dtype='L' then do;
   don_hbv_22=dhbsag;
   don_hcv_22=dhcscrn;
   don_hiv_22=dhiv_antibody;
end;
else if dtype='C' then do;
   don_hbv_22=hbsagc;
   don_hcv_22=dantihcv;
   don_hiv_22=dhiv;
end;         

wl_time_22=tdate-listdat;

if physical_capacity in (996, 998) or physical_capacity_l in (996, 998) then recip_phys_capacity_change_22=999;
else if physical_capacity<physical_capacity_l then recip_phys_capacity_change_22=1;
else if physical_capacity=physical_capacity_l then recip_phys_capacity_change_22=2;
else if physical_capacity>physical_capacity_l then recip_phys_capacity_change_22=3;

if protein_urine=' ' then do;
   if preop_urine_protein=1 then protein_urine='Yes';
   else if preop_urine_protein=2 then protein_urine='No';
   else if preop_urine_protein in (3, 998) then protein_urine='Unknown'; 
end;
don_urine_protein_22=protein_urine;
if don_urine_protein_22=' ' then don_urine_protein_22='Unknonwn'; 

if dabo='O' then abo_match_22='Yes';
else if dabo in ('A', 'A1', 'A2') and rabo in ('A', 'A1', 'AB', 'A1B', 'A2B') then abo_match_22='Yes';
else if dabo='B' and rabo in ('B', 'AB', 'A1B', 'A2B') then abo_match_22='Yes';
else if dabo in ('AB', 'A1B', 'A2B') and rabo in ('AB', 'A1B', 'A2B') then abo_match_22='Yes';
else if dabo ne ' ' and rabo ne ' ' then abo_match_22='No';
else abo_match_22='Unknown';   

time_esrd_tx_patients=tdate-first_se;

transplant_year=year(tdate);

if hyper='Y' then hypertension_medevid=1;
else if hyper='N' then hypertension_medevid=0;
if como_htn='Y' then hypertension_medevid=1;
else if como_htn='N' then hypertension_medevid=0;
 
if diabprim='Y' then pridiab_medevid=1;
if diabprim='N' then pridiab_medevid=0;
if como_dm_ins='Y' or como_dm_nomeds='Y' or como_dm_oral='Y' then pridiab_medevid=1;
else if como_dm_ins='N' and como_dm_nomeds='N' and como_dm_oral='N' then pridiab_medevid=0;
 
if carfail='Y' then chf_medevid=1;
else if carfail='N' then chf_medevid=0;
if como_chf='Y' then chf_medevid=1;
else if como_chf='N' then chf_medevid=0;
 
if ihd='Y' then cad_medevid=1;
else if ihd='N' then cad_medevid=0;
if mi='Y' then mi2_medevid=1;
else if mi='N' then mi2_medevid=0;
if como_ashd='Y' then cad_medevid=1;
else if como_ashd='N' then cad_medevid=0;
 
if cva='Y' then cvd_medevid=1;
else if cva='N' then cvd_medevid=0;
if como_cvatia='Y' then cvd_medevid=1;
else if como_cvatia='N' then cvd_medevid=0;
 
if pvasc='Y' then pvd_medevid=1;
else if pvasc='N' then pvd_medevid=0;
if como_pvd='Y' then pvd_medevid=1;
else if como_pvd='N' then pvd_medevid=0;
 
if pulmon='Y' then copd_medevid=1;
else if pulmon='N' then copd_medevid=0;
if como_copd='Y' then copd_medevid=1;
else if como_copd='N' then copd_medevid=0;
 
 
if mdcd='Y' then medicaid_esrd_medevid=1;
else if mdcd='N' then medicaid_esrd_medevid=0;
if medcov_medicaid='Y' then medicaid_esrd_medevid=1;
else if medcov_medicaid='N' then medicaid_esrd_medevid=0;
 
if mdcr='Y' then medicare_esrd_medevid=1;
else if mdcr='N' then medicare_esrd_medevid=0;
if medcov_medicare='Y' or medcov_advantage='Y' then medicare_esrd_medevid=1;
else if medcov_medicare='N' and medcov_advantage='N' then medicare_esrd_medevid=0;

if pdis in ('25000A', '25001A', '25040', '25041') then primary_cause=1;
if pdis in ('5829A', '5821A', '5831A', '5832C', '58381B', '58381C', '5804B', '5834C', '5800C', '5820A',
            '5832A',
            '5829', '5821', '5831', '58321', '58322', '58381', '58382', '5834', '5800', '5820') then primary_cause=2;
if pdis in ('7100E', '2870A', '7101B', '2831A', '4460C', '4464B', '5839C', '4462A', '5839B', 
            '7100', '2870', '7101', '28311', '4460', '4464', '58392', '44620', '44621', '58391') then primary_cause=3;
if pdis in ('75313A', '75314A', '75316A', '7595A', '7598A', '2700A', '2718B', '2727A', '7533A', '5839D', 
            '7532A', '7530B', '7567A', '7598B',
			'75313', '75314', '75316', '7595', '7598', '2700', '2718', '2727', '7533', '5839', '75321', '75322',
			'75329', '7530', '75671', '75989') then primary_cause=4;
if pdis in ('4039D', '4401A', '59381B', '59381E', 
            '40391', '4401', '59381', '59383') then primary_cause=5;
if pdis in ('1890B', '1899A', '2230A', '2239A', '2395A', '2395B', '20280A', '2030A', '2030B', '2773A', '99680A',
            '1890', '1899', '2230', '2239', '23951', '23952', '20280', '20300', '20308', '2773', '99680', '99681',
            '99682', '99683', '99684', '99685', '99686', '99687', '99689') then primary_cause=6;
if primary_cause=. and pdis ne ' ' then primary_cause=7;
if primary_cause=. then primary_cause=8;

/*******************************************************************************************************************
                                            Creation variables for KDRI assessment
*****************************************************************************************************************************************/

if dtype='C' then do;
   if .<dage<18 then dage_KDRI=0;
   if 18<=dage<=50 then dage_KDRI=1;
   if dage>50 then dage_KDRI=2;
   if dage=. then dage_KDRI=.;
   if .<dwgt<80 then dwgt_KDRI=1;
   if dwgt=. then dwgt_KDRI=.;
   if dwgt>=80 then dwgt_KDRI=0;
   ethnicity_KDRI=0;
   if drace=3 then ethnicity_KDRI=1;
   if drace=5 then ethnicity_KDRI=.;
   if DHHYP=1 then DHHYP_KDRI=0;
   if 2<=DHHYP<=5 then DHHYP_KDRI=1;
   if DHHYP=998 then DHHYP_KDRI=.;
   if DHDIAB=1 then DHDIAB_KDRI=0;
   if 2<=DHDIAB<=5 then DHDIAB_KDRI=1;
   if DHDIAB=998 then DHDIAB_KDRI=.;
   DCOD_KDRI=0;
   if DCOD=2 then DCOD_KDRI=1;
   if DCOD=998 then DCOD_KDRI=.;
   if .<DCREAT<=1.5 then DCREAT_KDRI=0;
   if DCREAT>1.5 then DCREAT_KDRI=1;
   if DCREAT=. then DCREAT_KDRI=.;
   if DANTIHCV="P" then HCV_KDRI=1;else HCV_KDRI=0;
   if DNHTBEAT="Y" then DCD_KDRI=1;
   if DNHTBEAT="N" then DCD_KDRI=0;
   if DNHTBEAT="U" then DCD_KDRI=.;

/**KDPI calculation**/
   if dage_KDRI=0 then xbeta_age_kdri=0.0128*(dage-40)-0.0194*(dage-18);
   if dage_KDRI=1 then xbeta_age_kdri=0.0128*(dage-40);
   if dage_KDRI=2 then xbeta_age_kdri=0.0128*(dage-40)+0.0107*(dage-50);
   if dhgt NE . then xbeta_hgt_kdri=-0.0464*(dhgt-170)/10;
   if dwgt_KDRI=0 then xbeta_wgt_kdri=0;
   if dwgt_KDRI=1 then xbeta_wgt_kdri=-0.0199*(dwgt-80)/5;
   if ethnicity_KDRI=0 then xbeta_ethnicity_kdri=0;
   if ethnicity_KDRI=1 then xbeta_ethnicity_kdri=0.1790;
   if DHHYP_KDRI=0 then xbeta_DHHYP_kdri=0;
   if DHHYP_KDRI=1 then xbeta_DHHYP_kdri=0.1260;
   if DHDIAB_KDRI=0 then xbeta_DHDIAB_kdri=0;
   if DHDIAB_KDRI=1 then xbeta_DHDIAB_kdri=0.1300;
   if DCOD_KDRI=0 then xbeta_DCOD_KDRI=0;
   if DCOD_KDRI=1 then xbeta_DCOD_KDRI=0.0881;
   if DCREAT_KDRI=0 then xbeta_DCREAT_KDRI=0.2200*(dcreat-1);
   if DCREAT_KDRI=1 then xbeta_DCREAT_KDRI=0.2200*(dcreat-1)-0.2090*(dcreat-1.5);
   if HCV_KDRI=0 then xbeta_HCV_KDRI=0;
   if HCV_KDRI=1 then xbeta_HCV_KDRI=0.2400;
   if DCD_KDRI=0 then xbeta_DCD_KDRI=0;
   if DCD_KDRI=1 then xbeta_DCD_KDRI=0.1330;
   if (xbeta_hgt_kdri NE . and xbeta_wgt_kdri NE . and xbeta_ethnicity_kdri NE . and xbeta_DHHYP_kdri NE . and 
   xbeta_DHDIAB_kdri NE . and xbeta_DCREAT_KDRI NE . and xbeta_DCD_KDRI NE .)
   then xbeta_kdri=sum(xbeta_age_kdri,xbeta_hgt_kdri,xbeta_wgt_kdri,xbeta_ethnicity_kdri,xbeta_DHHYP_kdri,
   xbeta_DHDIAB_kdri,xbeta_DCOD_KDRI,xbeta_DCREAT_KDRI,xbeta_HCV_KDRI,xbeta_DCD_KDRI);
   KDRI_RAO=exp(xbeta_kdri);
   KDRI_median=KDRI_RAO/1.20659821120231;

   if   0    < KDRI_median<= 0.50185050396296 then KDPI= 0    ;
   if   0.50185050396296 < KDRI_median<= 0.57926396655615 then KDPI= 1    ;
   if   0.57926396655615 < KDRI_median<= 0.60210405214618 then KDPI= 2    ;
   if   0.60210405214618 < KDRI_median<= 0.61860706087597 then KDPI= 3    ;
   if   0.61860706087597 < KDRI_median<= 0.63172253728813 then KDPI= 4    ;
   if   0.63172253728813 < KDRI_median<= 0.64174246250979 then KDPI= 5    ;
   if   0.64174246250979 < KDRI_median<= 0.65257721833424 then KDPI= 6    ;
   if   0.65257721833424 < KDRI_median<= 0.66144304129240 then KDPI= 7    ;
   if   0.66144304129240 < KDRI_median<= 0.66930795425145 then KDPI= 8    ;
   if   0.66930795425145 < KDRI_median<= 0.67613467154917 then KDPI= 9    ;
   if   0.67613467154917 < KDRI_median<= 0.68495851505834 then KDPI= 10   ;
   if   0.68495851505834 < KDRI_median<= 0.69238092545118 then KDPI= 11   ;
   if   0.69238092545118 < KDRI_median<= 0.70116993418609 then KDPI= 12   ;
   if   0.70116993418609 < KDRI_median<= 0.70839530230416 then KDPI= 13   ;
   if   0.70839530230416 < KDRI_median<= 0.71484995996445 then KDPI= 14   ;
   if   0.71484995996445 < KDRI_median<= 0.72355045025492 then KDPI= 15   ;
   if   0.72355045025492 < KDRI_median<= 0.73146156361777 then KDPI= 16   ;
   if   0.73146156361777 < KDRI_median<= 0.73867517266218 then KDPI= 17   ; 
   if   0.73867517266218 < KDRI_median<= 0.74482573217236 then KDPI= 18   ;
   if   0.74482573217236 < KDRI_median<= 0.75117171528009 then KDPI= 19   ;
   if   0.75117171528009 < KDRI_median<= 0.75726274050698 then KDPI= 20   ;
   if   0.75726274050698 < KDRI_median<= 0.76339216309683 then KDPI= 21   ;
   if   0.76339216309683 < KDRI_median<= 0.77016245604410 then KDPI= 22   ;
   if   0.77016245604410 < KDRI_median<= 0.77815853387671 then KDPI= 23   ;
   if   0.77815853387671 < KDRI_median<= 0.78577325759498 then KDPI= 24   ;
   if   0.78577325759498 < KDRI_median<= 0.79330937234446 then KDPI= 25   ;
   if   0.79330937234446 < KDRI_median<= 0.80102605487218 then KDPI= 26   ;
   if   0.80102605487218 < KDRI_median<= 0.80881634339535 then KDPI= 27   ;
   if   0.80881634339535 < KDRI_median<= 0.81645408301242 then KDPI= 28   ;
   if   0.81645408301242 < KDRI_median<= 0.82297866300788 then KDPI= 29   ;
   if   0.82297866300788 < KDRI_median<= 0.82861056043040 then KDPI= 30   ;
   if   0.82861056043040 < KDRI_median<= 0.83551664870164 then KDPI= 31   ;
   if   0.83551664870164 < KDRI_median<= 0.84237920447534 then KDPI= 32   ;
   if   0.84237920447534 < KDRI_median<= 0.85063835582234 then KDPI= 33   ;
   if   0.85063835582234 < KDRI_median<= 0.85867205560131 then KDPI= 34   ;
   if   0.85867205560131 < KDRI_median<= 0.86744340844365 then KDPI= 35   ;
   if   0.86744340844365 < KDRI_median<= 0.87493559196865 then KDPI= 36   ;
   if   0.87493559196865 < KDRI_median<= 0.88425923726334 then KDPI= 37   ;
   if   0.88425923726334 < KDRI_median<= 0.89213572088970 then KDPI= 38   ;
   if   0.89213572088970 < KDRI_median<= 0.90155682498225 then KDPI= 39   ;
   if   0.90155682498225 < KDRI_median<= 0.91002336451240 then KDPI= 40   ;
   if   0.91002336451240 < KDRI_median<= 0.91853891749904 then KDPI= 41   ;
   if   0.91853891749904 < KDRI_median<= 0.92642238918236 then KDPI= 42   ;
   if   0.92642238918236 < KDRI_median<= 0.93667753282169 then KDPI= 43   ;
   if   0.93667753282169 < KDRI_median<= 0.94627220848125 then KDPI= 44   ;
   if   0.94627220848125 < KDRI_median<= 0.95513269117685 then KDPI= 45   ;  
   if   0.95513269117685 < KDRI_median<= 0.96332560938776 then KDPI= 46   ;
   if   0.96332560938776 < KDRI_median<= 0.97286981380213 then KDPI= 47   ;
   if   0.97286981380213 < KDRI_median<= 0.98067418054028 then KDPI= 48   ;
   if   0.98067418054028 < KDRI_median<= 0.98863922209582 then KDPI= 49   ;
   if   0.98863922209582 < KDRI_median<= 1.00000000000001 then KDPI= 50   ;
   if   1.00000000000001 < KDRI_median<= 1.00964117503605 then KDPI= 51   ;
   if   1.00964117503605 < KDRI_median<= 1.01922791591536 then KDPI= 52   ;
   if   1.01922791591536 < KDRI_median<= 1.02814375370145 then KDPI= 53   ;
   if   1.02814375370145 < KDRI_median<= 1.03995245471443 then KDPI= 54   ;
   if   1.03995245471443 < KDRI_median<= 1.04732049763210 then KDPI= 55   ;
   if   1.04732049763210 < KDRI_median<= 1.05829571216862 then KDPI= 56   ;
   if   1.05829571216862 < KDRI_median<= 1.06990107987745 then KDPI= 57   ;
   if   1.06990107987745 < KDRI_median<= 1.08203572288809 then KDPI= 58   ;
   if   1.08203572288809 < KDRI_median<= 1.09239768943453 then KDPI= 59   ;
   if   1.09239768943453 < KDRI_median<= 1.10390356719827 then KDPI= 60   ; 
   if   1.10390356719827 < KDRI_median<= 1.11304666149313 then KDPI= 61   ;
   if   1.11304666149313 < KDRI_median<= 1.12296779252000 then KDPI= 62   ;
   if   1.12296779252000 < KDRI_median<= 1.13831715902115 then KDPI= 63   ;
   if   1.13831715902115 < KDRI_median<= 1.14912754216400 then KDPI= 64   ;
   if   1.14912754219564 < KDRI_median<= 1.16310714276401 then KDPI= 65   ;
   if   1.16310714276401 < KDRI_median<= 1.17401769710518 then KDPI= 66   ;
   if   1.17401769710518 < KDRI_median<= 1.18725770176371 then KDPI= 67   ;
   if   1.18725770176371 < KDRI_median<= 1.19924836193052 then KDPI= 68   ;
   if   1.19924836193052 < KDRI_median<= 1.20933932184919 then KDPI= 69   ;
   if   1.20933932184919 < KDRI_median<= 1.22019196592691 then KDPI= 70   ;
   if   1.22019196592691 < KDRI_median<= 1.23293190489985 then KDPI= 71   ;
   if   1.23293190489985 < KDRI_median<= 1.24754172563564 then KDPI= 72   ;
   if   1.24754172563564 < KDRI_median<= 1.26197126631414 then KDPI= 73   ;
   if   1.26197126631414 < KDRI_median<= 1.27652940807527 then KDPI= 74   ;
   if   1.27652940807527 < KDRI_median<= 1.29108867365711 then KDPI= 75   ;
   if   1.29108867365711 < KDRI_median<= 1.30322634729766 then KDPI= 76   ;
   if   1.30322634729766 < KDRI_median<= 1.31974032727877 then KDPI= 77   ;
   if   1.31974032727877 < KDRI_median<= 1.34013317786068 then KDPI= 78   ;
   if   1.34013317786068 < KDRI_median<= 1.35752039341624 then KDPI= 79   ;
   if   1.35752039341624 < KDRI_median<= 1.37362742404169 then KDPI= 80   ;
   if   1.37362742404169 < KDRI_median<= 1.38921521682408 then KDPI= 81   ;
   if   1.38921521682408 < KDRI_median<= 1.40902080061602 then KDPI= 82   ;
   if   1.40902080061602 < KDRI_median<= 1.42691532004134 then KDPI= 83   ;
   if   1.42691532004134 < KDRI_median<= 1.44739443046996 then KDPI= 84   ;
   if   1.44739443046996 < KDRI_median<= 1.46540696468729 then KDPI= 85   ;
   if   1.46540696468729 < KDRI_median<= 1.48946440482546 then KDPI= 86   ;
   if   1.48946440482546 < KDRI_median<= 1.52070523170853 then KDPI= 87   ;
   if   1.52070523170853 < KDRI_median<= 1.54752827294538 then KDPI= 88   ;
   if   1.54752827294538 < KDRI_median<= 1.57661318964456 then KDPI= 89   ;
   if   1.57661318964456 < KDRI_median<= 1.60544824102362 then KDPI= 90   ;
   if   1.60544824102362 < KDRI_median<= 1.63346413433423 then KDPI= 91   ;
   if   1.63346413433423 < KDRI_median<= 1.66681562619214 then KDPI= 92   ;
   if   1.66681562619214 < KDRI_median<= 1.71062533301544 then KDPI= 93   ;
   if   1.71062533301544 < KDRI_median<= 1.75496865346973 then KDPI= 94   ;
   if   1.75496865346973 < KDRI_median<= 1.80755842013345 then KDPI= 95   ;
   if   1.80755842013345 < KDRI_median<= 1.86685462410264 then KDPI= 96   ;
   if   1.86685462410264 < KDRI_median<= 1.94929505213229 then KDPI= 97   ;
   if   1.94929505213229 < KDRI_median<= 2.03882771972090 then KDPI= 98   ;
   if   2.03882771972090 < KDRI_median<= 2.19353631119986 then KDPI= 99   ;
   if   2.19353631119986 < KDRI_median<= 3.34436418231575 then KDPI= 100  ;
   if   3.34436418231575 < KDRI_median<= 999999999  then KDPI= 100  ;
   if KDPI=. then KDPI_class=9;
   if 0<=KDPI<=20 then KDPI_class=0;
   if 20<KDPI<=85 then KDPI_class=1;
   if 85<KDPI<=100 then KDPI_class=2;
end;

if kdpi_class>. then kdpi_class_final=kdpi_class;
else if dtype='L' then kdpi_class_final=3;

run;

proc format;
value cmv_risk 0='Low'
               1='Medium'
			   2='High';
value ebv_risk 0='Low'
               1='Medium'
			   2='High';
value don_funcstat 1='No activity limitations'
                   2='Some assistance'
    	    	   3='Total assistance'
				   999='NA/Unknown';
value don_funcstat_l 1='No activity limitations'
                     2='Some assistance'
				     3='Total assistance'
				     999='NA/Unknown';
value don_funcstat_change 1='Improvement'
                          2='No change'
                          3='Decline'
						  999='Unknown';
value recip_phys_capacity_change 1='Improvement'
                                 2='No change'
                                 3='Decline'
						         999='Unknown';
value primary_cause 1='Diabetes'
                    2='Primary GN'
					3='Secondary GN'
					4='Cystic/Hereditary/Congenital Disease'
					5='Hypertension'
					6='Neoplasms/Tumor'
					7='Other'
                    8='Missing';
run;

proc freq;
table primary_cause*pdis / missing;
run;

data cohort_preadm_4;
merge cohort_preadm_3(in=in1 keep=usrds_id tdate first_se) saf2016.rxhist(keep=usrds_id begdate enddate rxgroup);
by usrds_id;
if in1;
if .<enddate<tdate;
if rxgroup in ('1', '2', '3') then modality=1;
else if rxgroup in ('5', '7', '9') then modality=2;
else if rxgroup='A' then modality=3;
else if rxgroup='B' then modality=4;
else if rxgroup='D' then modality=5;
else if rxgroup='T' then modality=6;
else if rxgroup='X' then modality=7;
else if rxgroup='Z' then modality=8;

if modality ne 6;
run;

proc sort;
by usrds_id begdate modality;
run;

data cohort_preadm_5;
set cohort_preadm_4;
by usrds_id begdate;
modality_change=1;        **subject starts a new modality;
if usrds_id=lag(usrds_id) and modality=lag(modality) then modality_change=0;  **subject stays with the same modality as the previous observation;
run;

data cohort_preadm_6;
set cohort_preadm_5;
by usrds_id;
if modality_change=1;
run;

data cohort_preadm_7;
set cohort_preadm_6;
by usrds_id;
if first.usrds_id then modality_num=1;
if lag(usrds_id)=usrds_id then modality_num+1;
run;

data cohort_preadm_8;
set cohort_preadm_7;
by usrds_id;
if last.usrds_id;
keep usrds_id modality_num begdate first_se;
run;

data cohort_preadm_9;   ***GOING TO BE MERGED WITH COHORT_PREADM_3 LATER;
merge cohort_preadm_8 cohort_preadm_3;
by usrds_id;
dial_modal_change_rxhist=int(modality_num/dialysis_vintage_22+0.5);
if begdate+183>tdate then dial_change_pretx_rxhist='Yes';
else if begdate>. or tdate=first_se then dial_change_pretx_rxhist='No';
keep usrds_id dial_modal_change_rxhist dial_change_pretx_rxhist;
run;   

data fup_1yr_or_more;
set cohort_preadm_3;
by usrds_id;
if first_se+365<tdate;
run;

data fup_1yr_or_more_2;  
merge fup_1yr_or_more(in=in1 keep=usrds_id tdate first_se) saf2016.residenc(keep=usrds_id begres endres zipcode);
by usrds_id;
if in1;
if tdate-365<begres<=tdate;
run;

data fup_1yr_or_more_3;
set fup_1yr_or_more_2;
by usrds_id begres;
if first.usrds_id then zip_change=1; ***variable 'zip_change_pretx_residenc' indicates the number of CHANGES of residence zipcode; 
if lag(usrds_id)=usrds_id then zip_change+1;
run;

data fup_1yr_or_more_4;  
set fup_1yr_or_more_3;
by usrds_id;
if last.usrds_id;
rename zip_change=zip_change_pretx_residenc;
keep usrds_id zip_change;
run;

data fup_1yr_or_more_5;
merge fup_1yr_or_more(in=in1 keep=usrds_id) fup_1yr_or_more_4(in=in2 keep=usrds_id zip_change_pretx_residenc);
by usrds_id;
if in1 and not in2 then zip_change_pretx_residenc=0;
run;

data fup_lt_1yr;
set cohort_preadm_3;
by usrds_id;
if first_se+365>=tdate;
run;

data fup_lt_1yr_2;  
merge fup_lt_1yr(in=in1 keep=usrds_id tdate first_se) saf2016.residenc(keep=usrds_id begres endres zipcode);
by usrds_id;
if in1;
if first_se<=begres<=tdate;
run;

data fup_lt_1yr_3;
set fup_lt_1yr_2;
by usrds_id;
if first.usrds_id then zip_change=0;
if lag(usrds_id)=usrds_id then zip_change+1;
run;

data fup_lt_1yr_4;
set fup_lt_1yr_3;
by usrds_id;
if last.usrds_id;
if tdate>first_se then zip_change_pretx_residenc=int(zip_change/((tdate-first_se)/365));
else zip_change_pretx_residenc=0;
keep usrds_id zip_change_pretx_residenc;
run;

data fup_lt_1yr_5;
merge fup_lt_1yr(in=in1 keep=usrds_id) fup_lt_1yr_4(in=in2);
by usrds_id;
if in1 and not in2 then zip_change_pretx_residenc=0;
run;

data zipcode;
set fup_1yr_or_more_6 fup_lt_1yr_5;
by usrds_id;
run; 

data all;
merge cohort_preadm_3 cohort_preadm_9 zipcode;
by usrds_id;
run;

proc freq;
table hiv_don_22 cmv_react_risk_22 ebv_react_risk_22 diab_don_22 no_hbeat_don_22 donrel_22 don_physical_capacity_22
      recip_funcstat_22 recip_funcstat_l_22 recip_funcstat_change_22 don_hbv_22 don_hcv_22 don_hiv_22
      recip_phys_capacity_change_22 don_urine_protein_22 abo_match_22 transplant_year hypertension_medevid
	  pridiab_medevid chf_medevid cad_medevid mi2_medevid cvd_medevid pvd_medevid copd_medevid 
	  medicaid_esrd_medevid medicare_esrd_medevid primary_cause dial_modal_change_rxhist dial_change_pretx_rxhist
      zip_change_pretx_residenc kdpi kdpi_class kdpi_class_final;
run;

proc univariate;
var diab_vintage_22 back_tbl_flush_22 bmi_change_22 cold_isch_pump_ki_22 los_22 
    dialysis_vintage_22 organ_recover_time_22 wl_time_22 time_esrd_tx_patients;
run;

data pred.derivedvars_usrds;
set all;
keep usrds_id hiv_don_22 cmv_react_risk_22 ebv_react_risk_22 diab_don_22 no_hbeat_don_22 donrel_22 don_physical_capacity_22
     recip_funcstat_22 recip_funcstat_l_22 recip_funcstat_change_22 don_hbv_22 don_hcv_22 don_hiv_22
     recip_phys_capacity_change_22 don_urine_protein_22 abo_match_22 transplant_year hypertension_medevid
     pridiab_medevid chf_medevid cad_medevid mi2_medevid cvd_medevid pvd_medevid copd_medevid 
	 medicaid_esrd_medevid medicare_esrd_medevid primary_cause dial_modal_change_rxhist dial_change_pretx_rxhist
     zip_change_pretx_residenc diab_vintage_22 back_tbl_flush_22 bmi_change_22 cold_isch_pump_ki_22 los_22 
     dialysis_vintage_22 organ_recover_time_22 wl_time_22 time_esrd_tx_patients kdpi_class_final;
run;

proc freq data=pred.derivedvars_usrds;
table primary_cause;
format primary_cause primary_cause.;
run;
