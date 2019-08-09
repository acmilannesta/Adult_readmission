*********************************************************************************;
Libname saf2016 'C:\Users\hzhang3\Desktop\data2016\core';
libname hosp2016 'C:\Users\hzhang3\Desktop\data2016\hospital';
libname tx2016 'C:\Users\hzhang3\Desktop\data2016\transplant';
libname pred 'T:\biosprojs\USRDS\transplant outcomes\predictive analytics';
libname library 'C:\Users\hzhang3\Desktop\data2016\core';
options nofmterr;

data tx; 
merge saf2016.patients(keep=usrds_id first_se tx1date died) tx2016.txunos_ki_post_jul04; 
by usrds_id;
if mdy(1,1,2005)<=tdate<=mdy(12,31,2013) and tx1date=tdate;
if age>=18; 
keep usrds_id tdate admdate dcdate first_se died;
run;    ***125,363 first adult kidney transplants 1/1/2005-12/31/2013, some had duplicate records; 

data tx2;
set tx;
by usrds_id;
if first.usrds_id;
run;   ***125,347 unique first adult kidney transplants 1/1/2005-12/31/2013;

data tx_medicare;
merge tx2(in=in1) saf2016.payhist(keep=usrds_id begdate enddate mcare payer);
by usrds_id;
if in1;
if begdate<=tdate<=enddate and payer in ('MPAB', 'MPO');
run;       **45,948 subjects. Frequency table below shows more details;

/*****************************************************
proc freq;
table payer;
run;

PAYER                                     Frequency Percent 
Group Health Organization                 3143       3.80 
Medicare Primary, both Part A and Part B 43858      52.97 
Medicare Primary, Other                   2090       2.52 
Medicare as Secondary Payer with EGHP    26469      31.97 
Medicare as Secondary Payer, no EGHP       899       1.09 
Other/Unknown                             5767       6.96 
90 Day Waiting Period                      578       0.70 
*******************************************************/

data part1;
merge tx_medicare(in=in1) hosp2016.hosp_to2009;
by usrds_id;
if in1;
if clm_from<=tdate<=clm_thru and clm_from>.;
keep usrds_id tdate admdate dcdate first_se died;
run;

data part2;
merge tx_medicare(in=in1) hosp2016.hosp_2010on;
by usrds_id;
if in1;
if clm_from<=tdate<=clm_thru and clm_from>.;
keep usrds_id tdate admdate dcdate first_se died;
run;

data tx_medicare_hosp;
set part1 part2;
by usrds_id;
if last.usrds_id;
run;        **42,315 first transplants had Medicare Primary and index hospital records;

data pay_tx1;
merge tx_medicare_hosp(in=in1) 
      saf2016.payhist(keep=usrds_id begdate enddate mcare payer); 
by usrds_id;
if in1;
if begdate=. then delete;
if begdate>tdate then delete;
if .<enddate<first_se or .<enddate<tdate-365 then delete;
if payer in ('MPAB', 'MPO');
run;          ***46,858 pay records;        

proc sort data=pay_tx1;
by usrds_id begdate enddate;     
run;

data pay_tx1_nodup;
set pay_tx1;
by usrds_id begdate enddate;
if last.enddate;
run;        **46,858 observations with no duplicates;

proc transpose data=pay_tx1_nodup out=pay_tx1_trans;
var begdate enddate;
by usrds_id;
run;   

data begdate;
set pay_tx1_trans;
if _name_='BEGDATE';
array begdate(8) begdate1-begdate8;
array col(8) col1-col8;
do i=1 to 8;
   begdate(i)=col(i);
end;
keep usrds_id begdate1-begdate8;
run;      

data enddate;
set pay_tx1_trans;
if _name_='ENDDATE';
array enddate(8) enddate1-enddate8;
array col(8) col1-col8;
do i=1 to 8;
   enddate(i)=col(i);
end;
keep usrds_id enddate1-enddate8;
run;

data bothdate;
merge begdate(in=in1) enddate tx_medicare_hosp;
by usrds_id;
if in1;
array begdate (8) begdate1-begdate8;
array enddate (8) enddate1-enddate8;

if first_se=tdate then do;
   begdate1=.;
   enddate1=.;
end;

flag=0;     **checking to see whether there is overlap of insurance coverage dates. there was none;
do i=1 to 7;
   if .<begdate(i+1)<enddate(i) then flag+1;
end;

time_medicare=0;
do i=1 to 8;
   if enddate(i)>. then time_medicare=time_medicare+min(enddate(i), tdate)-max(begdate(i), first_se, tdate-365)+1;
end;

time_total=tdate-max(tdate-365, first_se)+1;

medicare_prop=time_medicare/time_total;

if first_se=tdate then do;
   preemp_tx=1;
   time_medicare=1;
end;
if preemp_tx=. then preemp_tx=0;

medicare_prop=time_medicare/time_total;

if time_total<366 then partial_time=1; 
else partial_time=0;

run;

proc freq;
table partial_time preemp_tx;
run;

data cohort;
set bothdate;

if medicare_prop>=11/12;

left_censor_date=max(tdate-365, first_se);

drop begdate1-begdate8 enddate1-enddate8 i flag;
run;   **36,215 adult subjects who had Primary Medicare and index hospitalization on the day of first transplant, and had 11/12 of the time in the prior year
         with Primary Medicare;

data preadm1;
merge cohort(in=in1) hosp2016.hosp_to2009(keep=usrds_id clm_from clm_thru);
by usrds_id;
if in1;
if clm_thru<left_censor_date or clm_from>=admdate then delete;
run; 

data preadm2;
merge cohort(in=in1) hosp2016.hosp_2010on(keep=usrds_id clm_from clm_thru);
by usrds_id;
if in1;
if clm_thru<left_censor_date or clm_from>=admdate then delete;
run;

data preadm_both;
set preadm1 preadm2;
by usrds_id;
run;

proc sort;
by usrds_id clm_from clm_thru;
run;

data preadm_both_2;
set preadm_both;
by usrds_id clm_from clm_thru;
if last.clm_from;
run;

data preadm_both_3;
set preadm_both_2;
by usrds_id clm_from clm_thru;
if first.usrds_id then preadm_count=1;
if lag(usrds_id)=usrds_id then preadm_count+1;
keep usrds_id clm_from clm_thru;
run;

proc transpose data=preadm_both_3 out=preadm_trans;
var clm_from clm_thru;
by usrds_id;
run;   

proc contents data=preadm_trans;
run;

data begdate;
set preadm_trans;
if _name_='CLM_FROM';
array clm_from(23) clm_from1-clm_from23;
array col(23) col1-col23;
do i=1 to 23;
   clm_from(i)=col(i);
end;
keep usrds_id clm_from1-clm_from23;
run;      

data enddate;
set preadm_trans;
if _name_='CLM_THRU';
array clm_thru(23) clm_thru1-clm_thru23;
array col(23) col1-col23;
do i=1 to 23;
   clm_thru(i)=col(i);
end;
keep usrds_id clm_thru1-clm_thru23;
run;

data preadm_both_4;
merge begdate enddate cohort;
by usrds_id;
array clm_from(23) clm_from1-clm_from23;
array clm_thru(23) clm_thru1-clm_thru23;
do i=1 to 22;           **take care of overlapping preadmission records;
   if clm_from(i)<=clm_from(i+1)<=clm_thru(i) and clm_from(i)<=clm_thru(i+1)<=clm_thru(i) then do;
      clm_from(i+1)=.;
	  clm_thru(i+1)=.;
   end;
end;

flag=0;
do i=1 to 22;
   if .<clm_from(i+1)<clm_thru(i) then flag+1;
end;
run;

proc freq; **checking whether there is still overlapping recoreds;
table flag;
run;

proc print;
where flag=1;
var usrds_id clm_from1-clm_from23 clm_thru1-clm_thru23;
format clm_from1-clm_from23 clm_thru1-clm_thru23 mmddyy8.;
run;

data preadm_both_5;
set preadm_both_4;
if usrds_id=1912699 then do;    **manually correcting overlapping records;
   clm_thru1=mdy(7,25,2011);
   clm_from2=.;
   clm_thru2=.;
end;
if usrds_id=3121925 then do;
   clm_thru1=mdy(12,4,2011);
   clm_from2=.;
   clm_thru2=.;
end;
run;

data preadm_both_6;
set preadm_both_5;
array clm_from(23) clm_from1-clm_from23;
array clm_thru(23) clm_thru1-clm_thru23;
flag=0;
do i=1 to 22;
   if .<clm_from(i+1)<clm_thru(i) then flag+1;
end;
run;

proc freq;          **checking one more time, resulted in none;
table flag;
run;

data cohort_preadm;
set preadm_both_6;
by usrds_id;

if partial_time=0 and preemp_tx=0 then cat=1;
if partial_time=1 and preemp_tx=0 then cat=2;
if preemp_tx=1 then cat=3;

array clm_from(23) clm_from1-clm_from23;
array clm_thru(23) clm_thru1-clm_thru23;

do i=1 to 23;
   if cat=3 then do;
      clm_from(i)=.;
	  clm_thru(i)=.;
   end;
end;

preadm_count=0;
preadm_days=0;
do i=1 to 23;
   if clm_from(i)>. then do;
      preadm_count=preadm_count+1;
      preadm_days=preadm_days+(min(admdate, clm_thru(i))-max(left_censor_date, clm_from(i)));
   end;
end;

drop i flag clm_from1-clm_from23 clm_thru1-clm_thru23;

run;

proc format;
value cat 1='Had ESRD services at least a year prior to transplants'
          2='Had ESRD services less than a year prior to transplants'
		  3='Had transplants on the same day as ESRD initiation';
run;

proc contents;
run;

proc freq;
table preemp_tx*partial_time cat;
format cat cat.;
run;

proc univariate;
var preadm_count preadm_days;
run;

proc sort;
by cat;
run;

proc univariate;
var preadm_count preadm_days;
by cat;
run;

proc sort data=cohort_preadm;
by usrds_id;
run;
/*
data pred.cohort_preadm;
set cohort_preadm;
by usrds_id;
run;
*/

data cohort_preadm_2;
set pred.cohort_preadm;
by usrds_id;
tx1date=tdate;
drop tdate;
run;

data tx2;
merge cohort_preadm_2(in=in1) tx2016.txunos_ki_post_jul04;
by usrds_id;
if in1;
if tx1date<tdate<=tx1date+365;
run;

data tx2_2;
set tx2;
by usrds_id;
if first.usrds_id;
tx2date=tdate;
keep usrds_id tx2date;
run;

data cohort_preadm_3;
merge cohort_preadm(in=in1) tx2_2;
by usrds_id;
if 0<tx2date-dcdate<=30 then censor_30day=tx2date;
if 0<tx2date-dcdate<=90 then censor_90day=tx2date;
if 0<tx2date-dcdate<=365 then censor_365day=tx2date;
run;

data hosp1;
merge cohort_preadm_3(in=in1) hosp2016.hosp_to2009;
by usrds_id;
if in1;
if clm_from>dcdate;
run;

data hosp2;
merge cohort_preadm_3(in=in1) hosp2016.hosp_2010on;
by usrds_id;
if in1;
if clm_from>dcdate;
run;

data hosp_all;
set hosp1 hosp2;
run;

proc sort;
by usrds_id clm_from;
run;

data hosp_all2;
merge hosp_all;
by usrds_id clm_from;
if last.clm_from;
run;

/*************THIS IS TO ALL CREATE READMISSIONS AFTER TRANSPLANTS**********/
data readmit;
set hosp_all2;
keep usrds_id clm_from clm_thru;
run;

proc transpose data=readmit out=readmit_trans;
var clm_from clm_thru;
by usrds_id;
run;

data readmit_trans_in;
set readmit_trans;
if _name_='CLM_FROM';
array readmit_in (113) readmit_in1-readmit_in113;
array col (113) col1-col113;
do i=1 to 113;
   readmit_in(i)=col(i);
end;
keep usrds_id readmit_in1-readmit_in113;
run;      

data readmit_trans_out;
set readmit_trans;
if _name_='CLM_THRU';
array readmit_out (113) readmit_out1-readmit_out113;
array col (113) col1-col113;
do i=1 to 113;
   readmit_out(i)=col(i);
end;
keep usrds_id readmit_out1-readmit_out113;
run;  

data readmit_all;
merge readmit_trans_in readmit_trans_out;
by usrds_id;
run;     **this dataset contains all the readmissions after transplants;
/****************************END OF THE CREATION OF READMISSIONS******************************************************/

data hosp_30day;
set hosp_all2;
by usrds_id clm_from;
if .<clm_from<=min(dcdate+30, censor_30day);
if first.usrds_id then count_hosp_30day=1;
if usrds_id=lag(usrds_id) then count_hosp_30day+1;
run;  

data hosp_30day_2;
set hosp_30day;
by usrds_id;
if last.usrds_id;
if .<clm_thru<censor_30day then count_hosp_30day=count_hosp_30day+1;
keep usrds_id count_hosp_30day;
run; 

data hosp_90day;
set hosp_all2;
by usrds_id clm_from;
if .<clm_from<=min(dcdate+90, censor_90day);
if first.usrds_id then count_hosp_90day=1;
if usrds_id=lag(usrds_id) then count_hosp_90day+1;
run;    

data hosp_90day_2;
set hosp_90day;
by usrds_id;
if last.usrds_id;
if .<clm_thru<censor_90day then count_hosp_90day=count_hosp_90day+1;
keep usrds_id count_hosp_90day;
run; 

data hosp_365day;
set hosp_all2;
by usrds_id clm_from;
if .<clm_from<=min(dcdate+365, censor_365day);
if first.usrds_id then count_hosp_365day=1;
if usrds_id=lag(usrds_id) then count_hosp_365day+1;
run;

data hosp_365day_2;
set hosp_365day;
by usrds_id;
if last.usrds_id;
if .<clm_thru<censor_365day then count_hosp_365day=count_hosp_365day+1;
keep usrds_id count_hosp_365day;
run;  

data hosp_tx;      ****THIS DATASET CONTAINS ALL READMISSIONS AFTER TRANSPLANTS*****;
merge cohort_preadm_3 hosp_30day_2 hosp_90day_2 hosp_365day_2 readmit_all;
by usrds_id;
if count_hosp_30day=. then count_hosp_30day=0;
if count_hosp_90day=. then count_hosp_90day=0;
if count_hosp_365day=. then count_hosp_365day=0;

if count_hosp_30day>0 then hosp_30day=1; else hosp_30day=0;
if count_hosp_90day>0 then hosp_90day=1; else hosp_90day=0;
if count_hosp_365day>0 then hosp_365day=1; else hosp_365day=0;

run;

proc contents;
run;

proc freq;
table hosp_30day hosp_90day hosp_365day;
run;
***hospital readmission, 30 day 31.8%, 90 day 44.9%, 365 day 62.6%;

data preadm1;
merge cohort_preadm(in=in1 keep=usrds_id admdate left_censor_date) hosp2016.hosp_to2009;
by usrds_id;
if in1;
if clm_thru<left_censor_date or clm_from>=admdate then delete;
run; 

data preadm2;
merge cohort_preadm(in=in1 keep=usrds_id admdate left_censor_date) hosp2016.hosp_2010on;
by usrds_id;
if in1;
if clm_thru<left_censor_date or clm_from>=admdate then delete;
run;

data preadm_both;
set preadm1 preadm2;
by usrds_id;
run;

%macro ICD9_E_CH(library=, dataset=, OUTPUT=);
data &library..&OUTPUT;
	set &library..&dataset (keep=usrds_id hsdiag1-hsdiag25);

	/*DC: Disease Codes*/
	%LET DC1=%STR('410','412');
	%LET DC2=%STR('39891','40201','40211','40291','40401','40403','40411','40413','40491','40493','4254','4255','4257','4258','4259','428');
	%LET DC3=%STR('0930','4373','440','441','4431','4432','4438','4439','4471','5571','5579','V434');
	%LET DC4=%STR('36234','430','431','432','433','434','435','436','437','438');
	%LET DC5=%STR('290','2941','3312');          
	%LET DC6=%STR('4168','4169','490','491','492','493','494','495','496','500','501','502','503','504','505','5064','5081','5088');
	%LET DC7=%STR('4465','7100','7101','7102','7103','7104','7140','7141','7142','7148','725');
	%LET DC8=%STR('531','532','533','534');      
	%LET DC9=%STR('07022','07023','07032','07033','07044','07054','0706','0709','570','571','5733','5734','5738','5739','V427');
	%LET DC10=%STR('2500','2501','2502','2503','2508','2509');
	%LET DC11=%STR('2504','2505','2506','2507');  
	%LET DC12=%STR('3341','342','343','3440','3441','3442','3443','3444','3445','3446','3449');
	%LET DC13=%STR('40301','40311','40391','40402','40403','40412','40413','40492','40493','582','5830','5831','5832','5834','5836','5837','585','586','5880','V420','V451','V56');
	%LET DC14=%STR('140','141','142','143','144','145','146','147','148','149','150','151','152','153','154','155','156','157','158','159','160','161','162','163','164','165','170','171','172','174','175','176','179','180','181','182','183','184','185','186','187','188','189','190','191','192','193','194','195','200','201','202','203','204','205','206','207','208','2386');
	%LET DC15=%STR('4560','4561','4562','5722','5723','5724','5728');
	%LET DC16=%STR('196','197','198','199');      
	%LET DC17=%STR('042','043','044');
	/**Myocardial Infarction**/
	%LET DIS1=MI;
	%LET LBL1=%STR(Myocardial Infarction);

	/**Congestive Heart Failure**/
	%LET DIS2=CHF;
	%LET LBL2=%STR(Congestive Heart Failure);

	/**Periphral Vascular Disease**/
	%LET DIS3=PVD;
	%LET LBL3=%STR(Periphral Vascular Disease);

	/**Cerebrovascular Disease**/
	%LET DIS4=CEVD;
	%LET LBL4=%STR(Cerebrovascular Disease);

	/**Dementia**/
	%LET DIS5=DEM;
	%LET LBL5=%STR(Dementia);

	/*Chronic Pulmonary Disease*/
	%LET DIS6=COPD;
	%LET LBL6=%STR(Chronic Pulmonary Disease);

	/**Connective Tissue Disease-Rheumatic Disease**/
	%LET DIS7=Rheum;
	%LET LBL7=%STR(Connective Tissue Disease-Rheumatic Disease);

	/**Peptic Ulcer Disease**/   
	%LET DIS8=PUD;
	%LET LBL8=%STR(Peptic Ulcer Disease);

	/**Mild Liver Disease **/
	%LET DIS9=MILDLD;
	%LET LBL9=%STR(Mild Liver Disease);

	/**Diabetes without complications**/
	%LET DIS10=DIAB_NC;
	%LET LBL10=%STR(Diabetes without complications);

	/**Diabetes with complications**/
	%LET DIS11=DIAB_C;
	%LET LBL11=%STR(Diabetes with complications);

	/**Paraplegia and Hemiplegia**/
	%LET DIS12=PARA;
	%LET LBL12=%STR(Paraplegia and Hemiplegia);

	/**Renal Disease**/
	%LET DIS13=RD;
	%LET LBL13=%STR(Renal Disease);

	/**Cancer**/
	%LET DIS14=CANCER;
	%LET LBL14=%STR(Cancer);

	/**Moderate or Severe Liver Disease**/
	%LET DIS15=MSLD;
	%LET LBL15=%STR(Moderate or Severe Liver Disease);

	/**Metastatic Carcinoma **/
	%LET DIS16=METS;
	%LET LBL16=%STR(Metastatic Carcinoma);

	/**AIDS/HIV**/
	%LET DIS17=HIV;
	%LET LBL17=%STR(AIDS/HIV);

	%do DI=1 %to 17;/*ICD9-E Charlson: 17 groups*/
		A&DI=0; 						
		%do DX=1 %to 25; 	/*HSDIAG1 - HSDIAG25*/
			B&DX=0;
			%do SN=3 %to 5;
				if substr(hsdiag&DX,1,&SN) in (&&DC&DI) then C&SN=1;else C&SN=0;
				B&DX=B&DX +C&SN;
				drop C&SN;
			%end;
			A&DI=A&DI+B&DX;
			DROP B&DX;
		%end;
		if A&DI>0 then 	ICD9_E_CH_&&DIS&DI=1;else ICD9_E_CH_&&DIS&DI=0;
		label ICD9_E_CH_&&DIS&DI = &&LBL&DI;
		DROP A&DI;	
	%end;
		
run;
%mend ICD9_E_CH;
%ICD9_E_CH(library=work, dataset=preadm_both, OUTPUT=ICD9_E_CH);

data ci;
set icd9_e_ch;
ci=sum(2*icd9_e_ch_chf, 2*icd9_e_ch_dem, icd9_e_ch_copd, icd9_e_ch_rheum, 2*icd9_e_ch_mildld,
       icd9_e_ch_diab_c, 2*icd9_e_ch_para, icd9_e_ch_rd, 2*icd9_e_ch_cancer, 4*icd9_e_ch_msld,
       6*icd9_e_ch_mets, 4*icd9_e_ch_hiv);
if icd9_e_ch_diab_c=1 or icd9_e_ch_diab_nc=1 then pridiab_update=1; else pridiab_update=0;
chf_update=icd9_e_ch_chf;
copd_update=icd9_e_ch_copd;
pvd_update=icd9_e_ch_pvd;
cvd_update=icd9_e_ch_cevd;
keep usrds_id pridiab_update chf_update copd_update pvd_update cvd_update;
run;

proc sort data=ci;
by usrds_id pridiab_update;
run;

data pridiab_update;
set ci;
by usrds_id pridiab_update;
if last.usrds_id;
keep usrds_id pridiab_update;
run;

proc sort data=ci;
by usrds_id chf_update;
run;

data chf_update;
set ci;
by usrds_id chf_update;
if last.usrds_id;
keep usrds_id chf_update;
run;

proc sort data=ci;
by usrds_id copd_update;
run;

data copd_update;
set ci;
by usrds_id copd_update;
if last.usrds_id;
keep usrds_id copd_update;
run;

proc sort data=ci;
by usrds_id pvd_update;
run;

data pvd_update;
set ci;
by usrds_id pvd_update;
if last.usrds_id;
keep usrds_id pvd_update;
run;

proc sort data=ci;
by usrds_id cvd_update;
run;

data cvd_update;
set ci;
by usrds_id cvd_update;
if last.usrds_id;
keep usrds_id cvd_update;
run;

data medevid;      
set saf2016.medevid; 
by usrds_id;
if last.usrds_id;
if hyper='Y' then hypertension=1;
else if hyper='N' then hypertension=0;
if como_htn='Y' then hypertension=1;
else if como_htn='N' then hypertension=0;

if diabprim='Y' then pridiab=1;
if diabprim='N' then pridiab=0;
if como_dm_ins='Y' or como_dm_nomeds='Y' or como_dm_oral='Y' then pridiab=1;
else if como_dm_ins='N' and como_dm_nomeds='N' and como_dm_oral='N' then pridiab=0; 

if carfail='Y' then chf=1;
else if carfail='N' then chf=0;
if como_chf='Y' then chf=1;
else if como_chf='N' then chf=0;

if ihd='Y' then cad=1;
else if ihd='N' then cad=0;
if mi='Y' then mi2=1;
else if mi='N' then mi2=0;
*if cad=1 or mi2=1 then cadother=1; else if cad=0 and mi2=0 then cadother=0;
if como_ashd='Y' then cad=1;
else if como_ashd='N' then cad=0;
*cadother=cad;

if cva='Y' then cvd=1;
else if cva='N' then cvd=0;
if como_cvatia='Y' then cvd=1;
else if como_cvatia='N' then cvd=0;

if pvasc='Y' then pvd=1;
else if pvasc='N' then pvd=0;
if como_pvd='Y' then pvd=1;
else if como_pvd='N' then pvd=0;
/*
if cararr='Y' then arrest=1;
else if cararr='N' then arrest=0;
if dysrhyt='Y' then dyrrhy=1;
else if dysrhyt='N' then dyrrhy=0;
if arrest=1 or dyrrhy=1 then arrestother=1; else if arrest=0 and dyrrhy=0 then arrestother=0;
*/
if pulmon='Y' then copd=1;
else if pulmon='N' then copd=0;
if como_copd='Y' then copd=1;
else if como_copd='N' then copd=0;

if height>0 then bmi=weight/((height/100)**2);

if mdcd='Y' then medicaid_esrd=1;
else if mdcd='N' then medicaid_esrd=0;
if medcov_medicaid='Y' then medicaid_esrd=1;
else if medcov_medicaid='N' then medicaid_esrd=0;

if mdcr='Y' then medicare_esrd=1;
else if mdcr='N' then medicare_esrd=0;
if medcov_medicare='Y' or medcov_advantage='Y' then medicare_esrd=1;
else if medcov_medicare='N' and medcov_advantage='N' then medicare_esrd=0;

if empgrp='Y' then grphealth_esrd=1;
else if empgrp='N' then grphealth_esrd=0;
if medcov_group='Y' then grphealth_esrd=1;
else if medcov_group='N' then grphealth_esrd=0;

if othcov='Y' then othcov_esrd=1;
else if othcov='N' then othcov_esrd=0;
if medcov_other='Y' or medcov_meddva='Y' then othcov_esrd=1;
else if medcov_other='N' and medcov_meddva='N' then othcov_esrd=0;

if nocov='Y' then nocov_esrd=1;
else if nocov='N' then nocov_esrd=0;
if medcov_none='Y' then nocov_esrd=1;
else if medcov_none='N' then nocov_esrd=0;

if medicare_esrd=1 and medicaid_esrd=1 then medicare_medicaid=1;
else if (medicare_esrd=. and medicaid_esrd=0) or (medicare_esrd=0 and medicaid_esrd=.) or (medicare_esrd=0 and medicaid_esrd=0)
     or (medicare_esrd=0 and medicaid_esrd=1) or (medicare_esrd=1 and medicaid_esrd=0) then medicare_medicaid=0;  

keep usrds_id hypertension pridiab chf cad cvd pvd copd pdis disgrpc bmi album heglb medicaid_esrd medicare_esrd grphealth_esrd othcov_esrd nocov_esrd
     medicare_medicaid empcur race ethn como_tobac smoke como_alcho;
run; 

data all;
merge cohort_preadm(in=in1) medevid pridiab_update chf_update copd_update pvd_update cvd_update;
by usrds_id;
if in1;
if pridiab_update=. then pridiab_update=0;
if chf_update=. then chf_update=0;
if copd_update=. then copd_update=0;
if pvd_update=. then pvd_update=0;
if cvd_update=. then cvd_update=0;

pridiab_all=max(pridiab, pridiab_update);
chf_all=max(chf, chf_update);
copd_all=max(copd, copd_update);
pvd_all=max(pvd, pvd_update);
cvd_all=max(cvd, cvd_update);

keep usrds_id admdate album bmi como_alcho como_tobac dcdate died disgrpc empcur race ethn first_se heglb pdis 
     smoke chf_all copd_all cvd_all hypertension left_censor_date medicare_prop partial_time preadm_count 
	 preadm_days preemp_tx pridiab_all pvd_all tdate time_medicare time_total;
run;

proc freq;
table pridiab_all chf_all copd_all pvd_all cvd_all;
run;

data control; 
merge saf2016.patients(keep=usrds_id first_se tx1date died) tx2016.txunos_ki_post_jul04(keep=usrds_id tdate age) 
      cohort_preadm(keep=usrds_id in=in1) medevid; 
by usrds_id;
if mdy(1,1,2005)<=tdate<=mdy(12,31,2013) and tx1date=tdate;
if age>=18; 
if in1 then cohort=1; else cohort=0;
run;

proc sort;
by usrds_id;
run;

data control_2;
set control;
by usrds_id;
if first.usrds_id;
run;

proc freq;
table (race ethn como_alcho como_tobac smoke hypertension chf cad cvd pvd copd pdis disgrpc  
      empcur)*cohort / chisq;
run;

proc sort;
by cohort;
run;

proc ttest;
var age album bmi heglb;
class cohort;
run;

proc freq;
table cohort;
run;























