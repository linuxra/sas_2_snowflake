/* ============================================================
   Comprehensive PROC FREQ Examples — All Major Variations
   ============================================================ */

/* ----- 1. Simple one-way frequency ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin;
RUN;

/* ----- 2. One-way with options ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin / NOCUM NOPERCENT;
RUN;

/* ----- 3. Two-way cross-tabulation ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type;
RUN;

/* ----- 4. Two-way with chi-square ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / CHISQ;
RUN;

/* ----- 5. Two-way with all statistical options ----- */
PROC FREQ DATA=mylib.clinical_trial;
  TABLES treatment * response / CHISQ CMH MEASURES FISHER EXACT CHISQ FISHER
    RELRISK RISKDIFF OR CL ALPHA=0.05;
RUN;

/* ----- 6. N-way (3-way) cross-tabulation ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type * drivetrain;
RUN;

/* ----- 7. Parenthesised expansion ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES (origin type) * drivetrain / CHISQ NOROW NOCOL;
RUN;

/* ----- 8. Multiple TABLES statements ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin;
  TABLES type;
  TABLES origin * type / CHISQ;
RUN;

/* ----- 9. OUTPUT dataset ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / OUT=freq_output OUTPCT OUTCUM;
RUN;

/* ----- 10. BY statement ----- */
PROC SORT DATA=sashelp.cars OUT=cars_sorted;
  BY origin;
RUN;

PROC FREQ DATA=cars_sorted;
  BY origin;
  TABLES type / CHISQ;
RUN;

/* ----- 11. WEIGHT statement ----- */
PROC FREQ DATA=survey_data;
  TABLES region * satisfaction;
  WEIGHT sample_weight;
RUN;

/* ----- 12. WHERE clause ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / CHISQ;
  WHERE msrp GT 30000 AND origin NE 'Asia';
RUN;

/* ----- 13. FORMAT statement ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type;
  FORMAT origin $upcase. type $upcase.;
RUN;

/* ----- 14. LIST format ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / LIST;
RUN;

/* ----- 15. CROSSLIST format ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / CROSSLIST;
RUN;

/* ----- 16. MISSING option ----- */
PROC FREQ DATA=mydata;
  TABLES status * category / MISSING;
RUN;

/* ----- 17. SPARSE option ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / SPARSE;
RUN;

/* ----- 18. ORDER= option ----- */
PROC FREQ DATA=sashelp.cars ORDER=FREQ;
  TABLES origin;
RUN;

/* ----- 19. Expected counts + cell chi-square ----- */
PROC FREQ DATA=sashelp.cars;
  TABLES origin * type / EXPECTED CELLCHI2 DEVIATION TOTPCT;
RUN;

/* ----- 20. AGREE (Kappa, McNemar) ----- */
PROC FREQ DATA=paired_data;
  TABLES rater1 * rater2 / AGREE;
RUN;

/* ----- 21. TREND test ----- */
PROC FREQ DATA=dose_response;
  TABLES dose * outcome / TREND;
RUN;

/* ----- 22. BINOMIAL test (one-way) ----- */
PROC FREQ DATA=coin_flips;
  TABLES result / BINOMIAL;
RUN;

/* ----- 23. SCORES= option ----- */
PROC FREQ DATA=ordinal_data;
  TABLES exposure * disease / CMH SCORES=RIDIT;
RUN;

/* ----- 24. EXACT with specific tests ----- */
PROC FREQ DATA=small_sample;
  TABLES group * outcome / EXACT CHISQ FISHER;
RUN;

/* ----- 25. NLEVELS proc option ----- */
PROC FREQ DATA=sashelp.cars NLEVELS;
  TABLES origin type;
RUN;

/* ----- 26. Complex real-world clinical trial ----- */
PROC FREQ DATA=work.adsl ORDER=FREQ;
  TABLES trt01p * sex / CHISQ FISHER EXACT CHISQ
    RELRISK RISKDIFF OR CL ALPHA=0.05
    NOROW NOCOL NOPERCENT
    OUT=work.sex_by_trt OUTPCT;
  TABLES trt01p * race / CHISQ CMH MEASURES
    EXPECTED CELLCHI2 DEVIATION
    MISSING SPARSE;
  BY siteid;
  WEIGHT randwt;
  WHERE saffl EQ 'Y' AND ittfl EQ 'Y';
RUN;
