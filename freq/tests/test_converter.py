"""
Tests for the SAS-to-Snowpark PROC FREQ converter.
Run with: python -m pytest tests/
"""

import pytest
from sas_to_snowpark.parser import ProcFreqParser
from sas_to_snowpark.converter import convert, ProcFreqConverter


@pytest.fixture
def parser():
    return ProcFreqParser()


# ====================================================================
# Parser tests
# ====================================================================

class TestParserBasic:
    """Test parsing of basic PROC FREQ structures."""

    def test_simple_oneway(self, parser):
        sas = """
        PROC FREQ DATA=mylib.sales;
          TABLES region;
        RUN;
        """
        blocks = parser.parse(sas)
        assert len(blocks) == 1
        b = blocks[0]
        assert b.input_dataset == "sales"
        assert b.library == "mylib"
        assert len(b.tables) == 1
        assert b.tables[0].variables == ["region"]

    def test_twoway_cross(self, parser):
        sas = """
        PROC FREQ DATA=cars;
          TABLES origin * type;
        RUN;
        """
        blocks = parser.parse(sas)
        assert len(blocks) == 1
        t = blocks[0].tables[0]
        assert t.variables == ["origin", "type"]

    def test_nway(self, parser):
        sas = """
        PROC FREQ DATA=data1;
          TABLES a * b * c;
        RUN;
        """
        blocks = parser.parse(sas)
        t = blocks[0].tables[0]
        assert t.variables == ["a", "b", "c"]

    def test_multiple_tables(self, parser):
        sas = """
        PROC FREQ DATA=ds;
          TABLES x;
          TABLES y;
          TABLES x * y / CHISQ;
        RUN;
        """
        blocks = parser.parse(sas)
        assert len(blocks[0].tables) == 3

    def test_parenthesised_expansion(self, parser):
        sas = """
        PROC FREQ DATA=ds;
          TABLES (a b) * c;
        RUN;
        """
        blocks = parser.parse(sas)
        t = blocks[0].tables[0]
        assert len(t.expansions) == 2
        assert ["a", "c"] in t.expansions
        assert ["b", "c"] in t.expansions


class TestParserOptions:
    """Test parsing of table options."""

    def test_chisq(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / CHISQ; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.chisq is True

    def test_fisher(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / FISHER; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.fisher is True

    def test_cmh_measures(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / CMH MEASURES; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.cmh is True
        assert t.measures is True

    def test_display_options(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a / NOCUM NOPERCENT NOFREQ; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.nocum is True
        assert t.nopercent is True
        assert t.nofreq is True

    def test_crosstab_display(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / NOROW NOCOL; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.norow is True
        assert t.nocol is True

    def test_out_dataset(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / OUT=myout OUTPCT OUTCUM; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.out_dataset == "myout"
        assert t.outpct is True
        assert t.outcum is True

    def test_alpha(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / CHISQ ALPHA=0.01; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.alpha == 0.01

    def test_scores(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / CMH SCORES=RIDIT; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.scores == "RIDIT"

    def test_missing_sparse(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / MISSING SPARSE; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.missing is True
        assert t.sparse is True

    def test_list_format(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / LIST; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.list_format is True

    def test_crosslist_format(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / CROSSLIST; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.crosslist_format is True

    def test_expected_cellchi2_deviation(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / EXPECTED CELLCHI2 DEVIATION TOTPCT; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.expected is True
        assert t.cellchi2 is True
        assert t.deviation is True
        assert t.totpct is True

    def test_risk_options(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / RELRISK RISKDIFF OR CL; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.relrisk is True
        assert t.riskdiff is True
        assert t.odds_ratio is True
        assert t.cl is True

    def test_agree(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / AGREE; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.agree is True

    def test_trend(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / TREND; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.trend is True

    def test_binomial(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a / BINOMIAL; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert t.binomial is True

    def test_exact_tests(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b / EXACT CHISQ FISHER; RUN;"
        t = parser.parse(sas)[0].tables[0]
        assert "CHISQ" in t.exact_tests
        assert "FISHER" in t.exact_tests


class TestParserStatements:
    """Test parsing of BY, WEIGHT, WHERE, FORMAT, OUTPUT."""

    def test_by_statement(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a; BY region; RUN;"
        b = parser.parse(sas)[0]
        assert b.by_vars == ["region"]

    def test_by_descending(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a; BY DESCENDING region site; RUN;"
        b = parser.parse(sas)[0]
        assert "region" in b.by_vars
        assert "site" in b.by_vars
        assert b.by_descending.get("region") is True

    def test_weight(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a*b; WEIGHT wt; RUN;"
        b = parser.parse(sas)[0]
        assert b.weight_var == "wt"

    def test_where(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a; WHERE (age GT 30 AND sex EQ 'M'); RUN;"
        b = parser.parse(sas)[0]
        assert "age" in b.where_clause
        assert "30" in b.where_clause

    def test_format(self, parser):
        sas = "PROC FREQ DATA=ds; TABLES a; FORMAT a $upcase. b best12.; RUN;"
        b = parser.parse(sas)[0]
        assert "a" in b.formats
        assert "b" in b.formats

    def test_order(self, parser):
        sas = "PROC FREQ DATA=ds ORDER=FREQ; TABLES a; RUN;"
        b = parser.parse(sas)[0]
        assert b.order == "FREQ"

    def test_nlevels(self, parser):
        sas = "PROC FREQ DATA=ds NLEVELS; TABLES a; RUN;"
        b = parser.parse(sas)[0]
        assert b.nlevels is True


class TestParserMultipleBlocks:
    def test_two_proc_freq(self, parser):
        sas = """
        PROC FREQ DATA=ds1; TABLES a; RUN;
        PROC FREQ DATA=ds2; TABLES b*c / CHISQ; RUN;
        """
        blocks = parser.parse(sas)
        assert len(blocks) == 2
        assert blocks[0].input_dataset == "ds1"
        assert blocks[1].input_dataset == "ds2"
        assert blocks[1].tables[0].chisq is True


# ====================================================================
# Generator / converter integration tests
# ====================================================================

class TestConverterOutput:
    """Test that generated code contains expected patterns."""

    def test_oneway_generates_group_by(self):
        code = convert("PROC FREQ DATA=ds; TABLES col1; RUN;")
        assert "group_by" in code
        assert "FREQUENCY" in code

    def test_twoway_generates_row_col_pct(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b; RUN;")
        assert "ROW_PCT" in code
        assert "COL_PCT" in code

    def test_chisq_generates_scipy(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / CHISQ; RUN;")
        assert "chi2_contingency" in code
        assert "pearson_chi2" in code
        assert "likelihood_ratio" in code
        assert "cramers_v" in code

    def test_fisher_generates_fisher_exact(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / FISHER; RUN;")
        assert "fisher_exact" in code

    def test_cmh_generates_cmh(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / CMH; RUN;")
        assert "cmh" in code.lower()

    def test_measures_generates_gamma(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / MEASURES; RUN;")
        assert "gamma" in code
        assert "concordant" in code
        assert "spearman" in code

    def test_out_dataset_generates_save(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / OUT=myfreq; RUN;")
        assert "save_as_table" in code
        assert "MYFREQ" in code

    def test_weight_generates_sum(self):
        code = convert("PROC FREQ DATA=ds; TABLES a; WEIGHT wt; RUN;")
        assert "F.sum" in code
        assert "WT" in code

    def test_where_generates_filter(self):
        code = convert("PROC FREQ DATA=ds; TABLES a; WHERE age GT 30; RUN;")
        assert "filter" in code
        assert "sql_expr" in code

    def test_by_generates_partition(self):
        code = convert("PROC FREQ DATA=ds; TABLES a; BY region; RUN;")
        assert "by_cols" in code

    def test_order_freq(self):
        code = convert("PROC FREQ DATA=ds ORDER=FREQ; TABLES a; RUN;")
        assert "desc()" in code

    def test_nocum_suppresses_cumulative(self):
        code = convert("PROC FREQ DATA=ds; TABLES a / NOCUM; RUN;")
        assert "CUM_FREQUENCY" not in code

    def test_norow_nocol_suppress(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / NOROW NOCOL; RUN;")
        assert "ROW_PCT" not in code
        assert "COL_PCT" not in code

    def test_expected_cellchi2(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / EXPECTED CELLCHI2 DEVIATION; RUN;")
        assert "EXPECTED" in code
        assert "CELLCHI2" in code
        assert "DEVIATION" in code

    def test_agree_kappa(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / AGREE; RUN;")
        assert "kappa" in code
        assert "mcnemar" in code

    def test_trend_test(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / TREND; RUN;")
        assert "trend" in code

    def test_relrisk(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / RELRISK; RUN;")
        assert "relative_risk" in code

    def test_riskdiff_with_cl(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / RISKDIFF CL ALPHA=0.05; RUN;")
        assert "risk_diff" in code
        assert "ci_lower" in code

    def test_odds_ratio(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / OR; RUN;")
        assert "odds_ratio" in code

    def test_binomial_oneway(self):
        code = convert("PROC FREQ DATA=ds; TABLES a / BINOMIAL; RUN;")
        assert "binom_test" in code

    def test_list_format(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / LIST; RUN;")
        assert "LIST format" in code

    def test_missing_option(self):
        code = convert("PROC FREQ DATA=ds; TABLES a * b / MISSING; RUN;")
        assert "MISSING" in code

    def test_library_prefix(self):
        code = convert("PROC FREQ DATA=mylib.mytable; TABLES col1; RUN;")
        assert "MYLIB" in code
        assert "MYTABLE" in code

    def test_parenthesised_expansion(self):
        code = convert("PROC FREQ DATA=ds; TABLES (a b) * c / CHISQ; RUN;")
        assert "expansion_combos" in code

    def test_complex_clinical(self):
        sas = """
        PROC FREQ DATA=work.adsl ORDER=FREQ;
          TABLES trt01p * sex / CHISQ FISHER RELRISK OR CL ALPHA=0.05
            NOROW NOCOL NOPERCENT OUT=work.sex_by_trt OUTPCT;
          BY siteid;
          WEIGHT randwt;
          WHERE saffl EQ 'Y' AND ittfl EQ 'Y';
        RUN;
        """
        code = convert(sas)
        assert "chi2_contingency" in code
        assert "fisher_exact" in code
        assert "relative_risk" in code
        assert "odds_ratio" in code
        assert "save_as_table" in code
        assert "F.sum" in code
        assert "filter" in code
        assert "by_cols" in code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
