"""
Generate architecture diagram for SAS to Snowflake Converter.
Run: python docs/generate_architecture_diagram.py
"""

import os
import math
from fpdf import FPDF


class ArchDiagram(FPDF):
    def __init__(self):
        super().__init__("L")  # Landscape
        self.set_auto_page_break(auto=False)

    def rounded_rect(self, x, y, w, h, style="FD"):
        self.rect(x, y, w, h, style, round_corners=True)

    def box(self, x, y, w, h, title, details=None, fill=(25, 60, 120)):
        """Draw a clean box with title and optional detail lines."""
        self.set_fill_color(*fill)
        r, g, b = fill
        self.set_draw_color(max(r - 30, 0), max(g - 30, 0), max(b - 30, 0))
        self.set_line_width(0.5)
        self.rounded_rect(x, y, w, h, "FD")

        # Title - centered vertically if no details
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 11)
        if details:
            self.set_xy(x, y + 5)
        else:
            self.set_xy(x, y + h / 2 - 3)
        self.cell(w, 6, title, align="C")

        # Detail lines
        if details:
            self.set_font("Helvetica", "", 7.5)
            self.set_text_color(220, 230, 255)
            for i, line in enumerate(details):
                self.set_xy(x, y + 13 + i * 5)
                self.cell(w, 4, line, align="C")

    def arrow_right(self, x1, y, x2, label=None, color=(120, 140, 170)):
        """Horizontal arrow with optional label above."""
        self.set_draw_color(*color)
        self.set_line_width(0.7)
        self.line(x1, y, x2, y)
        # Arrowhead
        self.line(x2, y, x2 - 3.5, y - 2)
        self.line(x2, y, x2 - 3.5, y + 2)
        if label:
            self.set_font("Helvetica", "B", 7.5)
            self.set_text_color(*color)
            mid = (x1 + x2) / 2
            self.set_xy(mid - 10, y - 7)
            self.cell(20, 5, label, align="C")

    def arrow_left(self, x1, y, x2, label=None, color=(120, 140, 170)):
        """Horizontal arrow pointing left."""
        self.set_draw_color(*color)
        self.set_line_width(0.7)
        self.line(x1, y, x2, y)
        self.line(x2, y, x2 + 3.5, y - 2)
        self.line(x2, y, x2 + 3.5, y + 2)
        if label:
            self.set_font("Helvetica", "B", 7.5)
            self.set_text_color(*color)
            mid = (x1 + x2) / 2
            self.set_xy(mid - 10, y - 7)
            self.cell(20, 5, label, align="C")

    def section_label(self, x, y, w, label):
        """Section header label."""
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(80, 100, 140)
        self.set_xy(x, y)
        self.cell(w, 5, label)

    def section_bg(self, x, y, w, h):
        """Light background for a section."""
        self.set_fill_color(243, 246, 252)
        self.set_draw_color(210, 220, 240)
        self.set_line_width(0.3)
        self.rounded_rect(x, y, w, h, "FD")


def main():
    pdf = ArchDiagram()
    pdf.add_page()

    # ── Title ───────────────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 24)
    pdf.set_text_color(25, 60, 120)
    pdf.set_xy(0, 6)
    pdf.cell(297, 10, "SAS to Snowflake Converter", align="C")

    pdf.set_font("Helvetica", "", 12)
    pdf.set_text_color(130, 130, 130)
    pdf.set_xy(0, 17)
    pdf.cell(297, 6, "Architecture Overview", align="C")

    # ── COMPILER PIPELINE ───────────────────────────────────────
    pipe_y = 30
    pdf.section_bg(12, pipe_y, 273, 58)
    pdf.section_label(16, pipe_y + 2, 100, "COMPILER PIPELINE")

    bx_y = pipe_y + 13
    bx_h = 36

    # SAS Code input
    pdf.box(20, bx_y, 42, bx_h, "SAS Code", ["DATA step", "PROC FREQ"], fill=(60, 130, 60))

    # Arrow 1
    pdf.arrow_right(62, bx_y + bx_h / 2, 76, color=(60, 130, 60))

    # Tokenizer
    pdf.box(76, bx_y, 52, bx_h, "Tokenizer", ["tokenizer.py", "100+ token types", "macros, dates, arrays"], fill=(40, 95, 170))

    # Arrow 2
    pdf.arrow_right(128, bx_y + bx_h / 2, 142, "Tokens", color=(40, 95, 170))

    # Parser
    pdf.box(142, bx_y, 52, bx_h, "Parser", ["parser.py", "Recursive descent", "20+ AST node types"], fill=(40, 95, 170))

    # Arrow 3
    pdf.arrow_right(194, bx_y + bx_h / 2, 208, "AST", color=(40, 95, 170))

    # Code Generator
    pdf.box(208, bx_y, 52, bx_h, "Code Gen", ["codegen.py", "JOIN, CASE WHEN", "UNPIVOT, QUALIFY"], fill=(40, 95, 170))

    # Arrow 4
    pdf.arrow_right(260, bx_y + bx_h / 2, 266, color=(190, 110, 40))

    # SQL Output
    pdf.box(266, bx_y + 5, 18, 26, "SQL", None, fill=(190, 110, 40))

    # functions.py label above codegen
    pdf.set_fill_color(70, 140, 200)
    pdf.set_draw_color(50, 120, 180)
    pdf.set_line_width(0.3)
    pdf.rounded_rect(215, bx_y - 8, 38, 7, "FD")
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(255, 255, 255)
    pdf.set_xy(215, bx_y - 8)
    pdf.cell(38, 7, "functions.py", align="C")
    # Small connector line
    pdf.set_draw_color(70, 140, 200)
    pdf.set_line_width(0.4)
    pdf.line(234, bx_y - 1, 234, bx_y)

    # ── WEB INTERFACES ──────────────────────────────────────────
    intf_y = 95
    pdf.section_bg(12, intf_y, 133, 98)
    pdf.section_label(16, intf_y + 2, 100, "INTERFACES")

    row1_y = intf_y + 14
    row2_y = intf_y + 56

    # FastAPI
    pdf.box(20, row1_y, 58, 32, "FastAPI", ["api_server.py", "POST /api/convert", "Port 8000"], fill=(110, 50, 110))

    # React
    pdf.box(82, row1_y, 58, 32, "React Frontend", ["Vite + React 19", "Split-pane editor", "Port 5173"], fill=(40, 130, 130))

    # Arrow React -> FastAPI
    pdf.arrow_left(82, row1_y + 16, 78, "/api", color=(130, 100, 160))

    # Streamlit
    pdf.box(20, row2_y, 58, 32, "Streamlit", ["streamlit_app.py", "10 examples", "Cloud deploy"], fill=(190, 60, 60))

    # CLI
    pdf.box(82, row2_y, 58, 32, "CLI", ["__main__.py", "File or stdin input", "Macro var flags"], fill=(90, 90, 90))

    # ── SAS PATTERNS ────────────────────────────────────────────
    pat_y = 95
    pdf.section_bg(152, pat_y, 133, 98)
    pdf.section_label(156, pat_y + 2, 120, "SAS TO SNOWFLAKE PATTERNS")

    patterns = [
        ("KEEP / DROP / RENAME", "EXCLUDE / RENAME"),
        ("IF / THEN / ELSE", "CASE WHEN"),
        ("MERGE with IN=", "JOIN (inner/left/full)"),
        ("%LET macros", "Inline substitution"),
        ("50+ SAS functions", "Snowflake equivalents"),
        ("RETAIN", "LAG() window fn"),
        ("FIRST. / LAST.", "ROW_NUMBER() OVER"),
        ("Multiple SET", "UNION ALL"),
        ("SELECT / WHEN", "CASE WHEN"),
        ("Array + DO + OUTPUT", "UNPIVOT INCLUDE NULLS"),
        ("Subsetting IF", "QUALIFY clause"),
    ]

    table_y = pat_y + 12
    # Table header
    pdf.set_fill_color(40, 70, 120)
    pdf.rounded_rect(157, table_y, 123, 7, "F")
    pdf.set_font("Helvetica", "B", 7.5)
    pdf.set_text_color(255, 255, 255)
    pdf.set_xy(160, table_y + 1)
    pdf.cell(55, 5, "SAS Pattern")
    pdf.set_xy(225, table_y + 1)
    pdf.cell(50, 5, "Snowflake SQL")

    for i, (sas, sf) in enumerate(patterns):
        row_y = table_y + 8 + i * 7
        # Alternate row bg
        if i % 2 == 0:
            pdf.set_fill_color(235, 240, 250)
            pdf.rect(157, row_y, 123, 7, "F")

        # SAS column
        pdf.set_font("Courier", "B", 7)
        pdf.set_text_color(60, 120, 60)
        pdf.set_xy(160, row_y + 1)
        pdf.cell(55, 5, sas)

        # Arrow symbol
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(170, 170, 170)
        pdf.set_xy(218, row_y + 1)
        pdf.cell(8, 5, "->")

        # Snowflake column
        pdf.set_font("Courier", "B", 7)
        pdf.set_text_color(40, 90, 170)
        pdf.set_xy(228, row_y + 1)
        pdf.cell(50, 5, sf)

    # ── CI/CD FOOTER ────────────────────────────────────────────
    footer_y = 197
    pdf.set_fill_color(35, 50, 75)
    pdf.set_draw_color(25, 40, 65)
    pdf.set_line_width(0.3)
    pdf.rounded_rect(12, footer_y, 273, 9, "FD")
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(180, 200, 230)
    pdf.set_xy(12, footer_y)
    pdf.cell(273, 9, "CI/CD:   GitHub Actions (pytest on push/PR)     |     Streamlit Cloud (auto-redeploy)     |     28 tests, 15 categories", align="C")

    # ── Save ────────────────────────────────────────────────────
    output_path = os.path.join(os.path.dirname(__file__), "Architecture_Diagram.pdf")
    pdf.output(output_path)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    main()
