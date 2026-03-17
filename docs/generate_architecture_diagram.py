"""
Generate architecture diagram for SAS to Snowflake Converter.
Run: python docs/generate_architecture_diagram.py
"""

import os
from fpdf import FPDF


class ArchDiagram(FPDF):
    def __init__(self):
        super().__init__("L")  # Landscape
        self.set_auto_page_break(auto=False)

    def draw_box(self, x, y, w, h, label, sublabel=None, fill_color=(25, 60, 120), text_color=(255, 255, 255), corner=3):
        self.set_fill_color(*fill_color)
        self.set_draw_color(fill_color[0] - 10 if fill_color[0] > 10 else 0,
                           fill_color[1] - 10 if fill_color[1] > 10 else 0,
                           fill_color[2] - 10 if fill_color[2] > 10 else 0)
        self.set_line_width(0.4)
        self.rect(x, y, w, h, "FD", round_corners=True)
        self.set_text_color(*text_color)
        self.set_font("Helvetica", "B", 10)
        label_y = y + h / 2 - 3 if sublabel else y + h / 2 - 2
        self.set_xy(x, label_y)
        self.cell(w, 5, label, align="C")
        if sublabel:
            self.set_font("Helvetica", "", 7)
            self.set_text_color(text_color[0] - 40 if text_color[0] > 40 else text_color[0],
                               text_color[1] - 40 if text_color[1] > 40 else text_color[1],
                               text_color[2] - 40 if text_color[2] > 40 else text_color[2])
            self.set_xy(x, label_y + 5)
            self.cell(w, 4, sublabel, align="C")

    def draw_arrow(self, x1, y1, x2, y2, color=(100, 100, 100)):
        self.set_draw_color(*color)
        self.set_line_width(0.6)
        self.line(x1, y1, x2, y2)
        # Arrowhead
        import math
        angle = math.atan2(y2 - y1, x2 - x1)
        arrow_len = 3
        self.line(x2, y2, x2 - arrow_len * math.cos(angle - 0.4), y2 - arrow_len * math.sin(angle - 0.4))
        self.line(x2, y2, x2 - arrow_len * math.cos(angle + 0.4), y2 - arrow_len * math.sin(angle + 0.4))

    def draw_label_on_arrow(self, x, y, label):
        self.set_font("Helvetica", "I", 7)
        self.set_text_color(120, 120, 120)
        self.set_xy(x - 15, y - 4)
        self.cell(30, 4, label, align="C")

    def draw_section_box(self, x, y, w, h, label, fill_color=(245, 248, 255)):
        self.set_fill_color(*fill_color)
        self.set_draw_color(200, 210, 230)
        self.set_line_width(0.3)
        self.rect(x, y, w, h, "FD", round_corners=True)
        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 120, 160)
        self.set_xy(x + 3, y + 2)
        self.cell(w - 6, 4, label)


def main():
    pdf = ArchDiagram()

    # ── Page 1: Compiler Pipeline ───────────────────────────────
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 22)
    pdf.set_text_color(25, 60, 120)
    pdf.set_xy(0, 8)
    pdf.cell(297, 10, "SAS to Snowflake Converter - Architecture", align="C")

    # Subtitle
    pdf.set_font("Helvetica", "", 11)
    pdf.set_text_color(120, 120, 120)
    pdf.set_xy(0, 18)
    pdf.cell(297, 6, "Three-Stage Compiler Pipeline with Web Interfaces", align="C")

    # ── Pipeline Section ────────────────────────────────────────
    pdf.draw_section_box(15, 32, 267, 55, "COMPILER PIPELINE")

    # Input
    pdf.draw_box(22, 48, 40, 28, "SAS Code", "DATA step input", (70, 130, 70), (255, 255, 255))

    # Arrow
    pdf.draw_arrow(62, 62, 73, 62, (70, 130, 70))

    # Stage 1: Tokenizer
    pdf.draw_box(73, 42, 50, 38, "Tokenizer", "tokenizer.py", (44, 100, 180), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(200, 220, 255)
    pdf.set_xy(73, 62)
    pdf.cell(50, 3, "100+ token types", align="C")
    pdf.set_xy(73, 66)
    pdf.cell(50, 3, "&var, dates, comments", align="C")
    pdf.set_xy(73, 70)
    pdf.cell(50, 3, "[ ] for array access", align="C")

    pdf.draw_arrow(123, 62, 134, 62, (44, 100, 180))
    pdf.draw_label_on_arrow(128, 58, "tokens")

    # Stage 2: Parser
    pdf.draw_box(134, 42, 50, 38, "Parser", "parser.py", (44, 100, 180), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(200, 220, 255)
    pdf.set_xy(134, 62)
    pdf.cell(50, 3, "Recursive descent", align="C")
    pdf.set_xy(134, 66)
    pdf.cell(50, 3, "DataStep, IfThenElse", align="C")
    pdf.set_xy(134, 70)
    pdf.cell(50, 3, "DoLoop, ArrayDecl", align="C")

    pdf.draw_arrow(184, 62, 195, 62, (44, 100, 180))
    pdf.draw_label_on_arrow(189, 58, "AST")

    # Stage 3: Code Generator
    pdf.draw_box(195, 42, 50, 38, "Code Generator", "codegen.py", (44, 100, 180), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(200, 220, 255)
    pdf.set_xy(195, 62)
    pdf.cell(50, 3, "CASE WHEN, JOIN", align="C")
    pdf.set_xy(195, 66)
    pdf.cell(50, 3, "UNPIVOT, QUALIFY", align="C")
    pdf.set_xy(195, 70)
    pdf.cell(50, 3, "Window functions", align="C")

    pdf.draw_arrow(245, 62, 256, 62, (180, 100, 44))

    # Output
    pdf.draw_box(256, 48, 20, 28, "SQL", None, (180, 100, 44), (255, 255, 255))

    # Function Registry
    pdf.draw_box(195, 34, 50, 7, "functions.py", None, (80, 140, 200), (255, 255, 255))
    pdf.set_font("Helvetica", "", 5)

    # ── Web Interfaces Section ──────────────────────────────────
    pdf.draw_section_box(15, 95, 130, 100, "WEB INTERFACES")

    # FastAPI
    pdf.draw_box(22, 110, 55, 25, "FastAPI Backend", "api_server.py", (120, 60, 120), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(220, 200, 220)
    pdf.set_xy(22, 126)
    pdf.cell(55, 3, "POST /api/convert", align="C")
    pdf.set_xy(22, 130)
    pdf.cell(55, 3, "CORS, static serving", align="C")

    # React
    pdf.draw_box(85, 110, 55, 25, "React Frontend", "Vite + React 19", (50, 140, 140), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(200, 240, 240)
    pdf.set_xy(85, 126)
    pdf.cell(55, 3, "Split-pane editor", align="C")
    pdf.set_xy(85, 130)
    pdf.cell(55, 3, "localhost:5173", align="C")

    # Arrow React -> FastAPI
    pdf.draw_arrow(85, 122, 77, 122, (100, 100, 100))
    pdf.draw_label_on_arrow(81, 118, "/api")

    # Streamlit
    pdf.draw_box(22, 145, 55, 25, "Streamlit App", "streamlit_app.py", (200, 70, 70), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(255, 200, 200)
    pdf.set_xy(22, 161)
    pdf.cell(55, 3, "10 examples, macros", align="C")
    pdf.set_xy(22, 165)
    pdf.cell(55, 3, "Streamlit Cloud", align="C")

    # CLI
    pdf.draw_box(85, 145, 55, 25, "CLI", "__main__.py", (80, 80, 80), (255, 255, 255))
    pdf.set_font("Helvetica", "", 6)
    pdf.set_text_color(200, 200, 200)
    pdf.set_xy(85, 161)
    pdf.cell(55, 3, "python -m sas_to_snowflake", align="C")
    pdf.set_xy(85, 165)
    pdf.cell(55, 3, "stdin/file in, stdout/file out", align="C")

    # ── SAS Patterns Section ────────────────────────────────────
    pdf.draw_section_box(155, 95, 127, 100, "SAS PATTERNS SUPPORTED (15)")

    patterns = [
        ("KEEP / DROP / RENAME", "SELECT / EXCLUDE / RENAME"),
        ("IF / THEN / ELSE", "CASE WHEN"),
        ("MERGE with IN=", "INNER / LEFT / FULL JOIN"),
        ("%LET macro vars", "Inline substitution"),
        ("50+ SAS functions", "Snowflake equivalents"),
        ("RETAIN", "LAG() window function"),
        ("FIRST. / LAST.", "ROW_NUMBER() PARTITION BY"),
        ("Multiple SET", "UNION ALL"),
        ("SELECT / WHEN", "CASE WHEN"),
        ("Array + DO + OUTPUT", "UNPIVOT INCLUDE NULLS"),
        ("Subsetting IF computed", "QUALIFY clause"),
        ("WHERE clause", "WHERE with ne -> <>"),
    ]

    y_start = 107
    for i, (sas, sf) in enumerate(patterns):
        y = y_start + i * 7

        # SAS pattern
        pdf.set_font("Courier", "", 6.5)
        pdf.set_text_color(70, 130, 70)
        pdf.set_xy(160, y)
        pdf.cell(55, 6, sas)

        # Arrow
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(150, 150, 150)
        pdf.set_xy(215, y)
        pdf.cell(8, 6, "->")

        # Snowflake
        pdf.set_font("Courier", "", 6.5)
        pdf.set_text_color(44, 100, 180)
        pdf.set_xy(223, y)
        pdf.cell(55, 6, sf)

    # ── CI/CD Section ───────────────────────────────────────────
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(100, 120, 160)

    # CI box
    ci_y = 32
    pdf.draw_section_box(15, ci_y + 168, 267, 8, "")
    pdf.set_xy(18, ci_y + 168)
    pdf.set_font("Helvetica", "B", 7)
    pdf.set_text_color(100, 120, 160)
    pdf.cell(0, 8, "CI/CD:  GitHub Actions (pytest on push/PR)   |   Streamlit Cloud (auto-redeploy on push)   |   28 tests across 15 categories")

    # Save
    output_path = os.path.join(os.path.dirname(__file__), "Architecture_Diagram.pdf")
    pdf.output(output_path)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    main()
