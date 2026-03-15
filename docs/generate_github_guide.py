"""
Generate PDF guide for GitHub Integration & CI Setup.
Run: python docs/generate_github_guide.py
"""

import os
from fpdf import FPDF


class GitHubGuide(FPDF):
    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(0, 8, "GitHub Integration & CI/CD Setup Guide", align="C")
            self.ln(4)
            self.set_draw_color(200, 200, 200)
            self.line(10, self.get_y(), 200, self.get_y())
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}}", align="C")

    def cover_page(self):
        self.add_page()
        self.ln(45)
        self.set_font("Helvetica", "B", 30)
        self.set_text_color(25, 60, 120)
        self.cell(0, 14, "GitHub Integration", align="C")
        self.ln(13)
        self.cell(0, 14, "& CI/CD Setup Guide", align="C")
        self.ln(20)
        self.set_font("Helvetica", "", 15)
        self.set_text_color(80, 80, 80)
        self.cell(0, 10, "From Local Project to Deployed Application", align="C")
        self.ln(15)
        self.set_draw_color(25, 60, 120)
        self.set_line_width(0.8)
        self.line(60, self.get_y(), 150, self.get_y())
        self.ln(15)
        self.set_font("Helvetica", "", 11)
        self.set_text_color(100, 100, 100)
        self.multi_cell(0, 6,
            "A step-by-step guide covering:\n\n"
            "  - Installing and authenticating GitHub CLI\n"
            "  - Initializing a git repository\n"
            "  - Creating a GitHub repo and pushing code\n"
            "  - Setting up CI/CD with GitHub Actions\n"
            "  - Writing pytest test cases for CI\n"
            "  - Deploying with Streamlit Cloud",
            align="C")
        self.ln(25)
        self.set_font("Helvetica", "I", 10)
        self.set_text_color(150, 150, 150)
        self.cell(0, 8, "SAS to Snowflake Converter Project", align="C")

    def section_title(self, number, title):
        self.ln(4)
        self.set_fill_color(25, 60, 120)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 12)
        badge = f"  {number}  "
        badge_w = self.get_string_width(badge) + 4
        self.cell(badge_w, 8, badge, fill=True)
        self.set_text_color(25, 60, 120)
        self.set_font("Helvetica", "B", 14)
        self.cell(0, 8, f"  {title}")
        self.ln(10)
        self.set_draw_color(25, 60, 120)
        self.set_line_width(0.4)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(6)

    def step_title(self, step_num, title):
        if self.get_y() > 250:
            self.add_page()
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(25, 60, 120)
        self.cell(0, 7, f"Step {step_num}: {title}")
        self.ln(8)

    def description(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(50, 50, 50)
        self.multi_cell(0, 5, text)
        self.ln(3)

    def code_block(self, code, label=None):
        if label:
            self.set_font("Helvetica", "B", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 5, label)
            self.ln(4)

        lines = code.strip().split("\n")
        block_height = len(lines) * 4.5 + 6

        if self.get_y() + block_height > 270:
            self.add_page()

        y_start = self.get_y()
        self.set_fill_color(30, 35, 50)
        self.rect(12, y_start, 186, block_height, "F")

        self.set_font("Courier", "", 8)
        self.set_text_color(220, 220, 220)
        self.set_y(y_start + 3)

        for line in lines:
            self.set_x(16)
            if len(line) > 100:
                line = line[:97] + "..."
            self.cell(0, 4.5, line)
            self.ln(4.5)
        self.ln(5)

    def output_block(self, text):
        lines = text.strip().split("\n")
        block_height = len(lines) * 4.5 + 6

        if self.get_y() + block_height > 270:
            self.add_page()

        self.set_font("Helvetica", "B", 8)
        self.set_text_color(100, 100, 100)
        self.cell(0, 5, "EXPECTED OUTPUT:")
        self.ln(4)

        y_start = self.get_y()
        self.set_fill_color(240, 248, 240)
        self.rect(12, y_start, 186, block_height, "F")

        self.set_font("Courier", "", 7.5)
        self.set_text_color(30, 100, 30)
        self.set_y(y_start + 3)

        for line in lines:
            self.set_x(16)
            if len(line) > 105:
                line = line[:102] + "..."
            self.cell(0, 4.5, line)
            self.ln(4.5)
        self.ln(5)

    def note_block(self, text):
        if self.get_y() > 260:
            self.add_page()

        y_start = self.get_y()
        lines = text.strip().split("\n")
        block_height = len(lines) * 5 + 8

        self.set_fill_color(255, 248, 230)
        self.set_draw_color(230, 180, 50)
        self.rect(12, y_start, 186, block_height, "FD")

        self.set_font("Helvetica", "B", 8)
        self.set_text_color(180, 130, 0)
        self.set_y(y_start + 3)
        self.set_x(16)
        self.cell(0, 5, "NOTE:")
        self.ln(5)

        self.set_font("Helvetica", "", 9)
        self.set_text_color(100, 80, 20)
        for line in lines:
            self.set_x(16)
            self.cell(0, 5, line)
            self.ln(5)
        self.ln(5)

    def tip_block(self, text):
        if self.get_y() > 260:
            self.add_page()

        y_start = self.get_y()
        lines = text.strip().split("\n")
        block_height = len(lines) * 5 + 8

        self.set_fill_color(235, 245, 255)
        self.set_draw_color(70, 130, 200)
        self.rect(12, y_start, 186, block_height, "FD")

        self.set_font("Helvetica", "B", 8)
        self.set_text_color(40, 90, 160)
        self.set_y(y_start + 3)
        self.set_x(16)
        self.cell(0, 5, "TIP:")
        self.ln(5)

        self.set_font("Helvetica", "", 9)
        self.set_text_color(40, 80, 140)
        for line in lines:
            self.set_x(16)
            self.cell(0, 5, line)
            self.ln(5)
        self.ln(5)

    def toc_page(self, sections):
        self.add_page()
        self.set_font("Helvetica", "B", 20)
        self.set_text_color(25, 60, 120)
        self.cell(0, 12, "Table of Contents")
        self.ln(12)
        self.set_draw_color(25, 60, 120)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(8)

        for num, title, steps in sections:
            self.set_font("Helvetica", "B", 11)
            self.set_text_color(25, 60, 120)
            self.cell(12, 7, f"{num}.")
            self.set_text_color(40, 40, 40)
            self.set_font("Helvetica", "", 11)
            self.cell(140, 7, title)
            self.set_font("Helvetica", "I", 9)
            self.set_text_color(130, 130, 130)
            self.cell(0, 7, f"{steps} step(s)", align="R")
            self.ln(8)


def main():
    pdf = GitHubGuide()
    pdf.alias_nb_pages()

    # ── Cover ───────────────────────────────────────────────────
    pdf.cover_page()

    # ── TOC ─────────────────────────────────────────────────────
    pdf.toc_page([
        (1, "Prerequisites - Install GitHub CLI", 2),
        (2, "Authenticate with GitHub", 4),
        (3, "Initialize Git Repository", 3),
        (4, "Create .gitignore", 1),
        (5, "Stage and Commit Files", 3),
        (6, "Create GitHub Repository & Push", 2),
        (7, "Set Up CI/CD with GitHub Actions", 3),
        (8, "Write Pytest Test Cases", 3),
        (9, "Push CI Configuration", 2),
        (10, "Verify CI Pipeline", 3),
        (11, "Deploy with Streamlit Cloud", 4),
        (12, "Ongoing Workflow", 4),
    ])

    # ── Section 1: Prerequisites ────────────────────────────────
    pdf.add_page()
    pdf.section_title(1, "Prerequisites - Install GitHub CLI")

    pdf.step_title("1.1", "Install GitHub CLI (gh)")
    pdf.description("The GitHub CLI (gh) lets you create repos, manage PRs, and view CI runs from the terminal.")

    pdf.code_block("# macOS (Homebrew)\nbrew install gh\n\n# Ubuntu/Debian\nsudo apt install gh\n\n# Windows (Scoop)\nscoop install gh", "INSTALL COMMAND:")

    pdf.step_title("1.2", "Verify Installation")
    pdf.code_block("gh --version", "COMMAND:")
    pdf.output_block("gh version 2.88.1 (2026-03-10)")

    # ── Section 2: Authenticate ─────────────────────────────────
    pdf.add_page()
    pdf.section_title(2, "Authenticate with GitHub")

    pdf.step_title("2.1", "Start the Login Flow")
    pdf.code_block("gh auth login", "COMMAND:")

    pdf.step_title("2.2", "Select Options")
    pdf.description("You will be prompted with interactive choices. Select the following:")
    pdf.output_block("? What account do you want to log into?  GitHub.com\n? What is your preferred protocol for Git operations?  HTTPS\n? How would you like to authenticate GitHub CLI?  Login with a web browser")

    pdf.step_title("2.3", "Complete Browser Authentication")
    pdf.description(
        "The CLI will display a one-time code (e.g., XXXX-XXXX) and open your browser.\n\n"
        "1. Copy the one-time code shown in the terminal\n"
        "2. Press Enter to open github.com/login/device\n"
        "3. Paste the code in the browser\n"
        "4. Click Authorize"
    )

    pdf.step_title("2.4", "Verify Authentication")
    pdf.code_block("gh auth status", "COMMAND:")
    pdf.output_block("github.com\n  Logged in to github.com account your-username (keyring)\n  Active account: true\n  Git operations protocol: https\n  Token scopes: 'gist', 'read:org', 'repo', 'workflow'")

    pdf.note_block("The 'workflow' scope is required for GitHub Actions CI/CD.\nIf missing, re-run: gh auth login --scopes workflow")

    # ── Section 3: Initialize Git ───────────────────────────────
    pdf.add_page()
    pdf.section_title(3, "Initialize Git Repository")

    pdf.step_title("3.1", "Navigate to Your Project")
    pdf.code_block("cd /path/to/your/project", "COMMAND:")

    pdf.step_title("3.2", "Initialize a New Git Repository")
    pdf.code_block("git init", "COMMAND:")
    pdf.output_block("Initialized empty Git repository in /path/to/your/project/.git/")

    pdf.step_title("3.3", "Rename Default Branch to 'main'")
    pdf.description("Modern convention uses 'main' instead of 'master' as the default branch name.")
    pdf.code_block("git branch -m main", "COMMAND:")

    pdf.tip_block("If you already have a git repo at a parent directory (e.g., home),\ndelete it first or create a new one scoped to your project folder.")

    # ── Section 4: .gitignore ───────────────────────────────────
    pdf.add_page()
    pdf.section_title(4, "Create .gitignore")

    pdf.step_title("4.1", "Create a .gitignore File")
    pdf.description("Exclude files that should not be tracked: build artifacts, dependencies, secrets, and OS files.")

    pdf.code_block(
        "# Python\n"
        "__pycache__/\n"
        "*.pyc\n"
        ".env\n"
        "*.egg-info/\n"
        "dist/\n"
        "build/\n"
        "\n"
        "# Node.js (frontend)\n"
        "node_modules/\n"
        "frontend/dist/\n"
        "\n"
        "# IDE\n"
        ".idea/\n"
        ".vscode/\n"
        "*.swp\n"
        "\n"
        "# OS\n"
        ".DS_Store\n"
        "Thumbs.db",
        "FILE: .gitignore"
    )

    pdf.note_block("Never commit secrets (.env, credentials.json, API keys).\nNever commit large binary files or datasets.")

    # ── Section 5: Stage and Commit ─────────────────────────────
    pdf.add_page()
    pdf.section_title(5, "Stage and Commit Files")

    pdf.step_title("5.1", "Check Status - See What Files Exist")
    pdf.code_block("git status -s", "COMMAND:")
    pdf.output_block("?? .gitignore\n?? api_server.py\n?? requirements.txt\n?? sas_to_snowflake/\n?? test_converter.py\n?? frontend/\n?? streamlit_app.py")

    pdf.step_title("5.2", "Stage Specific Files")
    pdf.description("Always add specific files rather than 'git add .' to avoid accidentally staging secrets or large files.")
    pdf.code_block(
        "# Stage specific files\n"
        "git add .gitignore api_server.py requirements.txt \\\n"
        "       test_converter.py streamlit_app.py \\\n"
        "       sas_to_snowflake/ frontend/src/ frontend/index.html \\\n"
        "       frontend/vite.config.js frontend/package.json",
        "COMMAND:"
    )

    pdf.step_title("5.3", "Create Initial Commit")
    pdf.code_block(
        'git commit -m "Initial commit: SAS to Snowflake SQL converter\n'
        '\n'
        'Python compiler that converts SAS DATA steps to Snowflake SQL.\n'
        'Includes FastAPI backend and React/Vite frontend."',
        "COMMAND:"
    )
    pdf.output_block("[main (root-commit) 12d9557] Initial commit: SAS to Snowflake SQL converter\n 23 files changed, 6902 insertions(+)")

    pdf.tip_block("Write commit messages that explain WHY, not just WHAT.\nBad:  'updated files'\nGood: 'Add QUALIFY support for computed column filters'")

    # ── Section 6: Create GitHub Repo & Push ────────────────────
    pdf.add_page()
    pdf.section_title(6, "Create GitHub Repository & Push")

    pdf.step_title("6.1", "Create Repo and Push in One Command")
    pdf.description("The gh CLI can create the repo, set the remote, and push all at once.")
    pdf.code_block(
        "# Public repo\n"
        "gh repo create sas_2_snowflake --public --source=. --remote=origin --push\n"
        "\n"
        "# Private repo (alternative)\n"
        "gh repo create sas_2_snowflake --private --source=. --remote=origin --push",
        "COMMAND:"
    )
    pdf.output_block("https://github.com/your-username/sas_2_snowflake\nTo https://github.com/your-username/sas_2_snowflake.git\n * [new branch]  HEAD -> main\nbranch 'main' set up to track 'origin/main'.")

    pdf.step_title("6.2", "Verify on GitHub")
    pdf.description("Visit your repository URL in the browser to confirm all files are uploaded.")
    pdf.code_block("# Open in browser\ngh repo view --web", "COMMAND:")

    # ── Section 7: Set Up CI/CD ─────────────────────────────────
    pdf.add_page()
    pdf.section_title(7, "Set Up CI/CD with GitHub Actions")

    pdf.step_title("7.1", "Create the Workflow Directory")
    pdf.code_block("mkdir -p .github/workflows", "COMMAND:")

    pdf.step_title("7.2", "Create the CI Workflow File")
    pdf.description("This YAML file tells GitHub Actions what to do on every push and pull request.")
    pdf.code_block(
        "name: CI\n"
        "\n"
        "on:\n"
        "  push:\n"
        "    branches: [main]\n"
        "  pull_request:\n"
        "    branches: [main]\n"
        "\n"
        "jobs:\n"
        "  test:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "\n"
        "      - uses: actions/setup-python@v5\n"
        "        with:\n"
        "          python-version: \"3.11\"\n"
        "\n"
        "      - name: Install dependencies\n"
        "        run: pip install pytest\n"
        "\n"
        "      - name: Run tests\n"
        "        run: pytest test_converter.py -v",
        "FILE: .github/workflows/ci.yml"
    )

    pdf.step_title("7.3", "Understanding the Workflow")
    pdf.description(
        "Triggers:\n"
        "  - 'push' to main: Runs tests when code is pushed directly\n"
        "  - 'pull_request' to main: Runs tests when a PR is opened\n\n"
        "Job steps:\n"
        "  1. Checkout: Downloads your code\n"
        "  2. Setup Python: Installs Python 3.11\n"
        "  3. Install dependencies: Installs pytest\n"
        "  4. Run tests: Executes all test cases with verbose output"
    )

    pdf.note_block("CI runs on PUSH (when you push code) and PULL REQUESTS (when\nsomeone proposes changes). It does NOT run when you pull/fetch code.")

    # ── Section 8: Write Pytest Tests ───────────────────────────
    pdf.add_page()
    pdf.section_title(8, "Write Pytest Test Cases")

    pdf.step_title("8.1", "Test Structure")
    pdf.description("Organize tests into classes by feature category. Each test method should assert specific patterns in the output.")
    pdf.code_block(
        "import pytest\n"
        "from sas_to_snowflake import SASToSnowflakeConverter\n"
        "\n"
        "def run_convert(sas_code, macro_vars=None):\n"
        "    converter = SASToSnowflakeConverter(macro_vars=macro_vars)\n"
        "    result = converter.convert(sas_code)\n"
        "    return result.sql\n"
        "\n"
        "class TestMergeJoins:\n"
        "\n"
        "    def test_inner_join(self):\n"
        "        sql = run_convert('''\n"
        "        data work.matched;\n"
        "            merge work.customers (in=a) work.orders (in=b);\n"
        "            by customer_id;\n"
        "            if a and b;\n"
        "        run;\n"
        "        ''')\n"
        "        assert 'INNER JOIN' in sql\n"
        "        assert 'ON a.customer_id = b.customer_id' in sql",
        "FILE: test_converter.py (example)"
    )

    pdf.step_title("8.2", "Assertion Best Practices")
    pdf.description(
        "Good assertions check for specific SQL patterns:\n\n"
        '  assert "INNER JOIN" in sql                    # Join type\n'
        '  assert "UNPIVOT INCLUDE NULLS" in sql         # Specific syntax\n'
        '  assert "QUALIFY" in sql                       # Clause used\n'
        '  assert "WHERE" not in sql                     # Clause NOT used\n'
        '  assert "PARTITION BY _row_id_" in sql         # Window function scope\n\n'
        "Add messages to assertions for clear CI failure output:\n\n"
        '  assert "QUALIFY" in sql, "Should use QUALIFY, not WHERE"'
    )

    pdf.step_title("8.3", "Run Tests Locally Before Pushing")
    pdf.code_block("pytest test_converter.py -v", "COMMAND:")
    pdf.output_block(
        "test_converter.py::TestMergeJoins::test_inner_join PASSED        [ 28%]\n"
        "test_converter.py::TestMergeJoins::test_left_join PASSED         [ 32%]\n"
        "test_converter.py::TestQualify::test_subsetting_if PASSED        [ 96%]\n"
        "...\n"
        "============================== 28 passed in 0.05s ===================="
    )

    # ── Section 9: Push CI Config ───────────────────────────────
    pdf.add_page()
    pdf.section_title(9, "Push CI Configuration")

    pdf.step_title("9.1", "Stage and Commit CI Files")
    pdf.code_block(
        "git add .github/workflows/ci.yml test_converter.py\n"
        'git commit -m "Add CI workflow and pytest test cases"',
        "COMMANDS:"
    )

    pdf.step_title("9.2", "Push to GitHub")
    pdf.code_block("git push", "COMMAND:")
    pdf.output_block("To https://github.com/your-username/sas_2_snowflake.git\n   12d9557..37bd459  main -> main")

    pdf.description("As soon as you push, GitHub Actions will automatically trigger the CI workflow.")

    # ── Section 10: Verify CI ───────────────────────────────────
    pdf.add_page()
    pdf.section_title(10, "Verify CI Pipeline")

    pdf.step_title("10.1", "Check CI Status from Terminal")
    pdf.code_block("gh run list --limit 3", "COMMAND:")
    pdf.output_block("completed  success  Add CI workflow  CI  main  push  23117230846  13s")

    pdf.step_title("10.2", "View Test Output from Terminal")
    pdf.code_block('gh run view <RUN_ID> --log | grep -E "(PASSED|FAILED|passed|failed)"', "COMMAND:")
    pdf.output_block(
        "test_converter.py::TestBasicSetOperations::test_simple_set_with_keep PASSED [  3%]\n"
        "test_converter.py::TestBasicSetOperations::test_set_with_drop PASSED       [  7%]\n"
        "test_converter.py::TestIfThenElse::test_simple_if_then_else PASSED         [ 17%]\n"
        "...\n"
        "============================== 28 passed in 0.13s ========================="
    )

    pdf.step_title("10.3", "View in Browser")
    pdf.description(
        "You can also view CI results in the GitHub web interface:\n\n"
        "1. Go to your repository on GitHub\n"
        "2. Click the 'Actions' tab\n"
        "3. Click the latest workflow run\n"
        "4. Click the 'test' job\n"
        "5. Expand the 'Run tests' step to see all PASSED/FAILED results"
    )

    pdf.tip_block("Add a CI badge to your README to show build status:\n\n![CI](https://github.com/USER/REPO/actions/workflows/ci.yml/badge.svg)")

    # ── Section 11: Deploy with Streamlit ───────────────────────
    pdf.add_page()
    pdf.section_title(11, "Deploy with Streamlit Cloud (Free)")

    pdf.step_title("11.1", "Add Streamlit App to Your Project")
    pdf.description("Create a streamlit_app.py in your project root. Streamlit Cloud looks for this file by default.")
    pdf.code_block(
        "import streamlit as st\n"
        "from sas_to_snowflake import SASToSnowflakeConverter\n"
        "\n"
        "st.set_page_config(page_title='SAS to Snowflake', layout='wide')\n"
        "st.title('SAS to Snowflake SQL Converter')\n"
        "\n"
        "col1, col2 = st.columns(2)\n"
        "with col1:\n"
        "    sas_code = st.text_area('SAS Code', height=400)\n"
        "with col2:\n"
        "    if st.button('Convert') and sas_code:\n"
        "        converter = SASToSnowflakeConverter()\n"
        "        result = converter.convert(sas_code)\n"
        "        st.code(result.sql, language='sql')",
        "FILE: streamlit_app.py (simplified)"
    )

    pdf.step_title("11.2", "Commit and Push")
    pdf.code_block(
        "git add streamlit_app.py .streamlit/config.toml\n"
        'git commit -m "Add Streamlit app for online deployment"\n'
        "git push",
        "COMMANDS:"
    )

    pdf.step_title("11.3", "Deploy on Streamlit Cloud")
    pdf.description(
        "1. Go to share.streamlit.io\n"
        "2. Sign in with your GitHub account\n"
        "3. Click 'Create app' (or 'New app')\n"
        "4. Select your settings:\n"
        "     Repository:  your-username/sas_2_snowflake\n"
        "     Branch:      main\n"
        "     Main file:   streamlit_app.py\n"
        "5. Click 'Deploy'\n"
        "6. Wait 1-2 minutes for the build to complete"
    )

    pdf.step_title("11.4", "Auto-Redeploy")
    pdf.description("Every time you push changes to the main branch, Streamlit Cloud will automatically redeploy your app with the latest code. No manual action required.")

    pdf.tip_block("Your app URL will be something like:\nhttps://your-username-sas-2-snowflake.streamlit.app\n\nShare this URL with anyone - no install needed!")

    # ── Section 12: Ongoing Workflow ────────────────────────────
    pdf.add_page()
    pdf.section_title(12, "Ongoing Workflow")

    pdf.step_title("12.1", "Making Changes")
    pdf.description("After the initial setup, your daily workflow is simple:")
    pdf.code_block(
        "# 1. Make code changes\n"
        "# 2. Run tests locally\n"
        "pytest test_converter.py -v\n"
        "\n"
        "# 3. Stage changed files\n"
        "git add <changed-files>\n"
        "\n"
        "# 4. Commit with descriptive message\n"
        'git commit -m "Add support for PROC SQL conversion"\n'
        "\n"
        "# 5. Push - CI runs automatically\n"
        "git push",
        "WORKFLOW:"
    )

    pdf.step_title("12.2", "Working with Branches and Pull Requests")
    pdf.description("For larger features, use feature branches and pull requests:")
    pdf.code_block(
        "# Create a feature branch\n"
        "git checkout -b feature/proc-sql-support\n"
        "\n"
        "# Make changes, commit, push\n"
        "git add .\n"
        'git commit -m "Add PROC SQL parser and codegen"\n'
        "git push -u origin feature/proc-sql-support\n"
        "\n"
        "# Create a pull request\n"
        'gh pr create --title "Add PROC SQL support" --body "Converts PROC SQL to Snowflake SQL"\n'
        "\n"
        "# CI runs on the PR - merge when all tests pass\n"
        "gh pr merge --squash",
        "WORKFLOW:"
    )

    pdf.step_title("12.3", "Adding New Test Cases")
    pdf.description("When you add a new SAS pattern, always add a matching test:")
    pdf.code_block(
        "class TestNewFeature:\n"
        "\n"
        "    def test_new_pattern(self):\n"
        "        sql = run_convert('''\n"
        "        data work.output;\n"
        "            set work.input;\n"
        "            /* new SAS pattern here */\n"
        "        run;\n"
        "        ''')\n"
        "        assert 'EXPECTED_SQL_PATTERN' in sql, 'Description of what should happen'",
        "TEMPLATE:"
    )

    pdf.step_title("12.4", "Regenerating Documentation")
    pdf.description("After adding features, regenerate the PDF documentation:")
    pdf.code_block(
        "# Regenerate feature guide\n"
        "python docs/generate_docs.py\n"
        "\n"
        "# Commit and push\n"
        "git add docs/\n"
        'git commit -m "Update feature guide with new patterns"\n'
        "git push",
        "COMMANDS:"
    )

    # ── Summary ─────────────────────────────────────────────────
    pdf.add_page()
    pdf.ln(10)
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(25, 60, 120)
    pdf.cell(0, 12, "Quick Reference", align="C")
    pdf.ln(15)

    commands = [
        ("Initialize repo", "git init && git branch -m main"),
        ("Check status", "git status -s"),
        ("Stage files", "git add <files>"),
        ("Commit", 'git commit -m "message"'),
        ("Create repo + push", "gh repo create NAME --public --source=. --remote=origin --push"),
        ("Push changes", "git push"),
        ("Check CI status", "gh run list --limit 1"),
        ("View CI logs", "gh run view <ID> --log"),
        ("Run tests locally", "pytest test_converter.py -v"),
        ("Create branch", "git checkout -b feature/name"),
        ("Create PR", 'gh pr create --title "Title" --body "Description"'),
        ("Run Streamlit", "streamlit run streamlit_app.py"),
    ]

    for desc, cmd in commands:
        if pdf.get_y() > 265:
            pdf.add_page()
        y_start = pdf.get_y()

        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(50, 50, 50)
        pdf.cell(55, 7, desc)

        pdf.set_fill_color(30, 35, 50)
        pdf.set_font("Courier", "", 8)
        pdf.set_text_color(220, 220, 220)
        cmd_w = pdf.get_string_width(cmd) + 8
        pdf.cell(min(cmd_w, 135), 7, f"  {cmd}  ", fill=True)
        pdf.ln(9)

    # ── Save ────────────────────────────────────────────────────
    output_path = os.path.join(os.path.dirname(__file__), "GitHub_Integration_CI_Guide.pdf")
    pdf.output(output_path)
    print(f"PDF generated: {output_path}")


if __name__ == "__main__":
    main()
