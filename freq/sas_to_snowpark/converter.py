"""
High-level converter API
========================
Convenience wrapper that chains the parser and generator together.
"""

from __future__ import annotations

from typing import List, Tuple

from .parser import ProcFreqParser, ProcFreqBlock
from .generator import SnowparkGenerator


class ProcFreqConverter:
    """
    End-to-end converter: SAS PROC FREQ source → Snowpark Python source.

    Usage::

        converter = ProcFreqConverter()
        python_code = converter.convert(sas_code)
        print(python_code)
    """

    def __init__(self):
        self.parser = ProcFreqParser()
        self.generator = SnowparkGenerator()

    def convert(self, sas_code: str, func_prefix: str = "proc_freq") -> str:
        """
        Parse all PROC FREQ blocks in *sas_code* and return a single Python
        source string containing Snowpark equivalents.

        Parameters
        ----------
        sas_code : str
            Raw SAS source that contains one or more ``PROC FREQ … RUN;`` blocks.
        func_prefix : str
            Base name for the generated Python functions.

        Returns
        -------
        str
            Complete, runnable Python source code.
        """
        blocks = self.parser.parse(sas_code)
        if not blocks:
            return "# No PROC FREQ blocks found in the input SAS code.\n"

        parts: List[str] = []
        for idx, block in enumerate(blocks):
            suffix = f"_{idx + 1}" if len(blocks) > 1 else ""
            name = f"{func_prefix}{suffix}"
            parts.append(self.generator.generate(block, func_name=name))
            parts.append("")  # blank separator

        return "\n".join(parts)

    def convert_to_blocks(self, sas_code: str) -> List[Tuple[ProcFreqBlock, str]]:
        """
        Return a list of ``(parsed_block, generated_code)`` tuples — useful
        when callers need both the IR and the output.
        """
        blocks = self.parser.parse(sas_code)
        results = []
        for idx, block in enumerate(blocks):
            suffix = f"_{idx + 1}" if len(blocks) > 1 else ""
            name = f"proc_freq{suffix}"
            code = self.generator.generate(block, func_name=name)
            results.append((block, code))
        return results


# Module-level convenience function
def convert(sas_code: str, func_prefix: str = "proc_freq") -> str:
    """Shorthand: parse + generate in one call."""
    return ProcFreqConverter().convert(sas_code, func_prefix=func_prefix)
