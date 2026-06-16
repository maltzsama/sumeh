"""
CSV rule loader.

Load validation rules from CSV files.
"""

import csv
from pathlib import Path
from typing import List, Union

from sumeh.core.rules.rule_definition import RuleDefinition


def load_rules_csv(filepath: Union[str, Path]) -> List[RuleDefinition]:
    """
    Load validation rules from CSV file.

    CSV Format:
        field,check_type,value,threshold,level,category
        email,is_complete,,1.0,ROW,completeness
        age,is_positive,,,ROW,comparison
        age,is_between,"[0,120]",,ROW,comparison

    Args:
        filepath: Path to CSV file

    Returns:
        List of RuleDef objects

    Example:
        >>> rules = load_rules_csv("rules.csv")
        >>> report = pandas.validate(df, rules)
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"Rules file not found: {filepath}")

    rules = []

    with open(filepath, "r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Parse value (handle JSON strings)
            value = row.get("value", "")
            if value:
                # Try to parse as JSON (lists, dicts)
                import json

                try:
                    value = json.loads(value)
                except:
                    # Keep as string
                    pass
            else:
                value = None

            # Parse threshold
            threshold = row.get("threshold", "1.0")
            try:
                threshold = float(threshold) if threshold else 1.0
            except:
                threshold = 1.0

            # Create RuleDef
            rule = RuleDefinition(
                field=row["field"],
                check_type=row["check_type"],
                value=value,
                threshold=threshold,
                level=row.get("level", "ROW"),
                category=row.get("category", "unknown"),
            )

            rules.append(rule)

    return rules


def save_rules_csv(rules: List[RuleDefinition], filepath: Union[str, Path]):
    """
    Save validation rules to CSV file.

    Args:
        rules: List of RuleDef objects
        filepath: Output path
    """
    filepath = Path(filepath)

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "field",
                "check_type",
                "value",
                "threshold",
                "level",
                "category",
            ],
        )

        writer.writeheader()

        for rule in rules:
            # Serialize value
            import json

            value = (
                json.dumps(rule.value)
                if isinstance(rule.value, (list, dict))
                else str(rule.value or "")
            )

            writer.writerow(
                {
                    "field": rule.field,
                    "check_type": rule.check_type,
                    "value": value,
                    "threshold": rule.threshold,
                    "level": rule.level,
                    "category": rule.category or "unknown",
                }
            )
