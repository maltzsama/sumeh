#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def __convert_value(value):
    """
        Converts the provided value to the appropriate type (date, float, or int).

        Depending on the format of the input value, it will be converted to a datetime object,
        a floating-point number (float), or an integer (int).

        Args:
            value (str): The value to be converted, represented as a string.

        Returns:
            Union[datetime, float, int]: The converted value, which can be a datetime object, float, or int.

        Raises:
            ValueError: If the value does not match an expected format.
    """
    from datetime import datetime

    value = value.strip()
    try:
        if "-" in value:
            return datetime.strptime(value, "%Y-%m-%d")
        else:
            return datetime.strptime(value, "%d/%m/%Y")
    except ValueError:
        if "." in value:
            return float(value)
        return int(value)
