#!/usr/bin/env python3
# -*- coding: utf-8 -*-


def __convert_value(value):
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
