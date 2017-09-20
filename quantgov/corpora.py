import csv
import logging
import re
import pandas as pd

import quantgov

from pathlib import Path

WORD_REGEX = re.compile(r'\b\w+\b')


def count_word_regex(doc):
    return doc.index, len(WORD_REGEX.findall(doc.text))


def count_words(driver):
    driver = quantgov.load_driver(driver)
    results = []
    for docindex, count in quantgov.utils.lazy_parallel(
            count_word_regex, driver.stream(), worker='thread'):
        temp = dict(zip(driver.index_labels, docindex))
        temp['count'] = count
        results.append(temp)
    df = pd.DataFrame(results)
    return df
