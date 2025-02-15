#!/usr/bin/env python3

import argparse
import logging
import os
from pathlib import Path
from typing import Iterable, List, Optional

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
log = logging.getLogger()


class StatisticEntry:
    log = logging.getLogger()

    def __init__(self, id, state, sum, line_number, prev: Optional["StatisticEntry"]):
        self._id = id
        self._state = state
        self._sum = sum
        self._line_number = line_number
        self._prev = prev
        self._original = None

    @property
    def id(self):
        return self._id

    @property
    def state(self):
        return self._state

    @property
    def sum(self):
        return self._sum

    @property
    def line_number(self):
        return self._line_number

    @property
    def prev(self) -> Optional["StatisticEntry"]:
        return self._prev

    @property
    def backup(self) -> Optional["StatisticEntry"]:
        return self._original

    def has_previous(self) -> bool:
        return self.prev != None

    def has_backup(self) -> bool:
        return self._original != None

    def __str__(self):
        return f"[{self.state}|{self.sum}]"

    def make_backup(self):
        original_previous = self.prev.backup if self.prev.backup else self.prev
        if not self.has_backup():
            self._original = StatisticEntry(self.id, self.state, self.sum, self.line_number, original_previous)

    def fix_state(self):
        self.make_backup()
        self._state = self.prev.state

    def fix_sum(self):
        self.make_backup()
        state_change = self.state - self.prev.state
        if state_change >= 0:
            self._sum = self.prev.sum + state_change
            log.debug(f"FIXED {self._original}->{self}")

def lines(path: str):
    with open(path) as file:
        for line in file:
            yield line

def read_statistics(path: Path) -> Iterable[StatisticEntry]:
    previous = None

    for idx, line in enumerate(lines(path)):
        line_number = idx + 1

        values = line.split(separator)

        sum_idx = len(values) - 1
        state_idx = sum_idx - 1
        first_value = values[0]

        if not first_value.isnumeric():
            log.info(f"First value in line #{line_number} is not a number, treating as column headers...")
            continue

        id = int(first_value)
        state = float(values[state_idx])
        sum = float(values[sum_idx])

        current = StatisticEntry(id, state, sum, line_number, previous)
        previous = current

        yield current

def load_statistics(path: Path) -> List[StatisticEntry]:
    return list(read_statistics(path))

def fixed_entries(statistics: Iterable[StatisticEntry]) -> Iterable[StatisticEntry]:
    for entry in statistics:
        if entry.has_backup():
            yield entry

def fix_states(statistics: Iterable[StatisticEntry]):
    log.info(f"Fixing states...")
    line_number = 0
    for idx, entry in enumerate(statistics):
        line_number = idx + 1
        if entry.state == 0 and entry.has_previous() and entry.prev.state != 0:
            log.debug(f"#{line_number} State is {entry.state}, fixing with previous value")
            entry.fix_state()
    log.info(f"Fixing states processed {line_number} lines")

def fix_sums(statistics: Iterable[StatisticEntry]):
    log.info(f"Fixing sums...")
    line_number = 0
    for idx, entry in enumerate(statistics):
        line_number = idx + 1
        if not entry.has_previous():
            continue

        state_change = entry.state - entry.prev.state
        if state_change < 0:
            log.warning(f"#{line_number} {entry.prev}->{entry} ({state_change}) looks like a meter replacement, ignoring")
            continue

        expected_sum = entry.prev.sum + state_change
        sum_diff = entry.sum - entry.prev.sum

        if entry.sum != expected_sum: #and sum_diff == entry.state and sum_diff == entry.prev.state:
            log.debug(f"#{line_number} {entry.prev}->{entry}: sum {entry.sum} instead {expected_sum} ({state_change}), looks like state 0 corruption")
            entry.fix_sum()
    log.info(f"Fixing sums processed {line_number} lines")

def entries_with_fixed_state(statistics: Iterable[StatisticEntry]) -> Iterable[StatisticEntry]:
    for entry in statistics:
        if entry.has_backup() and entry.backup.state != entry.state:
            yield entry

def entries_with_fixed_sum(statistics: Iterable[StatisticEntry]) -> Iterable[StatisticEntry]:
    for entry in statistics:
        if entry.has_backup() and entry.backup.sum != entry.sum:
            yield entry

def generate_sql(statistics: Iterable[StatisticEntry], output_file: Path, tablename: str):
    output_file.unlink(missing_ok=True)

    ids = []
    with open(output_file, mode="w") as file:
        file.write(f"   UPDATE {tablename}\n")
        file.write(f"       SET state = CASE id\n")
        for entry in entries_with_fixed_state(statistics):
            file.write(f"                       WHEN {entry.id} THEN {entry.state}\n")
            ids.append(str(entry.id))
        file.write(f"                   END,\n")
        file.write(f"       SET sum = CASE id\n")
        for entry in entries_with_fixed_sum(statistics):
            file.write(f"                   WHEN {entry.id} THEN {entry.sum}\n")
            ids.append(str(entry.id))
        file.write(f"                 END\n")
        ids_list = str.join(",", ids)
        file.write(f"WHERE id IN ({ids_list})\n")
    log.info(f"SQL file written to {output_file}")

parser = argparse.ArgumentParser("Sumfix")
parser.add_argument(
    "-i",
    "--input", 
    required=True,
    help="Path of the csv input file, expected format: ID, stuff,more stuff,STATE,SUM",
    type=str
)
parser.add_argument(
    "-o",
    "--output", 
    required=True,
    help="Path of the SQL file to generate",
    type=str
)
parser.add_argument(
    "-t",
    "--table", 
    required=True,
    choices=["statistics", "statistics_short_term"],
    help="Name of the table the statistics are from",
    type=str
)
parser.add_argument(
    "-s",
    "--separator", 
    required=False,
    help="The separator to expect in the input",
    default=",",
    type=str
)
args = parser.parse_args()

table = args.table #statistics_short_term
input_file = args.input #f"statistics_short_term.csv"
separator = args.separator
output_file = args.output #f"statistics_short_term_fix.sql"

statistics = load_statistics(Path(input_file))
fix_states(statistics)
fix_sums(statistics)

fixes = list(fixed_entries(statistics))
log.info(f"Fixed {len(fixes)} entries")
generate_sql(fixes, Path(output_file), tablename=table)
