import logging
import re

from typing import List, Any


class Clause():
    _allowed_ops = ['contains', 'notin', 'eq', 'neq', 'lt', 'gt']
    _clause_pattern = re.compile(r"(\w+)\s+(" + r'|'.join(_allowed_ops) +
                                 r")\s+(.+)")

    def __init__(self, operation: str, field: str, value: Any):
        self.operation = operation
        self.field = field
        self.value = value

    def apply(self, actual_val: Any) -> bool:
        if self.operation == 'contains':
            return self.value in actual_val
        if self.operation == 'notin':
            return actual_val not in self.value
        if self.operation == 'eq':
            return actual_val == self.value
        if self.operation == 'neq':
            return actual_val != self.value
        if self.operation == 'lt':
            return actual_val < self.value
        if self.operation == 'gt':
            return actual_val > self.value

        logging.error('Clause.apply fell through.')
        return False

    def __eq__(self, other):
        if isinstance(other, Query):
            return self.__key() == other.__key()
        return NotImplemented

    def __key(self):
        return (self.operation, self.field, self.value)

    def __hash__(self):
        return hash(self.__key())

    def __str__(self):
        return f'{self.field} {self.operation} {self.value}'


class Query():
    def __init__(self, query_str):
        self.clauses: List[Clause] = []
        for clause_str in query_str.split(' and '):
            match = Clause._clause_pattern.match(clause_str)
            if match:
                field, op, val = match.groups()
                self.clauses.append(Clause(op, field, val))
            else:
                raise KeyError(f'Malformed clause: {clause_str}')


if __name__ == '__main__':
    q_str = 'make eq Fender'
    q1 = Query(q_str)

    assert len(q1.clauses) == 1
    q1_clause = q1.clauses[0]
    assert q1_clause.operation == 'eq' and q1_clause.value == 'Fender' and q1_clause.field == 'make'

    q_str_2 = 'model eq Stratocaster'
    q2 = Query(f'{q_str} and {q_str_2}')
    assert len(q2.clauses) == 2

    f = 'dummy_field'
    assert Clause('eq', f, 1).apply(1)
    assert Clause('neq', f, 1).apply(2)
    assert Clause('contains', f,
                  'roasted').apply('Comes with a beautiful roasted maple neck')
    assert not Clause(
        'contains', f,
        'flamed').apply('Comes with a beautiful roasted maple neck')
    assert Clause('notin', f, 'Fender, Gibson, Squier, Epiphone').apply('Suhr')
    assert not Clause('notin', f,
                      'Fender, Gibson, Squier, Epiphone').apply('Gibson')

    assert Clause('lt', f, 42).apply(1)
    assert Clause('gt', f, 42).apply(43)
