from __future__ import print_function, division, absolute_import

import pytest

from wukong.counter import Counter
from wukong.utils_test import loop  # noqa F401

try:
    from wukong.counter import Digest
except ImportError:
    Digest = None


@pytest.mark.parametrize(
    "CD,size",
    [
        (Counter, lambda d: sum(d.values())),
        pytest.param(
            Digest,
            lambda x: x.size(),
            marks=pytest.mark.skipif(not Digest, reason="no crick library"),
        ),
    ],
)
def test_digest(loop, CD, size):
    c = CD(loop=loop)
    c.add(1)
    c.add(2)
    assert size(c.components[0]) == 2

    c.shift()
    assert 0 < size(c.components[0]) < 2
    assert 0 < size(c.components[1]) < 1
    assert sum(size(d) for d in c.components) == 2

    for i in range(len(c.components) - 1):
        assert size(c.components[i]) >= size(c.components[i + 1])

    c.add(3)

    assert sum(size(d) for d in c.components) == c.size()


def test_counter(loop):
    c = Counter(loop=loop)
    c.add(1)

    for i in range(5):
        c.shift()
        assert abs(sum(cc[1] for cc in c.components) - 1) < 1e-13
