from mrq.utils import MovingETA


def test_movingeta():

    eta = MovingETA(2)

    assert eta.next(10, t=0) is None
    assert eta.next(5, t=1) == 1
    assert eta.next(4, t=2) == 4
    assert eta.next(2, t=10) == 8

    eta = MovingETA(3)
    assert eta.next(10, t=0) is None
    assert eta.next(9, t=1) == 9
    assert eta.next(8, t=2) == 8
    assert eta.next(7, t=3) == 7
    assert 3 < eta.next(5, t=4) < 4

    eta = MovingETA(2)
    assert eta.next(0, t=0) is None
    assert eta.next(0, t=1) is None
