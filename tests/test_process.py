import os

from punctual.process import identity_matches, pid_alive, pid_start_time

DEAD_PID = 999_999_999


def test_pid_alive_for_self_and_a_dead_pid():
    assert pid_alive(os.getpid()) is True
    assert pid_alive(DEAD_PID) is False


def test_identity_matches_is_false_for_a_dead_pid():
    assert identity_matches(DEAD_PID, None) is False
    assert identity_matches(DEAD_PID, "whatever") is False


def test_identity_matches_self_with_and_without_a_token():
    me = os.getpid()
    assert identity_matches(me, None) is True
    token = pid_start_time(me)  # None off Linux — then the check degrades to liveness
    assert identity_matches(me, token) is True
    if token is not None:
        assert identity_matches(me, token + "0") is False
