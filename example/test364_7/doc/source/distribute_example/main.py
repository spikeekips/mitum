import sys
from collections import defaultdict
from itertools import combinations


def sum_c(assigned, c):
    b = list()
    for i in c:
        b.append(len(assigned[i]))

    return b


def compare_assigned(a, b) -> int:
  if a is None:
    if b is None:
      return 0

    return 1

  if b is None:
    return -1

  if len(b) > len(a):
    return 1

  if len(b) < len(a):
    return -1

  if len(a) == 0:
    return 0

  for n, i in enumerate(a):
    if b[n] > i:
      return 1

    if b[n] < i:
      return -1

  return 0


def sort_assigned(assigned):
  a = list()

  for i in assigned:
    a.extend(i)

  return sorted(a)


def assign_states_to_worker(share, assigned, left):
  last = None
  last_len = 1
  if len(assigned) > 0:
    last = assigned[-1]
    last_len = len(last)

  l = set(left)

  for s in range(last_len, share+1):
    for c in combinations(l, s):
      if last is not None and compare_assigned(last, c) < 0:
        continue

      na = list(assigned)
      na.append(c)

      le = list(left)
      list(le.remove(x) for x in c)
      if len(le) < 1:
        yield tuple(na)

        continue

      for i in assign_states_to_worker(share, na, le):
        yield i


def assing_states_to_workers(states, share):
  total = len(states) * share
  all_states = sorted(states * share)

  for n, i in enumerate(assign_states_to_worker(share, list(), all_states)):
    c = sort_assigned(i)
    if len(c) != total:
      raise Exception('len(c) != total; %d != %d' % (len(c), total))

    if compare_assigned(all_states, c) != 0:
      raise Exception('c != all; %s != %s' % (c, all_states))

    yield i



def check_is_failed(failed, share):
    m = defaultdict(int)

    for c in failed:
        for i in c:
            m[i]+=1
            if m[i] >= share:
                return True

    return False


def check_assigned_failure(assigned, share, n_fail):
    checked = dict()

    for c in combinations(range(len(assigned)), n_fail):
        failed = tuple(sorted([assigned[i] for i in c]))
        if failed in checked:
            continue

        checked[failed] = None

        is_failed = check_is_failed(failed, share)
        if n_fail < share and is_failed:
            raise Exception('  exception: n_fail=%d failed=%s' % (n_fail, failed))

        sx = 'x' if is_failed else 'o'

        b = sum_c(assigned, c)

        print(sx, sum(b), b, "|", failed, "|", assigned)


def test_failed_workers(n_states, share, n_fail):
    states = sorted(list(range(n_states)))
    for f in assing_states_to_workers(states, share):
        check_assigned_failure(f, share, n_fail)


def usage(msg):
    print('%s: <number of states> <share> <fail>' % (sys.argv[0],))

    if msg != '':
        print(msg)

    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage('')

    n_states, share, n_fail = 0, 0, 0

    try:
        n_states = int(sys.argv[1], base=10)
    except ValueError as e :
        usage('<number of states>: %s' % e)

    try:
        share = int(sys.argv[2], base=10)
    except ValueError as e :
        usage('<share>: %s' % e)

    try:
        n_fail = int(sys.argv[3], base=10)
    except ValueError as e :
        usage('<fail>: %s' % e)

    if n_fail > n_states * share:
        usage('<fail> too big; <fail> <= <number of states> * <share>')

    print('> states=%d share=%d fail=%d' % (n_states, share, n_fail))
    test_failed_workers(n_states, share, n_fail)
