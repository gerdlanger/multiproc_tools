from multiprocessing import Process, active_children
import shared_tuple_list as stl
from time import time


def slave(id, data, result, idx, steps):
    print('slave ', id, ' started: ', idx, '->', steps)
    data_in = stl.SharedTupleList.create_by_ref(data)
    data_out = stl.SharedTupleList.create_by_ref(result)
    for pos in range(idx, idx + steps):
        x = data_in[pos][0]
        y = data_in[pos][1]
        data_out[pos] = (1.1*x*y, id)
    print('slave ', id, ' cleanup')
    data_in.close()
    data_out.close()
    print('slave ', id, ' ended')


if __name__ == '__main__':
    num = 100000
    num_procs = 5
    procs = []

    prototype_data = (1, 1)
    prototype_result = (1.0, 0)

    origin = stl.SharedTupleList(prototype_data, ['x', 'y'], initializer=(lambda idx:[(x, num-x)[idx] for x in range(num)]))
    result = stl.SharedTupleList(prototype_result, size=num)

    print('init values')
    for x in range(20):
        print(origin[x])
    #for x in range(num):
    #    origin[x] = (x, 1000-x)

    print('launching ...')
    idx = 0
    incr = num // num_procs
    for p in range(num_procs):
        print('...1', end='')
        if idx + incr < num:
            steps = incr
        else:
            steps = num - idx

        proc = Process(target=slave, name='slave-'+str(p), args=(p, origin, result, idx, steps))
        print('...', end='')
        procs.append(proc)

        idx += steps

    t0 = time()
    for p in range(num_procs):
        procs[p].start()
        print('started')

    print(active_children())

    for p in range(num_procs):
        procs[p].join()

    print('tool:', time()-t0)

    for x in range(1950, 2050):
        print(result[x])

    result.close()
    result.unlink()
    origin.close()
    origin.unlink()







