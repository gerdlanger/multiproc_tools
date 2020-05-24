from multiprocessing import Process, active_children, SimpleQueue
import shared_tuple_list as stl
from time import time


def slave(id, data, result_queue, idx, steps):
    print('slave ', id, ' started: ', idx, '->', steps)
    data_in = stl.SharedTupleList.create_by_ref(data)
    for pos in range(idx, idx + steps):
        x = data_in[pos][0]
        y = data_in[pos][1]
        result_queue.put((pos, 1.1*x*y, id))
    result_queue.put(-1)
    print('slave ', id, ' cleanup')
    data_in.close()
    print('slave ', id, ' ended')


if __name__ == '__main__':
    num = 100000
    num_procs = 5
    procs = []

    prototype_data = (1, 1)
    #prototype_result = (1.0, 0)

    origin = stl.SharedTupleList(prototype_data, ['x', 'y'], initializer=(lambda idx:[(x, num-x)[idx] for x in range(num)]))
    #result = stl.SharedTupleList(prototype_result, size=num)

    result_array = [0.0 for _ in range(num)]

    results = SimpleQueue()

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
            steps = num - idx -1

        proc = Process(target=slave, name='slave-'+str(p), args=(p, origin, results, idx, steps))
        print('...', end='')
        proc.start()
        print('started')
        procs.append(proc)

        idx += steps

    print(active_children())

    t0 = time()
    count = 0
    running = num_procs
    while running > 0:
        value = results.get()
        if value == -1:
            running -= 1
        else:
            count += 1
            if count % 10000 == 0:
                print('got:', count, 'took:', time()-t0)
            #print('got:', value)
            result_array[value[0]] = value[1]

    for p in range(num_procs):
        procs[p].join()

    print('---> took:', time()-t0)

    for x in range(1950, 2050):
        print(result_array[x])

    origin.close()
    origin.unlink()







