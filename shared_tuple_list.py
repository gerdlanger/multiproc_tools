"""
SharedTupleList (maybe better named as ShareableTupleList) uses ShareableList elements to emulate an array of tuple
that can be shared across multiple processes via SharedMemory.
Note: requires Python >= 3.8 due to usage of multiprocessing.shared_memory
(c) 2020 Gerd Langer
"""
from multiprocessing.shared_memory import ShareableList


class SharedTupleList:
    """
    SharedTupleList uses a list of ShareableList to emulate a list of tuples.
    The fields tuple passed is used as prototype and default value.
    Each field of the tuple will end up in a separate ShareableList with its
    own SharedMemory.
    Optionally, each tuple field might be named passed field_names.
    """

    # Constructor and convenience factory method

    def __init__(self, fields, field_names=None, size=None, shm_names=None):
        """
        Create a ShareTupleList newly or as reference to an existing one.
        Optionally, each tuple field might be named passed field_names.
        Either pass the size to create the ShareTupleList in a new SharedMemory,
        or pass the list of names of existing SharedMemory using the same order
        as the elements of the fields parameter.
        :param fields: initializer tuple
        :param field_names: optionally to get, set fields of tuple by name
        :param size: number of tuples in the list (storage capacity), used to create a list
        :param shm_names: pass the names of the existing list (SharedMemory)
        """
        assert (type(fields) == tuple), "passed fields must be a tuple"
        assert (size is not None or shm_names is not None), "either pass size or shm_names"

        self._create_shm = shm_names is None
        self._size = size
        self._prototype = fields
        self._tuple_len = len(fields)

        self._fields_type = [type(f) for f in fields]
        self._fields_smm_name = []
        self._fields_list = []
        self._fields_name = field_names
        self._fields_names_map = {n: i for (i, n) in enumerate(field_names)}

        for i, f in enumerate(fields):
            if self._create_shm:
                shl = ShareableList([f for _ in range(size)])
            else:
                shl = ShareableList(name=shm_names[i])
                if self._size is None:
                    self._size = len(shl)
            self._fields_list.append(shl)
            self._fields_smm_name.append(shl.shm.name)

    @staticmethod
    def create_by_ref(stl):
        """
        create_by_ref generates a new ShareTupleList based on the passed stl.
        The newly created list uses the SharedMemory of the passed stl.
        :param stl: the stl used to make a copy of it using the same shared memory
        :return: a new SharedTupleList
        """
        return SharedTupleList(stl.get_prototype(), stl.get_field_names(), shm_names=stl.get_shm_names())

    # internal methods for field name to index handling

    def _item2idx(self, item):
        """_item2idx return the item itself, if it is an int value or the index of the passed field_name"""
        if type(item) == int:
            return item
        else:
            return self._fields_names_map.get(item)

    def _get_list(self, item):
        """_get_list returns the ShareableList of the given index or field_name"""
        return self._fields_list[self._item2idx(item)]

    # shared memory name retrieval

    def get_shm_name(self, item):
        """get_shm_name returns the name of the underlying shared memory for the index or field_name"""
        return self._fields_smm_name[self._item2idx(item)]

    def get_shm_names(self):
        """get_shm_names returns all names of the underlying shared memories in order of the tuple fields"""
        return self._fields_smm_name.copy()

    # meta info retrieval

    def get_prototype(self):
        """get_prototype returns the original tuple used to define and initiate the SharedTupleList"""
        return self._prototype

    def get_field_name(self, item):
        """get_field_name returns the name of the tuple's field at the given index (or name ... itself :-))"""
        return self._fields_name[self._item2idx(item)]

    def get_field_names(self):
        """get_field_names returns all names of tuple's fields in order of the tuple fields"""
        return self._fields_name.copy()

    def get_field_type(self, item):
        """get_field_type returns the type of the tuple's field at the given index or name"""
        return self._fields_type[self._item2idx(item)]

    def get_field_types(self):
        """get_field_type returns all types of the tuple's fields in order of the tuple fields"""
        return self._fields_type.copy()

    def get_tuple_len(self):
        """get_tuple_len returns the number of fields of the original tuple (the prototype)"""
        return self._tuple_len

    def get_width(self):
        """get_width it the same as get_tuple_len"""
        return self.get_tuple_len()

    def __len__(self):
        """__len__ is the storage capacity in number of tuples in this SharedTupleList"""
        return self._size

    # index-based getting and setting of whole tuples

    def __getitem__(self, idx):
        """__getitem__ allows to get the tuple at the given index.
        Note: as the result is a tuple, the returned tuple cannot be used to change values in the list."""
        # reconstruct the tuple from the different internal ShareableLists
        return tuple(self._fields_list[i][idx] for i in range(self._tuple_len))

    def __setitem__(self, idx, value):
        """__setitem__ sets the values of the tuple at the given index"""
        # distribute the tuple across the different internal ShareableLists
        for i in range(self._tuple_len):
            self._fields_list[i][idx] = value[i]

    # index-based getting and setting of tuple fields

    def get(self, idx, item):
        """
        get returns the value of the passed field (index or name) of the tuple at the given idx
        :param idx: the tuple's index
        :param item: the field index inside the tuple or field name
        :return: the field of the addressed tuple by index or field name
        """
        return self._get_list(item)[idx]

    def set(self, idx, item, value) -> None:
        """
        set sets the value of the passed field (index or name) of the tuple at the given idx to the passed value
        :param idx: the tuple's index
        :param item: the field index inside the tuple or field name
        :param value: the value
        """
        self._get_list(item)[idx] = value

    # shared memory clean ups#

    def close(self):
        """close the attached shared memory of all referenced ShareableLists"""
        for i in range(self._tuple_len):
            self._get_list(i).shm.close()

    def unlink(self):
        """unlink of attached shared memory of all referenced ShareableLists,
        if shared memory was created by this instance"""
        if self._create_shm:
            for i in range(self._tuple_len):
                self._get_list(i).shm.unlink()


if __name__ == '__main__':
    """some dummy, test and example code"""

    tpl =   (1,  2,  3,  'name', 1.0)
    names = ['a','b','c','txt',  'f']
    l = SharedTupleList(tpl, names, size=100)
    cp = SharedTupleList(tpl, names, shm_names=l.get_shm_names())
    cp2 = SharedTupleList.create_by_ref(l)

    print('len(l)=', len(l))
    print('len(cp)=', len(cp))

    print(l[0])
    for e in l[0]:
        print(e, end=' | ')
    print()

    for n in names:
        print(n, '=', l.get(0, n), end=';')
    print()
    for n in names:
        print(n, '=', cp.get(0, n), end=';')
    print()

    print('change')
    l.set(0,0,37)
    cp.set(0,'b',-27)
    cp2.set(0,'txt','abcdefgh')

    for n in names:
        print(n, '=', l.get(0, n), end=';')
    print()
    for n in names:
        print(n, '=', cp.get(0, n), end=';')
    print()

    for i, v in enumerate(tpl):
        print(i, ': ', v, '(l) =>', l.get_shm_name(i))
        print(i, ': ', v, '(cp)=>', cp.get_shm_name(i))

    for x in range(len(l)):
        l.set(x,2,x)
    l[0] = (-1,-1,-1,'start',-1.0)
    cp[len(l)-1] = (0,0,0,'end', 0.0)
    for x in range(len(l)):
        print(cp2[x])

    l.close()
    cp.close()
    cp2.close()
    l.unlink()

