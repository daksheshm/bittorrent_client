from math import ceil

class BitField:

    bitfield: bytearray
    num_bits: int
    num_bytes: int

    def __init__(self, num_bits: int, bytestr: bytes = None):
        self.num_bytes = ceil(num_bits / 8)
        if bytestr:
            if not len(bytestr) >= self.num_bytes:
                raise Exception
            self.bitfield = bytearray(bytestr)
        else:
            self.bitfield = bytearray(self.num_bytes)
        self.num_bits = num_bits

    def set_bit(self, index: int):
        if not self.num_bits > index >= 0:
            raise IndexError()
        byte_index = index // 8
        bit_index = index % 8
        self.bitfield[byte_index] |= (1 << 7-bit_index)

    def clear_bit(self, index: int):
        if not self.num_bits > index >= 0:
            raise IndexError()
        byte_index = index // 8
        bit_index = index % 8
        self.bitfield[byte_index] &= ~(1 << 7-bit_index)

    def get_bit(self, index: int):
        if not self.num_bits > index >= 0:
            raise IndexError()
        byte = self.bitfield[index//8]
        bit = index % 8
        return (byte >> 7-bit) & 1
    
    def is_set(self, index: int) -> bool:
        return bool(self.get_bit(index))

    def clear(self):
        self.bitfield = bytearray(self.num_bytes)

    def add_to(self, targ: bytearray):
        for i in range(min(len(targ), self.num_bits)):
            targ[i] += self.get_bit(i)

    def sub_from(self, targ: bytearray):
        for i in range(min(len(targ), self.num_bits)):
            targ[i] -= self.get_bit(i)
    
    def len_bytes(self) -> int:
        return self.num_bytes
    
    def len_bits(self) -> int:
        return self.num_bits

    def to_bytes(self) -> bytes:
        return bytes(self.bitfield)
    
    def to_bytearray(self) -> bytearray:
        return self.bitfield
    
    def to_str(self) -> str:
        out = f''
        for i in range(self.num_bits):
            out += f'{self.get_bit(i)}'
        return out


def test():
    b = BitField(20)
    b.set_bit(5)
    b.set_bit(0)
    b.set_bit(19)
    print(b.to_str())

    s = bytearray(20)
    b.add_to(s)
    b.set_bit(6)
    b.add_to(s)
    b.sub_from(s)
    print(s)

    b.clear_bit(5)
    print(b.get_bit(5))
    print(b.get_bit(6))
    print(b.len_bytes())

    print(b.to_str())
    c = BitField(20, b.to_bytes())
    print(c.to_str())

    b.clear()
    print(b.to_str())

if __name__ == '__main__':
    test()