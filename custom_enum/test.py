from .enum import Enum, IntEnum


class ConstA(Enum):
    enum_a = 'enum_a'
    enum_b = 'enum_b'
    enum_c = 'enum_c'

    __choices__ = (
        (enum_a, u'test_enum_a'),
        (enum_b, u'test_enum_a')
    )


a = ConstA.enum_a
print(a.name, a.label, a.value, a, a == a.value)
c = ConstA.enum_c
print(c.name, c.value, c, c == c.value)

b = ConstA['enum_b']
print(b, b.name, b.value, b.label, b == b.value)
print('enum_a' in (ConstA.enum_a, ConstA.enum_b))

print(ConstA.to_dict())
print(ConstA.label_items())


class ConstB(IntEnum):
    enum_a = 1
    enum_b = 2
    enum_c = 3

    __choices__ = (
        (enum_a, u'test_enum_a'),
        (enum_b, u'test_enum_a')
    )


a = ConstB.enum_a
print(a.name, a.label, a.value, a, a == a.value)
c = ConstB.enum_c
print(c.name, c.value, c, c == c.value)

b = ConstB['enum_b']
print(b, b.name, b.value, b.label, b == b.value)
b = ConstB[2]
print(b, b.name, b.value, b.label, b == b.value)

print(ConstB.to_dict())
print(ConstB.label_items())
