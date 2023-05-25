
class test:

    var1 = "test"

inst = test()

print(test.var1)
print(inst.var1)

test.var1 = "test2"

print(test.var1)
print(inst.var1)

inst.var1 = "test3"

print(test.var1)
print(inst.var1)

test.var1 = "test4"

print(test.var1)
print(inst.var1)