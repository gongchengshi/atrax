def convert(converter, value):
    return converter(value)

result = convert(int, '5')

print result
print type(result)
