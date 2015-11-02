import sys

source = sys.argv[1]

with open(source) as json:
    lumiList = eval(json.read())

with open('/tmp/json', 'w') as out:
    out.write('{')
    runBlocks = []
    for run in sorted(lumiList.keys()):
        if int(run) > 258158:
            break

        runBlock = '"%s": [' % run
        lumiRanges = []
        for start, end in lumiList[run]:
            lumiRanges.append('[%d, %d]' % (start, end))

        runBlock += ', '.join(lumiRanges)
        runBlock += ']'

        runBlocks.append(runBlock)

    out.write(', '.join(runBlocks))
    out.write('}')

