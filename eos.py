import subprocess
import config

def eos(cmd, *args):
    proc = subprocess.Popen(['eos', cmd] + list(args), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = proc.communicate()
    if err.strip():
        print err.strip()

    res = out.strip().split('\n')
    if len(res) == 1 and res[0] == '':
        res = []

    return res


def cleanup(pathParts):
    lines = eos('ls', '-l', config.eosdir + '/' + '/'.join(pathParts))
    if len(lines) == 0:
        return True

    for line in lines:
        words = line.split()
        newParts = pathParts + [words[-1]]
        path = '/' + '/'.join(newParts)
        if words[0][0] == 'd':
            if cleanup(newParts):
                print 'Removing', path
                eos('rmdir', config.eosdir + path)

        else:
            return False

    return True
