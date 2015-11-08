import subprocess

DEBUG = False

def download(remotePath, localPath):
    if DEBUG:
        print 'download', remotePath, localPath
    proc = subprocess.Popen(['xrdcp', 'root://eoscms.cern.ch/' + remotePath, localPath])
    proc.wait()

    return proc.returncode

def upload(localPath, remotePath):
    if DEBUG:
        print 'upload', localPath, remotePath
    proc = subprocess.Popen(['xrdcp', localPath, 'root://eoscms.cern.ch/' + remotePath])
    proc.wait()

    return proc.returncode

def ls(remotePath, lopt = False):
    if DEBUG:
        print 'ls', remotePath
    command = ['xrdfs', 'eoscms.cern.ch', 'ls']
    if lopt:
        command.append('-l')
    command.append(remotePath)

    proc = subprocess.Popen(command, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = proc.communicate()

    if err.strip():
        print err.strip()

    out = out.strip()
    if out == '':
        return []
    else:
        return out.split('\n')

def rm(remotePath):
    if DEBUG:
        print 'rm', remotePath
    proc = subprocess.Popen(['xrdfs', 'eoscms.cern.ch', 'rm', remotePath])

    return proc.returncode

def rmdir(remotePath):
    if DEBUG:
        print 'rmdir', remotePath
    proc = subprocess.Popen(['xrdfs', 'eoscms.cern.ch', 'rmdir', remotePath])

    return proc.returncode

def mkdir(remotePath):
    if DEBUG:
        print 'mkdir', remotePath
    proc = subprocess.Popen(['xrdfs', 'eoscms.cern.ch', 'mkdir', remotePath])

    return proc.returncode

def cleanup(remotePath, force = False):
    if DEBUG:
        print 'cleanup', remotePath

    lines = ls(remotePath, lopt = True)
    if len(lines) == 0:
        rmdir(remotePath)
        return True

    for line in lines:
        words = line.split()
        newPath = words[-1]
        if words[0][0] == 'd':
            if not cleanup(newPath):
                return False
        else:
            if force:
                rm(newPath)
            else:
                return False

    return True
