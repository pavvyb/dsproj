import uuid
import os
import math
import rpyc

def getPath(path):
    folders = path.split('/')
    if path == '/':
        return []
    return folders[1:]


def currentDirectory(path):
    directoryP = getPath(path)
    pwd = FS
    for directory in directoryP:
        pwd = pwd[directory]
    return pwd

def createBID():
    return str(uuid.uuid1())

def fileInfo(path, filename):
    print('[file info]', filename)
    pwd = currentDirectory(path)
    res = pwd[filename]
    code = 0
    if not (type(pwd[filename]['block']) == type([])):
        code = 1
        res = []
        print('\t directory, not a file')
    return code, res

def fileCreate(path, inpFile):
    pwd = currentDirectory(path)
    global lastNode
    lastNode += 1
    pwd[filename] = {'name': inpFile, 'node_id': lastNode, 'size': 0, 'block': [], 'blocks_address': {}}
    return 0

def fileDelete(path, filename):
    pwd = currentDirectory(path)
    file = pwd[filename]
    block_names = file['block']
    code = 1
    for name in block_names:
        addresses = file['blocks_address'][name]
        for address in addresses:
            new_code = deleteBlock(name, address)

            if (code != 0 and new_code == 0):
                code = 0
            elif code != 0:
                code = new_code

    pwd.pop(filename, None)
    return code

def fileCopy(path, filename, newpath):
    pwd = currentDirectory(path)
    file = pwd[filename]
    newpwd = currentDirectory(newpath)
    fileCreate(newpath, filename)
    newpwd[filename]['size'] = file['size']

    global lastNode
    lastNode += 1
    newpwd[filename]['node_id'] = lastNode

    blockNames = file['block'].copy()

    code = -1
    for name in blockNames:
        addresses = file['blocks_address'][name]
        newname = createBID()
        for address in addresses:
            newcode = copyBlock(name, newname, address)

            if (code != 0 and newcode == 0):
                code = 0
            elif code != 0:
                code = newcode

        new_pwd[filename]['block'].append(new_name)
        new_pwd[filename]['blocks_address'][new_name] = data_nodes

    return code


def fileMove(path, filename, newpath):
    pwd = currentDirectory(path)
    newpwd = currentDirectory(newpath)
    newpwd[filename] = pwd.pop(filename, None)
    return 0


def fileRead(path, filename):
    pwd = currentDirectory(path)
    file = pwd[filename]
    blocknames = file['block']
    bfile = b''
    for name in block_names:
        addresses = file['blocks_address'][name]
        byteblock = None
        for address in addresses:
            newcode, byteblock = readBlock(name, address)

        bfile += byteblock
    return bfile


def fileWrite(path, filename, bfile):
    size = len(bfile)
    fileCreate(path, filename)
    pwd = currentDirectory(path)
    file = pwd[filename]
    file['size'] = size
    blockNum = math.ceil(size / block_size)
    addresses = []
    for i in range(blockNum):
        b_part = i * blockSize
        if i + 1 == blockNum:
            e_part = len(bfile)
        else:
            e_part = (i + 1) * block_size
        byteblock = bfile[b_part:e_part]
        uid = createBID()
        file['block'].append(uid)
        file['blocks_address'][uid] = data_nodes
        for node in data_nodes:
            addresses.append((uid, node))
            writeBlock(uid, byteblock, node)


def readBlock(name, address):
    print('trye to connect')
    byteblock = None
    c = rpyc.connect(address["ip"], address["port"])
    code, byteblock = c.root.read_block(name)

    return byteblock


def writeBlock(name, byteblock, address):
    datanode = rpyc.connect(address["ip"], address["port"])
    code = datanode.root.writeBlock(name, byteblock)


def deleteBlock(name, address):
    datanode = rpyc.connect(address['ip'], address['port'])
    code = datanode.root.delete_block(name)


def copyBlock(name, new_name, address):
    datanode = rpyc.connect(address['ip'], address['port'])
    code = datanode.root.copyBlock(name, new_name)

class Namenode(rpyc.Service):
    
    # Calls to API
   
    def exposed_file_read(self, directoryPath, filename):
        code = 0
        bfile = b''
        try:
            code, bfile = fileRead(directoryPath, filename)
        except KeyError:
            code = 1
        except:
            code = 3

        return code, bfile

    def exposed_file_write(self, directoryPath, filename, file_bytes):
        code = 0
        try:
            code = fileWrite(directoryPath, filename, file_bytes)
        except KeyError:
            code = 1
        except:
            code = 3

        return code



    def exposed_file_delete(self, filepath, filename):
        code = 0
        try:
            fileDelete(filepath, filename)
        except KeyError:
            code = 1
        except:
            code = 3

        return code

    def exposed_file_copy(self, filepath, filename, newdir):
        code = 0
        try:
            fileCopy(filepath, filename, newdir)
        except KeyError:
            code = 1
        except:
            code = 3

        return code

    def exposed_file_create(self, filepath, filename):
        code = 0
        try:
            fileCreate(filepath, filename)
        except KeyError:
            code = 2

        return code

    def exposed_file_info(self, filepath, filename):
        code = 0
        content = ""
        try:
            code, content = fileInfo(filepath, filename)
        except KeyError:
            code = 1

        return code, str(content)

    def exposed_file_move(self, filepath, filename, newdir):
        code = 0
        try:
            fileMove(filepath, filename, newdir)
        except KeyError:
            code = 1

        return code


if __name__ == '__main__':
    print('Naming Node [starting]')
    

