import os
import rpyc

def initialize():
    conection = rpyc.connect("localhost", 22888)
    block_size = conection.root.initialize()
    print(f'OK. Available size :{block_size}')


def fileCreate(path):
    conection = rpyc.connect("localhost", 22888)
    print(conection.root.create_file(path))


def fileRead(path):
    conection = rpyc.connect("localhost", 22888)
    code, res = conection.root.fileRead(path)
    if code == "success":
        with open(path.split('/')[-1], 'wb') as f:
            f.write(res)
    else:
        print(code)


def fileWrite(loc, rem):
    conection = rpyc.connect("localhost", 22888)
    print(conection.root.write_file(loc, rem))


def fileDelete(path):
    conection = rpyc.connect("localhost", 22888)
    print(conection.root.delete_file(path))


def fileInfo(path):
    conection = rpyc.connect("localhost", 22888)
    print(conection.root.get_file_info(path))


def fileCopy(src, dest):
    conection = rpyc.connect("localhost", 22888)
    print(conection.root.copy_file(src, dest))


def fileMove(src, dest):
    conection = rpyc.connect("localhost", 22888)
    print(conection.root.move_file(src, dest))
