import shutil
import rpyc
import os
import signal


class Datanode(rpyc.Service):

    def exposed_write_block(self, name, block):
        code = 0
        try:
            writeBlock(name, block)
        except:
            code = 1
        return code

    def exposed_delete_block(self, name):
        code = 0
        try:
            deleteBlock(name)
        except KeyError:
            code = 1
        return code


    def exposed_copy_block(self, prev_name, new_name):
        code = 0
        try:
            copyBlock(prev_name, new_name)
        except KeyError:
            code = 1
        return code

    def exposed_free_disk_space(self):
        return disSpace()

    def exposed_read_block(self, name):
        code = 0
        content = []
        try:
            content = readBlock(name)
        except KeyError:
            code = 1
        return code, content

    def exposed_flush(self):
        free_space = disSpace()
        free_space = purge()
        except KeyError:
            code = 4
        return free_space


def disSpace():
    total, used, free = shutil.disk_usage("/")
    print("Free Space: %d MiB" % (free // (2 ** 20)))
    return free // (2 ** 20)


def writeBlock(name, block):
    print('try to write block')
    f = open("./storage" + '/' + name, 'wb')
    f.write(block)
    f.close()
    return 0


def readBlock(name):
    f = open("./storage" + '/' + name, 'rb')
    block = f.read()
    f.close()
    return block


def deleteBlock(name):
    if os.path.exists("./storage" + '/' + name):
        os.remove("./storage" + '/' + name)
    else:
        print("The file does not exist")
    return 0

def copyBlock(prev_block_name, new_block_name):
    f1 = open("./storage" + '/' + prev_block_name, 'rb')
    block = f1.read()
    f2 = open("./storage" + '/' + new_block_name, 'wb')
    f2.write(block)
    f2.close()
    f1.close()


def purge():
    os.makedirs("./storage", exist_ok=True)
    storag = os.listdir("./storage")
    for filename in storag:
        os.unlink("./storage" + '/' + filename)
    return disSpace()

if __name__ == "__main__":
    print("Data Node [starting]")
