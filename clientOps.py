import os
import rpyc


class Client(rpyc.Service):

    def __init__(self):
        self.connection = rpyc.connect(" ", 77777)
        super(Client, self).__init__()

    def fullPath(self, path):
        if path[0] == '/':
            return path
        elif '..' in path:
            currDir = ['/'].copy()
            for dir in path.split('/'):
                if dir == '..':
                    currDir.pop(-1)
                else:
                    currDir.append(dir)
            return os.path.join(*currDir)
        elif path[0] == '.':
            return os.path.join(*currDir)
        else:
            return os.path.join(*(currDir + path.split('/')))

    def ffname(self, path):
        abs_path = self.fullPath(path)
        path_files = abs_path.split('/')
        filepath, filename = os.path.join(*path_files[:-1]), path_files[-1] if path_files[-1] else path_files[0]
        return '/' + filepath if filepath else '/', filename

    # Calls to API
    def exposed_initialize(self):
        available_size = self.connection.root.init()
        return available_size

    def exposed_get_file_info(self, path):
        filepath, filename = self.ffname(path)
        code, res = self.connection.root.fileInfo(filepath, filename)
        if code == 0:
            return res
        else:
            return code

    def exposed_create_file(self, path):
        filepath, filename = self.ffname(path)
        code = self.connection.root.fileCreate(filepath, filename)
        return code

    def exposed_delete_file(self, path):
        filepath, filename = self.ffname(path)
        code = self.connection.root.fileDelete(filepath, filename)
        return code

    def exposed_copy_file(self, src, dest):
        filepath, filename = self.ffname(src)
        remdest = self.fullPath(dest)

        code = self.connection.root.fileCopy(filepath, filename, remdest)
        return code

    def exposed_read_file(self, path):
        filepath, filename = self.ffname(path)
        code, res = self.connection.root.fileRead(filepath, filename)
        if code == 0:
            return code, res
        else:
            return code, 'error'

    def exposed_write_file(self, local_path, remote_path):
        if os.path.isfile(local_path):
            with open(local_path, 'rb') as f:
                content = f.read()

                filepath, filename = self.ffname(remote_path)
                code = self.connection.root.fileWrite(filepath, filename, content)
                return code

        else:
            return 'File' + local_path + 'does not exist'

    def exposed_move_file(self, src, dest):
        filepath, filename = self.ffname(src)
        remdest = self.fullPath(dest)

        code = self.connection.root.fileMove(filepath, filename, remdest)
        return code


if __name__ == '__main__':
    print('Client Node [starting]')

