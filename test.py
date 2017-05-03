import dbdb
import settings
if __name__ == '__main__':
    i = 0
    print('connecting database')
    db = dbdb.connect(settings.FILE_DATA)
    print(len(db))
    with open(settings.FILE_LINK, 'r') as f:
        for line in f.readlines():
            if line.strip() in db:
                print('{}:{}'.format(i, line))
                print('okay')
            i += 1